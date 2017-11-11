'''
A Mini-implementation of the Storlet middleware filter.

@author: josep sampe
'''
from swift.common.utils import get_logger
from swift.common.utils import register_swift_info
from swift.common.swob import Request
from swift.common.utils import config_true_value
from storlets.swift_middleware.handlers.base import SwiftFileManager
from swift.common.swob import wsgify


class StorletFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.exec_server = self.conf.get('execution_server')
        self.logger = get_logger(self.conf, log_route='storlet_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.gateway_class = self.conf['storlets_gateway_module']
        self.sreq_class = self.gateway_class.request_class

        self.storlet_container = conf.get('storlet_container')
        self.storlet_dependency = conf.get('storlet_dependency')
        self.log_container = conf.get('storlet_logcontainer')
        self.client_conf_file = '/etc/swift/storlet-proxy-server.conf'

        self.register_info()

    def register_info(self):
        register_swift_info('storlet_filter')

    def _setup_gateway(self):
        """
        Setup gateway instance
        """
        self.gateway = self.gateway_class(self.conf, self.logger, self.scope)

    def _augment_storlet_request(self, req):
        """
        Add to request the storlet parameters to be used in case the request
        is forwarded to the data node (GET case)
        :param params: parameters to be augmented to request
        """
        req.headers['X-Storlet-Language'] = self.filter_data['language']
        req.headers['X-Storlet-Main'] = self.filter_data['main']
        req.headers['X-Storlet-Dependency'] = self.filter_data['dependencies']
        req.headers['X-Storlet-Content-Length'] = self.filter_data['size']
        req.headers['X-Storlet-Generate-Log'] = False
        req.headers['X-Storlet-X-Timestamp'] = 0

    def _get_storlet_invocation_options(self, req):
        options = dict()

        filtered_key = ['X-Storlet-Range', 'X-Storlet-Generate-Log']

        for key in req.headers:
            prefix = 'X-Storlet-'
            if key.startswith(prefix) and key not in filtered_key:
                new_key = 'storlet_' + \
                    key[len(prefix):].lower().replace('-', '_')
                options[new_key] = req.headers.get(key)

        generate_log = req.headers.get('X-Storlet-Generate-Log')
        options['generate_log'] = config_true_value(generate_log)
        options['scope'] = self.scope
        options['file_manager'] = \
            SwiftFileManager(self.account, self.storlet_container,
                             self.storlet_dependency, self.log_container,
                             self.client_conf_file, self.logger)

        return options

    def _build_storlet_request(self, req_resp, params, data_iter):
        storlet_id = self.storlet_name

        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)

        req.headers['X-Run-Storlet'] = self.storlet_name
        self._augment_storlet_request(req)
        options = self._get_storlet_invocation_options(req)

        if hasattr(data_iter, '_fp'):
            sreq = self.sreq_class(storlet_id, params, dict(),
                                   data_fd=data_iter._fp.fileno(),
                                   options=options)
        else:
            sreq = self.sreq_class(storlet_id, params, dict(),
                                   data_iter, options=options)

        return sreq

    def _call_gateway(self, req_resp, params, crystal_iter):
        sreq = self._build_storlet_request(req_resp, params, crystal_iter)
        sresp = self.gateway.invocation_flow(sreq)

        return sresp.data_iter

    @wsgify
    def __call__(self, req):
        if req.method in ('GET', 'PUT'):
            storlet = self.filter_data.pop('name')
            params = self.parameters
            self.storlet_name = storlet
            etag = None

            try:
                if self.exec_server == 'proxy':
                    _, self.account, _, _ = req.split_path(4, 4, rest_with_last=True)
                elif self.exec_server == 'object':
                    _, _, self.account, _, _ = req.split_path(5, 5, rest_with_last=True)
            except:
                # No object Request
                return req.get_response(self.app)

            self.scope = self.account[5:18]

            self.logger.info('Go to execute ' + storlet +
                             ' storlet with parameters "' + str(params) + '"')

            self._setup_gateway()

            if 'Etag' in req.headers.keys():
                etag = req.headers.pop('Etag')

            if req.method == 'GET':
                response = req.get_response(self.app)
                content_length = response.headers['Content-Length']
                if 'X-Object-Sysmeta-Crystal' in response.headers:
                    crystal_md = eval(response.headers.pop('X-Object-Sysmeta-Crystal'))
                    if crystal_md['original-size']:
                        content_length = crystal_md['original-size']
                    if crystal_md['original-size']:
                        etag = crystal_md['original-etag']
                data_iter = response.app_iter
                response.app_iter = self._call_gateway(response, params, data_iter)

                if 'Content-Length' not in response.headers:
                    response.headers['Content-Length'] = content_length
                if 'Transfer-Encoding' in response.headers:
                    response.headers.pop('Transfer-Encoding')

            elif req.method == 'PUT':
                reader = req.environ['wsgi.input'].read
                data_iter = iter(lambda: reader(65536), '')
                req.environ['wsgi.input'] = self._call_gateway(req, params, data_iter)
                if 'CONTENT_LENGTH' in req.environ:
                    req.environ.pop('CONTENT_LENGTH')
                req.headers['Transfer-Encoding'] = 'chunked'
                response = req.get_response(self.app)

            if etag:
                response.headers['etag'] = etag
            else:
                response.headers['etag'] = ''

            return response

        return req.get_response(self.app)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def storlet_filter(app):
        return StorletFilter(app, conf)
    return storlet_filter
