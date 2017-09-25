from swift.common.swob import Request
from swift.common.utils import config_true_value
from storlets.swift_middleware.handlers.base import SwiftFileManager


class CrystalGatewayStorlet():

    def __init__(self, conf, logger):
        self.conf = conf
        self.logger = logger

        self.gateway_class = self.conf['storlets_gateway_module']
        self.sreq_class = self.gateway_class.request_class

        self.storlet_container = conf.get('storlet_container')
        self.storlet_dependency = conf.get('storlet_dependency')
        self.log_container = conf.get('storlet_logcontainer')
        self.client_conf_file = '/etc/swift/storlet-proxy-server.conf'

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
        req.headers['X-Storlet-Language'] = self.storlet_metadata['language']
        req.headers['X-Storlet-Main'] = self.storlet_metadata['main']
        req.headers['X-Storlet-Dependency'] = self.storlet_metadata['dependencies']
        req.headers['X-Storlet-Content-Length'] = self.storlet_metadata['size']
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

    def execute(self, req_resp, data_iter, storlet_data):
        storlet = storlet_data.pop('name')
        params = storlet_data.pop('params')
        self.storlet_name = storlet
        self.storlet_metadata = storlet_data

        self.account = req_resp.environ['PATH_INFO'].split('/')[2]
        self.scope = self.account[5:18]

        self.logger.info('Go to execute ' + storlet +
                         ' storlet with parameters "' + str(params) + '"')

        self._setup_gateway()
        return self._call_gateway(req_resp, params, data_iter)
