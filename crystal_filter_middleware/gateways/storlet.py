from swift.common.swob import Request
from swift.common.utils import config_true_value
from storlet_middleware.handlers.base import SwiftFileManager


class CrystalGatewayStorlet():

    def __init__(self, conf, logger, request_data):
        self.conf = conf
        self.logger = logger
        self.app = request_data['app']
        self.version = request_data['api_version']
        self.account = request_data['account']
        self.scope = self.account[5:18]
        self.container = request_data['container']
        self.obj = request_data['object']
        self.method = request_data['method']
        self.server = self.conf['execution_server']

        self.storlet_name = None
        self.storlet_metadata = None
        self.gateway = None
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
        :param params: paramegers to be augmented to request
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

        scope = self.account
        if scope.rfind(':') > 0:
            scope = scope[:scope.rfind(':')]
        
        options['scope'] = self.scope

        options['generate_log'] = \
            config_true_value(req.headers.get('X-Storlet-Generate-Log'))

        options['file_manager'] = \
            SwiftFileManager(self.account, self.storlet_container,
                             self.storlet_dependency, self.log_container,
                             self.client_conf_file, self.logger)

        return options    

    def _build_storlet_request(self, req_resp, params, crystal_iter):
        storlet_id = self.storlet_name

        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)

        req.environ['QUERY_STRING'] = params
        req.headers['X-Run-Storlet'] = self.storlet_name
        self._augment_storlet_request(req)
        options = self._get_storlet_invocation_options(req)
 
        if hasattr(crystal_iter, '_fp'):
            sreq = self.sreq_class(storlet_id, req.params, dict(),
                                   data_fd=crystal_iter._fp.fileno(),
                                   options=options)
        else:
            sreq = self.sreq_class(storlet_id, req.params, dict(),
                                   crystal_iter, options=options)

        return sreq

    def _call_gateway(self, req_resp, params, crystal_iter):
        sreq = self._build_storlet_request(req_resp, params, crystal_iter)
        sresp = self.gateway.invocation_flow(sreq)
        
        return sresp.data_iter

    def execute(self, req_resp, storlet_data, crystal_iter):
        storlet = storlet_data.pop('name')
        params = storlet_data.pop('params')
        self.storlet_name = storlet
        self.storlet_metadata = storlet_data

        self.logger.info('Crystal Filters - Go to execute ' + storlet +
                         ' storlet with parameters "' + params + '"')
        
        self._setup_gateway()
        data_iter = self._call_gateway(req_resp, params, crystal_iter)

        return data_iter
