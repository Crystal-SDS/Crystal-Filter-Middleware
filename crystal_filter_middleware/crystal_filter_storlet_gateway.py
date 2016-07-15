'''===========================================================================
16-Oct-2015    josep.sampe    Initial implementation.
05-Feb-2016    josep.sampe    Added Proxy execution.
01-Mar-2016    josep.sampe    Addded pipeline (multi-node)
22-Mar-2016    josep.sampe    Enhanced performance
==========================================================================='''
from swift.common.swob import Request

class SDSGatewayStorlet():

    def __init__(self, conf, logger, request_data):
        self.conf = conf
        self.logger = logger
        self.app = request_data['app']
        self.version = request_data['api_version']
        self.account = request_data['account']
        self.container = request_data['container']
        self.obj = request_data['object']
        self.method = request_data['method']
        self.server = self.conf['execution_server']
        
        self.storlet_metadata = None
        self.storlet_name = None
        self.gateway_module = self.conf['storlets_gateway_module']
        self.gateway_method = None
        self.gateway_docker = None

    def set_storlet_request(self, req_resp, params):

        self.gateway_docker = self.gateway_module(self.conf, self.logger, 
                                                  self.app, self.account)

        self.gateway_method = getattr(self.gateway_docker, "gateway" +
                                      self.server.title() +
                                      self.method.title() + "Flow")

        # Set the Storlet metadata to the request
        # Available keys:
        # Content-Length
        # Interface-Version
        # Language
        # Dependency
        # X-Timestamp
        # Object-Metadata
        # Main 

        #storlet_metadata = {}
        #storlet_metadata['Main'] = 
        #storlet_metadata['Dependency'] = 
        #storlet_metadata['Content-Length'] = 
        #md['ETag'] = self.storlet_metadata['etag']
        
        # Simulate Storlet request
        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)
        
        req.headers['X-Run-Storlet'] = self.storlet_name
        req.headers['X-Storlet-Main'] = self.storlet_metadata['main']
        req.headers['X-Storlet-Dependency'] = self.storlet_metadata['dependencies']
        req.headers['X-Storlet-Content-Length'] = self.storlet_metadata['size']
        req.headers['X-Storlet-X-Timestamp'] = 0
        
        #self.gateway_docker.augmentStorletRequest(req, storlet_metadata)
        req.environ['QUERY_STRING'] = params.replace(',', '&')

        return req

    def _launch_storlet(self, req_resp, params, input_pipe=None):
        req = self.set_storlet_request(req_resp, params)

        sresp = self.gateway_method(req, req_resp, input_pipe)
        
        return sresp.data_iter

    def execute_storlet(self, req_resp, storlet_data, app_iter):
        storlet = storlet_data['name']
        params = storlet_data['params']
        self.storlet_name = storlet
        self.storlet_metadata = storlet_data

        self.logger.info('Crystal Filters - Go to execute ' + storlet +
                         ' storlet with parameters "' + params + '"')
                
        data_iter = self._launch_storlet(req_resp, params, app_iter)
        
        return data_iter
