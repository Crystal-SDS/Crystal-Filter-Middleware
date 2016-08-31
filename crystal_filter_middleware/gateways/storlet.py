from swift.common.swob import Request


class CrystalGatewayStorlet():

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
        self.gateway_docker = None
        self.gateway_method = None

    def _set_storlet_request(self, req_resp, params):

        self.gateway_docker = self.gateway_module(self.conf, self.logger,
                                                  self.app, self.account)

        self.gateway_method = getattr(self.gateway_docker, "gateway" +
                                      self.server.title() +
                                      self.method.title() + "Flow")

        """ Simulate Storlet request """
        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)

        # TODO(josep): check X-Storlet-Range header
        req.headers['X-Run-Storlet'] = self.storlet_name
        req.headers['X-Storlet-Main'] = self.storlet_metadata['main']
        req.headers['X-Storlet-Dependency'] = self.storlet_metadata['dependencies']
        req.headers['X-Storlet-Content-Length'] = self.storlet_metadata['size']
        # TODO(josep): Change to correct timestamp
        req.headers['X-Storlet-X-Timestamp'] = 0

        req.environ['QUERY_STRING'] = params.replace(',', '&')

        return req

    def _launch_storlet(self, req_resp, params, crystal_iter):
        req = self._set_storlet_request(req_resp, params)

        if self.method == 'put':
            sresp = self.gateway_method(req, crystal_iter)
        elif self.method == 'get':
            sresp = self.gateway_method(req, req_resp, crystal_iter)
        # TODO(josep): Other methods
        return sresp.data_iter

    def execute(self, req_resp, storlet_data, crystal_iter):
        storlet = storlet_data['name']
        params = storlet_data['params']
        self.storlet_name = storlet
        self.storlet_metadata = storlet_data

        self.logger.info('Crystal Filters - Go to execute ' + storlet +
                         ' storlet with parameters "' + params + '"')

        data_iter = self._launch_storlet(req_resp, params, crystal_iter)

        return data_iter
