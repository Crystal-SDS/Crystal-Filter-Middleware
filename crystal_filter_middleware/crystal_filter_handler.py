from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger

from crystal_filter_middleware.handlers import CrystalProxyHandler
from crystal_filter_middleware.handlers import CrystalObjectHandler
from crystal_filter_middleware.handlers.base import NotCrystalRequest
from crystal_filter_middleware.filters.filter_control import CrystalFilterControl

import ConfigParser
import redis

try:
    import storlets
    STORLETS = True
except:
    STORLETS = False


class CrystalHandlerMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.exec_server = self.conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server +
                                 "-server Crystal Filters",
                                 log_route='crystal_filter_handler')
        self.handler_class = self._get_handler(self.exec_server)

        ''' Singleton instance of filter control '''
        self.control_class = CrystalFilterControl
        self.filter_control = self.control_class(conf=self.conf,
                                                 log=self.logger)

    def _get_handler(self, exec_server):
        if exec_server == 'proxy':
            return CrystalProxyHandler
        elif exec_server == 'object':
            return CrystalObjectHandler
        else:
            raise ValueError('configuration error: execution_server must be'
                             ' either proxy or object but is ' + exec_server)

    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(req, self.conf,
                                                 self.app, self.logger,
                                                 self.filter_control)
            self.logger.debug('call in %s-server with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container, request_handler.obj))
        except HTTPException:
            raise
        except NotCrystalRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Middleware execution failed')
            raise
        except Exception:
            self.logger.exception('Middleware execution failed')
            raise HTTPInternalServerError(
                body='Crystal filter middleware execution failed')


def filter_factory(global_conf, **local_conf):
    """
    Standard filter factory to use the middleware with paste.deploy
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    conf['redis_host'] = conf.get('redis_host', 'controller')
    conf['redis_port'] = int(conf.get('redis_port', 6379))
    conf['redis_db'] = int(conf.get('redis_db', 0))

    conf['native_filters_path'] = conf.get('native_filters_path',
                                           '/opt/crystal/native_filters')

    """
    Storlets Configuration
    """
    if STORLETS:
        """ Load Storlets Gateway class """
        from storlets.gateway.loader import load_gateway
        module_name = conf.get('storlet_gateway_module', 'stub')
        gateway_class = load_gateway(module_name)
        conf['storlets_gateway_module'] = gateway_class

        """ Load Storlets Gateway configuration """
        configParser = ConfigParser.RawConfigParser()
        configParser.read(conf.get('storlet_gateway_conf',
                                   '/etc/swift/storlet_stub_gateway.conf'))
        additional_items = configParser.items("DEFAULT")

        for key, val in additional_items:
            conf[key] = val

    """
    Register Lua script to retrieve policies in a single redis call
    """
    r = redis.StrictRedis(conf['redis_host'],
                          conf['redis_port'],
                          conf['redis_db'])
    lua = """
        local t = {}
        if redis.call('EXISTS', 'pipeline:'..ARGV[1]..':'..ARGV[2]..':'..ARGV[3])==1 then
          t = redis.call('HGETALL', 'pipeline:'..ARGV[1]..':'..ARGV[2]..':'..ARGV[3])
        elseif redis.call('EXISTS', 'pipeline:'..ARGV[1]..':'..ARGV[2])==1 then
          t = redis.call('HGETALL', 'pipeline:'..ARGV[1]..':'..ARGV[2])
        elseif redis.call('EXISTS', 'pipeline:'..ARGV[1])==1 then
          t = redis.call('HGETALL', 'pipeline:'..ARGV[1])
        end
        t[#t+1] = '@@@@'
        local t3 = redis.call('HGETALL', 'pipeline:global')
        for i=1,#t3 do
          t[#t+1] = t3[i]
        end
        return t"""
    lua_sha = r.script_load(lua)
    conf['LUA_get_pipeline_sha'] = lua_sha

    def crystal_filter_handler(app):
        return CrystalHandlerMiddleware(app, conf)

    return crystal_filter_handler
