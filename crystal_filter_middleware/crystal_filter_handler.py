from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import config_true_value
from swift.common.utils import get_logger

from crystal_filter_middleware.handlers import CrystalProxyHandler
from crystal_filter_middleware.handlers import CrystalObjectHandler
from crystal_filter_middleware.handlers.base import NotCrystalRequest
from crystal_filter_middleware.filters.control import CrystalFilterControl

import ConfigParser
import redis


class CrystalHandlerMiddleware(object):

    def __init__(self, app, conf, crystal_conf):
        self.app = app
        self.conf = crystal_conf
        self.logger = get_logger(conf, log_route='crystal_filter_handler')
        self.exec_server = self.conf.get('execution_server')
        self.containers = [self.conf.get('storlet_container'),
                           self.conf.get('storlet_dependency')]
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
            self.logger.debug('crystal_filter_handler call in %s: with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container, request_handler.obj))
        except HTTPException:
            raise
        except NotCrystalRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Crystal filter middleware execution failed')
            raise
        except Exception:
            self.logger.exception('Crystal filter middleware execution failed')
            raise HTTPInternalServerError(
                body='Crystal filter middleware execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""

    conf = global_conf.copy()
    conf.update(local_conf)

    crystal_conf = dict()
    crystal_conf['execution_server'] = conf.get('execution_server', 'object')
    crystal_conf['identifier'] = conf.get('os_identifier')

    crystal_conf['redis_host'] = conf.get('redis_host', 'controller')
    crystal_conf['redis_port'] = int(conf.get('redis_port', 6379))
    crystal_conf['redis_db'] = int(conf.get('redis_db', 0))

    crystal_conf['storlet_timeout'] = int(conf.get('storlet_timeout', 40))
    crystal_conf['storlet_container'] = conf.get('storlet_container',
                                                 'storlet')
    crystal_conf['storlet_dependency'] = conf.get('storlet_dependency',
                                                  'dependency')
    crystal_conf['storlet_logcontainer'] = conf.get('storlet_logcontainer',
                                                    'storletlog')

    crystal_conf['storlet_gateway_module'] = conf.get('storlet_gateway_module')
    crystal_conf['storlet_execute_on_proxy_only'] = \
        config_true_value(conf.get('storlet_execute_on_proxy_only', 'false'))

    crystal_conf['reseller_prefix'] = conf.get('reseller_prefix', 'AUTH')
    crystal_conf['bind_ip'] = conf.get('bind_ip')
    crystal_conf['bind_port'] = conf.get('bind_port')

    """ Load Storlets Gateway class """
    module_name = conf.get('storlet_gateway_module', '')
    mo = module_name[:module_name.rfind(':')]
    cl = module_name[module_name.rfind(':') + 1:]
    module = __import__(mo, fromlist=[cl])
    the_class = getattr(module, cl)
    crystal_conf["storlets_gateway_module"] = the_class

    """ Load Storlets Gateway configuration """
    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf',
                               '/etc/swift/storlet_docker_gateway.conf'))
    additional_items = configParser.items("DEFAULT")

    for key, val in additional_items:
        crystal_conf[key] = val

    """ Register Lua script to retrieve policies in a single redis call """
    r = redis.StrictRedis(crystal_conf['redis_host'],
                          crystal_conf['redis_port'],
                          crystal_conf['redis_db'])
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
        local t3 = redis.call('HGETALL', 'global_filters')
        for i=1,#t3 do
          t[#t+1] = t3[i]
        end
        return t"""
    lua_sha = r.script_load(lua)
    crystal_conf['LUA_get_pipeline_sha'] = lua_sha

    def crystal_filter_handler(app):
        return CrystalHandlerMiddleware(app, conf, crystal_conf)

    return crystal_filter_handler
