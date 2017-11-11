from swift.proxy.controllers.base import get_account_info
from swift.common.utils import config_true_value
try:
    from crystal_filter_middleware.filters.storlet import StorletFilter
    STORLETS = True
except:
    STORLETS = False
import redis


class NotCrystalRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotCrystalRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class CrystalBaseHandler(object):
    """
    This is an abstract handler for Proxy/Object Server middleware
    """
    request = _request_instance_property()

    def __init__(self, request, conf, app, logger):
        """
        :param request: swob.Request instance
        :param conf: gateway conf dict
        """
        self.request = request
        self.server = conf.get('execution_server')
        self.sds_containers = [conf.get('storlet_container', 'storlet'),
                               conf.get('storlet_dependency', 'dependencies'),
                               conf.get('storlet_images', 'docker_images')]
        self.app = app
        self.logger = logger
        self.conf = conf

        self.redis_host = conf.get('redis_host')
        self.redis_port = conf.get('redis_port')
        self.redis_db = conf.get('redis_db')

        self.method = self.request.method.lower()

        self.redis = redis.StrictRedis(self.redis_host,
                                       self.redis_port,
                                       self.redis_db)

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        self._api_version, self._account, self._container, self._obj = \
            self._parse_vaco()

    @property
    def api_version(self):
        return self._api_version

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    @property
    def is_crystal_valid_request(self):
        if self.server == 'proxy':
            crystal_enabled = self.is_account_crystal_enabled()
        else:
            crystal_enabled = True
        crystal_container = self.container in self.sds_containers

        return (not crystal_container and crystal_enabled)

    def _parse_vaco(self):
        """
        Parse method of path from self.request which depends on child class
        (Proxy or Object)
        :return tuple: a string tuple of (version, account, container, object)
        """
        raise NotImplementedError()

    def handle_request(self):
        """
        Run storlet
        """
        raise NotImplementedError()

    @property
    def is_storlet_execution(self):
        return 'X-Run-Storlet' in self.request.headers

    @property
    def is_range_request(self):
        """
        Determines whether the request is a byte-range request
        """
        return 'Range' in self.request.headers

    def is_available_trigger(self):
        return any((True for x in self.available_triggers
                    if x in self.request.headers.keys()))

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.request.params.get('multipart-manifest') == 'get'

    def is_slo_response(self, resp):
        self.logger.debug(
            'Verify if {0}/{1}/{2} is an SLO assembly object'.format(
                self.account, self.container, self.obj))
        is_slo = 'X-Static-Large-Object' in resp.headers
        if is_slo:
            self.logger.debug(
                '{0}/{1}/{2} is indeed an SLO assembly '
                'object'.format(self.account, self.container, self.obj))
        else:
            self.logger.debug(
                '{0}/{1}/{2} is NOT an SLO assembly object'.format(
                    self.account, self.container, self.obj))
        return is_slo

    def is_account_crystal_enabled(self):
        account_meta = get_account_info(self.request.environ,
                                        self.app)['meta']
        crystal_enabled = account_meta.get('crystal-enabled',
                                           'False')

        if not config_true_value(crystal_enabled):
            return False

        return True

    def _load_native_filter(self, app, conf):
        filter_data = conf['filter_data']
        modulename = filter_data['name'].split('.')[0]
        classname = filter_data['main']
        m = __import__(modulename, globals(),
                       locals(), [classname])
        m_class = getattr(m, classname)
        filter_class = m_class(app, conf)

        return filter_class

    def _build_pipeline(self, filter_exec_list):
        app = self.app

        for key in sorted(filter_exec_list, reverse=True):
            filter_data = filter_exec_list[key]
            filter_type = filter_data['type']
            self.conf['filter_data'] = filter_data

            if filter_type == 'storlet' and STORLETS:
                app = StorletFilter(app, self.conf)
            elif filter_type == 'native':
                app = self._load_native_filter(app, self.conf)

        self.app = app
