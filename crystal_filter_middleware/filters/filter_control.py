import json
import sys

try:
    from crystal_filter_middleware.gateways.storlet import CrystalGatewayStorlet
    STORLETS = True
except:
    STORLETS = False

from swift.common.swob import Request


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CrystalFilterControl(object):
    __metaclass__ = Singleton

    def __init__(self, conf, log):
        self.logger = log
        self.conf = conf
        self.server = self.conf.get('execution_server')

        # Add source directory to sys path
        native_filters_path = self.conf.get('native_filters_path')
        sys.path.insert(0, native_filters_path)

    def _setup_storlet_gateway(self, conf, logger):
        return CrystalGatewayStorlet(conf, logger)

    def _load_native_filter(self, filter_data):
        modulename = filter_data['name'].split('.')[0]
        classname = filter_data['main']
        m = __import__(modulename, globals(),
                       locals(), [classname])
        m_class = getattr(m, classname)
        metric_class = m_class(global_conf=self.conf,
                               filter_conf=filter_data,
                               logger=self.logger)

        return metric_class

    def _get_data_iter(self, req_resp):
        if isinstance(req_resp, Request):
            reader = req_resp.environ['wsgi.input'].read
            data_iter = iter(lambda: reader(65536), '')
        else:
            data_iter = req_resp.app_iter

        return data_iter

    def _execute_storlet_filter(self, req_resp, data_iter, filter_data):
        """
        Storlet Filter Execution method
        """
        self.logger.info('Go to execute storlet filter: ' + filter_data['main'])
        storlet_gateway = self._setup_storlet_gateway(self.conf, self.logger)
        return storlet_gateway.execute(req_resp, data_iter, filter_data)

    def _execute_native_filter(self, req_resp, data_iter, filter_data):
        """
        Native Filter execution method
        """
        self.logger.info('Go to execute native filter: ' + filter_data['main'])
        native_filter = self._load_native_filter(filter_data)
        parameters = filter_data['params']
        return native_filter.execute(req_resp, data_iter, parameters)

    def execute_filters(self, req_resp, filter_exec_list):
        """
        Entry Point for executing all filters
        :param req_resp: swift.common.swob.Request or swift.common.swob.Response instance
        :param filter_exec_list: list of filters to execute
        :returns req_resp: swift.common.swob.Request or
                           swift.common.swob.Response instance with a new data_iter
        """
        on_other_server = dict()
        filter_executed = False

        data_iter = self._get_data_iter(req_resp)

        for key in sorted(filter_exec_list):
            filter_data = filter_exec_list[key]
            server = filter_data["execution_server"]
            filter_type = filter_data['type']
            if server == self.server and filter_type == 'storlet' and STORLETS:
                data_iter = self._execute_storlet_filter(req_resp, data_iter, filter_data)
                filter_executed = True

            elif server == self.server and filter_type == 'native':
                data_iter = self._execute_native_filter(req_resp, data_iter, filter_data)
                filter_executed = True

            else:
                on_other_server[key] = filter_exec_list[key]

        if on_other_server:
            req_resp.headers['crystal/filters'] = json.dumps(on_other_server)

        if filter_executed and isinstance(req_resp, Request):
            req_resp.environ['wsgi.input'] = data_iter
        else:
            req_resp.app_iter = data_iter

        return req_resp
