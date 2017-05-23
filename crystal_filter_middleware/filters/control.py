import json
import os
import sys

from crystal_filter_middleware.gateways.storlet import CrystalGatewayStorlet
from swift.common.swob import Request
from swift.common.utils import InputProxy

PACKAGE_NAME = __name__.split('.')[0]


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

        # Add source directories to sys path
        global_native_filters_path = os.path.join('/opt', 'crystal', 'global_native_filters')
        native_filters_path = os.path.join('/opt', 'crystal', 'native_filters')
        sys.path.insert(0, global_native_filters_path)
        sys.path.insert(0, native_filters_path)

    def _setup_storlet_gateway(self, conf, logger, request_data):
        return CrystalGatewayStorlet(conf, logger, request_data)

    def _load_native_filter(self, filter_data):
        (modulename, classname) = filter_data['main'].rsplit('.', 1)

        # m = __import__(PACKAGE_NAME + '.filters.' + modulename, globals(),
        m = __import__(modulename, globals(),
                       locals(), [classname])
        m_class = getattr(m, classname)
        metric_class = m_class(filter_conf=filter_data,
                               global_conf=self.conf,
                               logger=self.logger)

        return metric_class

    def execute_filters(self, req_resp, filter_exec_list, app,
                        api_version, account, container, obj, method):

        request_data = dict()
        request_data['app'] = app
        request_data['api_version'] = api_version
        request_data['account'] = account
        request_data['container'] = container
        request_data['object'] = obj
        request_data['method'] = method

        on_other_server = dict()
        filter_executed = False
        storlet_filter = None

        if isinstance(req_resp, Request):
            reader = req_resp.environ['wsgi.input'].read
            crystal_iter = iter(lambda: reader(65536), '')
        else:
            crystal_iter = req_resp.app_iter

        for key in sorted(filter_exec_list):
            filter_data = filter_exec_list[key]
            server = filter_data["execution_server"]
            if server == self.server:
                if method == 'get' and isinstance(req_resp, Request) and filter_executed and not isinstance(crystal_iter, InputProxy):
                    self.logger.info('Crystal Filters - A previous filter generated a response. Ignoring filter ' + filter_data['main'])
                    continue

                if filter_data['type'] == 'storlet':
                    """ Storlet Filter Execution """
                    if not storlet_filter:
                        storlet_filter = self._setup_storlet_gateway(self.conf,
                                                                     self.logger,
                                                                     request_data)
                    # setting the default supported language TODO: support python storlets
                    filter_data['language'] = 'java'
                    crystal_iter = storlet_filter.execute(req_resp,
                                                          filter_data,
                                                          crystal_iter)
                    filter_executed = True

                else:
                    """ Native Filter execution """
                    self.logger.info('Crystal Filters - Go to execute native'
                                     ' filter: ' + filter_data['main'])

                    native_filter = self._load_native_filter(filter_data)
                    crystal_iter = native_filter.execute(req_resp, crystal_iter,
                                                         request_data)

                    filter_executed = True

            else:
                on_other_server[key] = filter_exec_list[key]

        if on_other_server:
            req_resp.headers['crystal/filters'] = json.dumps(on_other_server)

        if filter_executed:
            if isinstance(req_resp, Request):
                req_resp.environ['wsgi.input'] = crystal_iter
            else:
                req_resp.app_iter = crystal_iter

        return req_resp
