from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import public
import mimetypes
import operator
import json

mappings = {'>': operator.gt, '>=': operator.ge,
            '==': operator.eq, '<=': operator.le, '<': operator.lt,
            '!=': operator.ne, "OR": operator.or_, "AND": operator.and_}


class CrystalProxyHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger, filter_control):
        super(CrystalProxyHandler, self).__init__(request, conf,
                                                  app, logger,
                                                  filter_control)

        # Dynamic binding of policies
        account_key_list = self.redis.keys(
            "pipeline:" + str(self.account) + "*")
        self.global_filters = self.redis.hgetall('global_filters')

        self.filter_list = None
        key = self.account + "/" + self.container + "/" + self.obj
        for target in range(3):
            self.target_key = key.rsplit("/", target)[0]
            if 'pipeline:' + self.target_key in account_key_list:
                self.filter_list = self.redis.hgetall(
                    'pipeline:' + self.target_key)
                break

    def _parse_vaco(self):
        return self.request.split_path(4, 4, rest_with_last=True)

    def _get_object_type(self):
        object_type = self.request.headers['Content-Type']
        if not object_type:
            object_type = mimetypes.guess_type(
                self.request.environ['PATH_INFO'])[0]
        return object_type

    @property
    def is_proxy_runnable(self, resp):
        # SLO / proxy only case:
        # storlet to be invoked now at proxy side:
        runnable = any([self.is_range_request, self.is_slo_response(resp),
                        self.conf['storlet_execute_on_proxy_only']])
        return runnable

    def handle_request(self):

        if self.is_crystal_object_put:
            return self.request.get_response(self.app)

        if self.is_account_storlet_enabled():
            if hasattr(self, self.request.method):
                try:
                    handler = getattr(self, self.request.method)
                    getattr(handler, 'publicly_accessible')
                except AttributeError:
                    return HTTPMethodNotAllowed(request=self.request)
                return handler()
        else:
            self.logger.info('SDS Storlets - Account disabled for Storlets')

        return self.request.get_response(self.app)

    def _check_size_type(self, filter_metadata):

        correct_type = True
        correct_size = True

        if filter_metadata['object_type']:
            obj_type = filter_metadata['object_type']
            correct_type = self._get_object_type() in \
                self.redis.lrange("object_type:" + obj_type, 0, -1)

        if filter_metadata['object_size']:
            object_size = filter_metadata['object_size']
            op = mappings[object_size[0]]
            obj_lenght = int(object_size[1])

            correct_size = op(int(self.request.headers['Content-Length']),
                              obj_lenght)

        return correct_type and correct_size

    def _build_filter_execution_list(self):
        filter_execution_list = dict()

        ''' Parse global filters '''
        for key, filter_metadata in self.global_filters.items():
            filter_metadata = json.loads(filter_metadata)
            if filter_metadata["is_" + self.method]:
                filter_main = filter_metadata["main"]
                filter_type = 'global'
                server = filter_metadata["execution_server"]
                filter_execution = {'main': filter_main,
                                    'execution_server': server,
                                    'type': filter_type}
                filter_execution_list[int(key)] = filter_execution

        ''' Parse filter list '''
        if self.filter_list:
            for _, filter_metadata in self.filter_list.items():
                filter_metadata = json.loads(filter_metadata)

                # Check conditions
                if filter_metadata["is_" + self.method]:
                    if self._check_size_type(filter_metadata):
                        filter_name = filter_metadata['filter_name']
                        server = filter_metadata["execution_server"]
                        reverse = filter_metadata["execution_server_reverse"]
                        params = filter_metadata["params"]
                        filter_id = filter_metadata["filter_id"]
                        filter_type = filter_metadata["filter_type"]
                        filter_main = filter_metadata["main"]
                        filter_dep = filter_metadata["dependencies"]
                        filter_size = filter_metadata["content_length"]
                        has_reverse = filter_metadata["has_reverse"]

                        filter_execution = {'name': filter_name,
                                            'params': params,
                                            'execution_server': server,
                                            'execution_server_reverse': reverse,
                                            'id': filter_id,
                                            'type': filter_type,
                                            'main': filter_main,
                                            'dependencies': filter_dep,
                                            'size': filter_size,
                                            'has_reverse': has_reverse}

                        launch_key = filter_metadata["execution_order"]
                        filter_execution_list[launch_key] = filter_execution

        return filter_execution_list

    @public
    def GET(self):
        """
        GET handler on Proxy
        """

        if self.global_filters or self.filter_list:
            self.app.logger.info(
                'Crystal Filters - There are Filters to execute')
            filter_exec_list = self._build_filter_execution_list()
            self.request.headers[
                'CRYSTAL-FILTERS'] = json.dumps(filter_exec_list)

        # TODO(josep): cache filter should be applied here PRE-GET

        resp = self.request.get_response(self.app)

        if 'CRYSTAL-FILTERS' in resp.headers:
            self.logger.info('Crystal Filters - There are filters to execute '
                             'from object server')
            filter_exec_list = json.loads(resp.headers.pop('CRYSTAL-FILTERS'))
            resp = self.apply_filters_on_get(resp, filter_exec_list)

        return resp

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if self.global_filters or self.filter_list:
            self.app.logger.info(
                'Crystal Filters - There are Filters to execute')
            filter_exec_list = self._build_filter_execution_list()
            if filter_exec_list:
                self.request.headers['Filter-Executed-List'] = json.dumps(filter_exec_list)
                self.request.headers['Original-Size'] = self.request.headers.get('Content-Length', '')
                self.request.headers['Original-Etag'] = self.request.headers.get('ETag', '')

                if 'ETag' in self.request.headers:
                    # The object goes to be modified by some Storlet, so we
                    # delete the Etag from request headers to prevent checksum
                    # verification.
                    self.request.headers.pop('ETag')

                self.apply_filters_on_put(filter_exec_list)

            else:
                self.logger.info('Crystal Filters - No filters to execute')
        else:
            self.logger.info('Crystal Filters - No filters to execute')

        return self.request.get_response(self.app)