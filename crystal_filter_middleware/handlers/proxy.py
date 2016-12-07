from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import public
from copy import deepcopy
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
        self.etag = ''
        # Dynamic binding of policies
        account_key_list = self.redis.keys("pipeline:" + str(self.account) + "*")
        self.global_filters = self.redis.hgetall('global_filters')
        self.filter_list = None
        key = self.account + ":" + self.container + ":" + self.obj
        for target in range(3):
            self.target_key = key.rsplit(":", target)[0]
            if 'pipeline:' + self.target_key in account_key_list:
                self.filter_list = self.redis.hgetall('pipeline:' + self.target_key)
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
        for _, filter_metadata in self.global_filters.items():
            filter_metadata = json.loads(filter_metadata)
            if (filter_metadata["is_pre_" + self.method] or
                    filter_metadata["is_post_" + self.method]):

                filter_main = filter_metadata["main"]
                filter_type = filter_metadata["filter_type"]
                server = filter_metadata["execution_server"]
                filter_dep = filter_metadata["dependencies"]
                has_reverse = filter_metadata["has_reverse"]
                reverse = filter_metadata["execution_server_reverse"]
                order = filter_metadata["execution_order"]

                if filter_metadata["is_pre_" + self.method]:
                    when = "on_pre_" + self.method
                elif filter_metadata["is_post_" + self.method]:
                    when = "on_post_" + self.method

                filter_data = {'main': filter_main,
                               'execution_server': server,
                               'execution_server_reverse': reverse,
                               'type': filter_type,
                               'dependencies': filter_dep,
                               'has_reverse': has_reverse,
                               'when': when}

                filter_execution_list[int(order)] = filter_data

        storlet_request = False
        if 'X-Run-Storlet' in self.request.headers:
            storlet_request = True
            storlet = self.request.headers.pop('X-Run-Storlet')

        ''' Parse filter list (Storlet and Native)'''
        if self.filter_list:
            for _, filter_metadata in self.filter_list.items():
                filter_metadata = json.loads(filter_metadata)

                if filter_metadata["is_pre_" + self.method] or \
                   filter_metadata["is_post_" + self.method]:

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

                        if filter_metadata["is_pre_" + self.method]:
                            when = "on_pre_" + self.method
                        elif filter_metadata["is_post_" + self.method]:
                            when = "on_post_" + self.method

                        filter_data = {'name': filter_name,
                                       'params': params,
                                       'execution_server': server,
                                       'execution_server_reverse': reverse,
                                       'id': filter_id,
                                       'type': filter_type,
                                       'main': filter_main,
                                       'dependencies': filter_dep,
                                       'size': filter_size,
                                       'has_reverse': has_reverse,
                                       'when': when}

                        launch_key = int(filter_metadata["execution_order"]) +\
                            len(filter_execution_list)

                        filter_execution_list[launch_key] = filter_data

                        if storlet_request:
                            if storlet == filter_data['name']:
                                self.request.headers['X-Run-Storlet'] = storlet
                                filter_execution_list.pop(launch_key)

        return filter_execution_list

    def _format_crystal_metadata(self, crystal_md):
        for key in crystal_md["filter-list"].keys():
            cfilter = crystal_md["filter-list"][key]
            if cfilter['type'] != 'global' and cfilter['has_reverse']:
                cfilter.pop('has_reverse')
                cfilter['when'] = 'on_post_get'
                current_params = cfilter['params']
                if current_params:
                    cfilter['params'] = current_params + ',' + 'reverse=True'
                else:
                    cfilter['params'] = 'reverse=True'

                cfilter['execution_server'] = cfilter['execution_server_reverse']
                cfilter.pop('execution_server_reverse')
            else:
                crystal_md["filter-list"].pop(key)

        return crystal_md

    def _set_crystal_metadata(self, filter_exec_list):
        crystal_md = {}
        crystal_md["original-etag"] = self.request.headers.get('ETag', '')
        crystal_md["original-size"] = self.request.headers.get('Content-Length', '')
        crystal_md["filter-list"] = deepcopy(filter_exec_list)
        cmd = self._format_crystal_metadata(crystal_md)
        self.request.headers['X-Object-Sysmeta-Crystal'] = cmd

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        if self.global_filters or self.filter_list:
            self.logger.info('Crystal Filters - There are Filters to execute')
            filter_list = self._build_filter_execution_list()
            self.request.headers['crystal/filters'] = json.dumps(filter_list)
            self.apply_filters_on_pre_get(filter_list)

        response = self.request.get_response(self.app)

        if 'crystal/filters' in response.headers:
            self.logger.info('Crystal Filters - There are filters to execute'
                             ' from object server')
            filter_list = json.loads(response.headers.pop('crystal/filters'))
            response = self.apply_filters_on_post_get(response, filter_list)

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if self.global_filters or self.filter_list:
            self.logger.info('Crystal Filters - There are Filters to execute')
            filter_list = self._build_filter_execution_list()
            if filter_list:
                self._set_crystal_metadata(filter_list)
                if 'ETag' in self.request.headers:
                    # The object goes to be modified by some Filter, so we
                    # delete the Etag from request headers to prevent checksum
                    # verification.
                    self.etag = self.request.headers.pop('ETag')
                self.apply_filters_on_pre_put(filter_list)
        else:
            self.logger.info('Crystal Filters - No filters to execute')

        response = self.request.get_response(self.app)
        response.headers['ETag'] = self.etag

        return response
