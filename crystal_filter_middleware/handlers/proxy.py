from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed, Response
from swift.common.utils import public, InputProxy
from copy import deepcopy
import mimetypes
import operator
import json
import urllib


mappings = {'>': operator.gt, '>=': operator.ge,
            '==': operator.eq, '<=': operator.le, '<': operator.lt,
            '!=': operator.ne, "OR": operator.or_, "AND": operator.and_}


class CrystalProxyHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger, filter_control):
        super(CrystalProxyHandler, self).__init__(request, conf,
                                                  app, logger,
                                                  filter_control)
        self.etag = ''

    def _get_dynamic_policies(self):
        # Dynamic binding of policies: using a Lua script that executes
        # a hgetall on the first matching key of a list and also returns
        # the global filters
        lua_sha = self.conf.get('LUA_get_pipeline_sha')
        args = (self.account.replace('AUTH_', ''), self.container, self.obj)
        redis_list = self.redis.evalsha(lua_sha, 0, *args)
        index = redis_list.index("@@@@")  # Separator between pipeline and global filters

        self.filter_list = dict(zip(redis_list[0:index:2], redis_list[1:index:2]))
        self.global_filters = dict(zip(redis_list[index+1::2], redis_list[index+2::2]))

    def _parse_vaco(self):
        return self.request.split_path(4, 4, rest_with_last=True)

    def _get_object_type(self):
        object_type = self.request.headers['Content-Type']
        if not object_type:
            object_type = mimetypes.guess_type(self.request.environ['PATH_INFO'])[0]
        return object_type

    def handle_request(self):

        if self.is_crystal_valid_request and hasattr(self, self.request.method):
            try:
                self._get_dynamic_policies()
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            self.logger.info('Request disabled for Crystal')
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

    def _parse_filter_metadata(self, filter_metadata):
        """
        This method parses the filter metadata
        """
        filter_name = filter_metadata['filter_name']
        language = filter_metadata["language"]
        server = filter_metadata["execution_server"]
        params = filter_metadata["params"]
        filter_type = filter_metadata["filter_type"]
        filter_main = filter_metadata["main"]
        filter_dep = filter_metadata["dependencies"]
        filter_size = filter_metadata["content_length"]
        reverse = filter_metadata["reverse"]

        when = filter_metadata[self.method]

        if filter_type == 'storlet':
            if self.method == 'put':
                when = 'on_pre_put'
            elif self.method == 'get':
                when = 'on_post_get'
        else:
            if when == 'Request':
                when = 'on_pre_'+self.method
            if when == 'Response':
                when = 'on_post_'+self.method
            if when == 'Request/Response':
                when = "on_both_"+self.method

        if server == 'Proxy Node':
            server = 'proxy'
        elif server == 'Storage Node':
            server = 'object'

        if reverse:
            if reverse == 'Proxy Node':
                reverse = 'proxy'
            elif reverse == 'Storage Node':
                reverse = 'object'

        filter_data = {'name': filter_name,
                       'language': language,
                       'params': self._parse_csv_params(params),
                       'execution_server': server,
                       'reverse': reverse,
                       'type': filter_type,
                       'main': filter_main,
                       'dependencies': filter_dep,
                       'size': filter_size,
                       'when': when}

        return filter_data

    def _build_filter_execution_list(self):
        """
        This method builds the filter execution list (ordered).
        """
        filter_execution_list = {}

        ''' Parse global filters '''
        for _, filter_metadata in self.global_filters.items():
            filter_metadata = json.loads(filter_metadata)
            if filter_metadata[self.method]:

                filter_data = self._parse_filter_metadata(filter_metadata)
                order = filter_metadata["execution_order"]
                filter_execution_list[int(order)] = filter_data

        ''' Parse Project specific filters'''
        for _, filter_metadata in self.filter_list.items():
            filter_metadata = json.loads(filter_metadata)

            if filter_metadata[self.method]:
                filter_data = self._parse_filter_metadata(filter_metadata)
                order = filter_metadata["execution_order"]

                filter_execution_list[order] = filter_data

        return filter_execution_list

    def _format_crystal_metadata(self, crystal_md):
        """
        This method generates the metadata that will be stored alongside the
        object in the PUT requests. It allows the reverse case of the filters
        without querying the centralized controller.
        """
        for key in crystal_md["filter-list"].keys():
            cfilter = crystal_md["filter-list"][key]
            if cfilter['reverse']:
                cfilter['when'] = 'on_post_get'
                current_params = cfilter['params']
                if current_params:
                    cfilter['params']['reverse'] = 'True'
                else:
                    cfilter['params'] = {'reverse': 'True'}

                cfilter['execution_server'] = cfilter['reverse']
                cfilter.pop('reverse')
            else:
                crystal_md["filter-list"].pop(key)

        return crystal_md

    def _set_crystal_metadata(self, filter_exec_list):
        """
        This method generates the metadata that will be stored alongside the
        object in the PUT requests. It allows the reverse case of the filters
        without querying the centralized controller.
        """
        crystal_md = {}
        crystal_md["original-etag"] = self.request.headers.get('ETag', '')
        crystal_md["original-size"] = self.request.headers.get('Content-Length', '')
        crystal_md["filter-list"] = deepcopy(filter_exec_list)
        cmd = self._format_crystal_metadata(crystal_md)
        self.request.headers['X-Object-Sysmeta-Crystal'] = cmd

    def _parse_csv_params(self, csv_params):
        """
        Provides comma separated parameters "a=1,b=2" as a dictionary
        """
        # self.logger.info('csv_params: ' + csv_params)
        params_dict = dict()
        plist = csv_params.split(",")
        plist = filter(None, plist)  # Remove empty strings
        for p in plist:
            k, v = p.strip().split('=')
            params_dict[k] = v
        return params_dict

    def _parse_headers_params(self):
        """
        Extract parameters from headers
        """
        parameters = dict()
        for param in self.request.headers:
            if param.lower().startswith('x-crystal-parameter'):
                keyvalue = self.request.headers[param]
                keyvalue = urllib.unquote(keyvalue)
                [key, value] = keyvalue.split(':')
                parameters[key] = value
        return parameters

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        if 'Etag' in self.request.headers.keys():
            self.etag = self.request.headers.pop('Etag')

        if self.global_filters or self.filter_list:
            self.logger.info('There are Filters to execute')
            filter_exec_list = self._build_filter_execution_list()
            self.logger.info('' + str(filter_exec_list))
            self.request.headers['crystal/filters'] = json.dumps(filter_exec_list)
            self.apply_filters_on_pre_get(filter_exec_list)

        if not isinstance(self.request.environ['wsgi.input'], InputProxy):
            if not hasattr(self.request, 'response_headers'):
                self.request.response_headers = None
            return Response(app_iter=self.request.environ['wsgi.input'],
                            headers=self.request.response_headers,
                            request=self.request)

        response = self.request.get_response(self.app)

        if 'crystal/filters' in response.headers:
            self.logger.info('There are filters to execute'
                             ' from object server')
            filter_list = json.loads(response.headers.pop('crystal/filters'))
            response = self.apply_filters_on_post_get(response, filter_list)

        if 'Content-Length' in response.headers:
            response.headers.pop('Content-Length')
        if 'Transfer-Encoding' in response.headers:
            response.headers.pop('Transfer-Encoding')

        if 'etag' in self.request.headers.keys():
            response.headers['etag'] = self.etag

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if 'Etag' in self.request.headers.keys():
            self.etag = self.request.headers.pop('Etag')

        if self.global_filters or self.filter_list:
            self.logger.info('There are Filters to execute')
            filter_exec_list = self._build_filter_execution_list()
            self.logger.info('' + str(filter_exec_list))
            if filter_exec_list:
                self._set_crystal_metadata(filter_exec_list)
                self.apply_filters_on_pre_put(filter_exec_list)
        else:
            self.logger.info('No filters to execute')

        response = self.request.get_response(self.app)

        if 'Etag' in self.request.headers.keys():
            response.headers['Etag'] = self.etag

        return response
