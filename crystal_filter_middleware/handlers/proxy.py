from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.wsgi import make_subrequest
from swift.common.utils import public
import operator
import json
import copy
import urllib
import os
import re

mappings = {'>': operator.gt, '>=': operator.ge,
            '==': operator.eq, '<=': operator.le, '<': operator.lt,
            '!=': operator.ne, "OR": operator.or_, "AND": operator.and_}


class CrystalProxyHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger):
        super(CrystalProxyHandler, self).__init__(request, conf,
                                                  app, logger)
        self.etag = None
        self.filter_exec_list = None

    def _get_dynamic_filters(self):
        # Dynamic binding of policies: using a Lua script that executes
        # a hgetall on the first matching key of a list and also returns
        # the global filters
        lua_sha = self.conf.get('LUA_get_pipeline_sha')
        args = (self.account.replace('AUTH_', ''), '' if self.container is None else self.container)
        redis_list = self.redis.evalsha(lua_sha, 0, *args)
        index = redis_list.index("@@@@")  # Separator between pipeline and global filters

        self.filter_list = dict(zip(redis_list[0:index:2], redis_list[1:index:2]))
        self.global_filters = dict(zip(redis_list[index+1::2], redis_list[index+2::2]))

        self.proxy_filter_exec_list = {}
        self.object_filter_exec_list = {}

        if self.global_filters or self.filter_list:
            self.proxy_filter_exec_list = self._build_filter_execution_list('proxy')
            self.object_filter_exec_list = self._build_filter_execution_list('object')

    def _parse_vaco(self):
        return self.request.split_path(2, 4, rest_with_last=True)

    def handle_request(self):

        if self.is_crystal_valid_request and hasattr(self, self.request.method):
            try:
                self._get_dynamic_filters()
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            self.logger.info('Request disabled for Crystal')
            return self.request.get_response(self.app)

    def _check_conditions(self, filter_metadata):
        """
        This method ckecks the object_tag, object_type and object_size parameters
        introduced by the dashborad to run the filter.
        """
        if not filter_metadata['object_type'] and \
           not filter_metadata['object_tag'] and \
           not filter_metadata['object_size']:
            return True

        metadata = {}
        if self.method == 'put':
            for key in self.request.headers.keys():
                metadata[key.lower()] = self.request.headers.get(key)
        else:
            sub_req = make_subrequest(self.request.environ, method='HEAD',
                                      path=self.request.path_info,
                                      headers=self.request.headers,
                                      swift_source='Crystal Filter Middleware')
            resp = sub_req.get_response(self.app)
            metadata = resp.headers

        correct_type = True
        correct_size = True
        correct_tags = True

        try:
            if filter_metadata['object_type']:
                object_name = filter_metadata['object_name']
                filename = self.request.environ['PATH_INFO']
                pattern = re.compile(object_name)
                if not pattern.search(filename):
                    correct_type = False

            if filter_metadata['object_tag']:
                tags = filter_metadata['object_tag'].split(',')
                tag_checking = list()
                for tag in tags:
                    key, value = tag.split(':')
                    meta_key = ('X-Object-Meta-'+key).lower()
                    sysmeta_key = ('X-Object-Sysmeta-Meta-'+key).lower()
                    correct_tag = (meta_key in metadata and
                                   metadata[meta_key] == value) or \
                                  (sysmeta_key in metadata and
                                   metadata[sysmeta_key] == value)
                    tag_checking.append(correct_tag)
                correct_tags = all(tag_checking)

            if filter_metadata['object_size']:
                object_size = filter_metadata['object_size']
                op = mappings[object_size[0]]
                obj_lenght = int(object_size[1])
                correct_size = op(int(metadata['Content-Length']),
                                  obj_lenght)
        except Exception as e:
            self.logger.error(str(e))
            return False

        return correct_type and correct_size and correct_tags

    def _parse_filter_metadata(self, filter_metadata):
        """
        This method parses the filter metadata
        """
        filter_name = filter_metadata['filter_name']
        language = filter_metadata["language"]
        params = filter_metadata["params"]
        filter_type = filter_metadata["filter_type"]
        filter_main = filter_metadata["main"]
        filter_dep = filter_metadata["dependencies"]
        filter_size = filter_metadata["content_length"]
        reverse = filter_metadata["reverse"]

        filter_data = {'name': filter_name,
                       'language': language,
                       'params': self._parse_csv_params(params),
                       'reverse': reverse,
                       'type': filter_type,
                       'main': filter_main,
                       'dependencies': filter_dep,
                       'size': filter_size}

        return filter_data

    def _build_filter_execution_list(self, server):
        """
        This method builds the filter execution list (ordered).
        """
        filter_execution_list = {}

        ''' Parse global filters '''
        for _, filter_metadata in self.global_filters.items():
            filter_metadata = json.loads(filter_metadata)
            if self.method in filter_metadata and filter_metadata[self.method] \
               and filter_metadata['execution_server'] == server \
               and self._check_conditions(filter_metadata):
                filter_data = self._parse_filter_metadata(filter_metadata)
                order = filter_metadata["execution_order"]
                filter_execution_list[int(order)] = filter_data

        ''' Parse Project specific filters'''
        for _, filter_metadata in self.filter_list.items():
            filter_metadata = json.loads(filter_metadata)
            if self.method in filter_metadata and filter_metadata[self.method] \
               and filter_metadata['execution_server'] == server \
               and self._check_conditions(filter_metadata):
                filter_data = self._parse_filter_metadata(filter_metadata)
                order = filter_metadata["execution_order"]

                filter_execution_list[order] = filter_data

        return filter_execution_list

    def _format_crystal_metadata(self, filter_list):
        """
        This method generates the metadata that will be stored alongside the
        object in the PUT requests. It allows the reverse case of the filters
        without querying the centralized controller.
        """
        for key in filter_list.keys():
            cfilter = filter_list[key]
            if cfilter['reverse'] != 'False':
                current_params = cfilter['params']
                if current_params:
                    cfilter['params']['reverse'] = 'True'
                else:
                    cfilter['params'] = {'reverse': 'True'}

                cfilter['execution_server'] = cfilter['reverse']
                cfilter.pop('reverse')
            else:
                filter_list.pop(key)

        return filter_list

    def _set_crystal_metadata(self):
        """
        This method generates the metadata that will be stored alongside the
        object in the PUT requests. It allows the reverse case of the filters
        without querying the centralized controller.
        """
        filter_exec_list = {}
        for key in sorted(self.proxy_filter_exec_list.keys()):
            filter_exec_list[len(filter_exec_list)] = self.proxy_filter_exec_list[key]

        for key in sorted(self.object_filter_exec_list.keys()):
            filter_exec_list[len(filter_exec_list)] = self.object_filter_exec_list[key]

        filter_list = copy.deepcopy(filter_exec_list)
        crystal_md = self._format_crystal_metadata(filter_list)
        if crystal_md:
            self.request.headers['X-Object-Sysmeta-Crystal'] = crystal_md

    def _save_size_and_etag(self):
        """
        Save original object Size and Etag
        """
        etag = self.request.headers.get('ETag', None)
        if etag:
            self.request.headers['X-Object-Sysmeta-Etag'] = etag
            self.request.headers['X-Backend-Container-Update-Override-Etag'] = etag

        size = self.request.headers.get('Content-Length')
        self.request.headers['X-Object-Sysmeta-Size'] = size
        self.request.headers['X-Backend-Container-Update-Override-Size'] = size

    def _recover_size_and_etag(self, response):
        """
        Recovers the original Object Size and Etag
        """
        if 'X-Object-Sysmeta-Size' in response.headers and self.obj:
            size = response.headers.pop('X-Object-Sysmeta-Size')
            response.headers['Content-Length'] = size

        if 'X-Object-Sysmeta-Etag' in response.headers and self.obj:
            etag = response.headers.pop('X-Object-Sysmeta-Etag')
            response.headers['etag'] = etag

        if 'Transfer-Encoding' in response.headers and self.obj:
                response.headers.pop('Transfer-Encoding')

    def _parse_csv_params(self, csv_params):
        """
        Provides comma separated parameters "a=1,b=2" as a dictionary
        """
        params_dict = dict()

        params = [x.strip() for x in csv_params.split('=')]
        for index in range(len(params)):
            if len(params) > index + 1:
                if index == 0:
                    params_dict[params[index]] = params[index + 1].rsplit(',', 1)[0].strip()
                elif index < len(params):
                    params_dict[params[index].rsplit(',', 1)[1].strip()] = params[index + 1].rsplit(',', 1)[0].strip()
                else:
                    params_dict[params[index].rsplit(',', 1)[1].strip()] = params[index + 1]

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
        """Handler for HTTP GET requests."""
        return self.GETorHEAD()

    @public
    def HEAD(self):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD()

    @public
    def POST(self):
        """Handler for HTTP POST requests."""
        return self.POSTorDELETE()

    @public
    def DELETE(self):
        """Handler for HTTP DELETE requests."""
        return self.POSTorDELETE()

    def GETorHEAD(self):
        """
        Handle HTTP GET or HEAD requests.
        """
        if self.proxy_filter_exec_list:
            self.logger.info('There are Filters to execute')
            self.logger.info(str(self.proxy_filter_exec_list))
            self._build_pipeline(self.proxy_filter_exec_list)
        else:
            self.logger.info('No Filters to execute')

        if self.object_filter_exec_list:
            object_server_filters = json.dumps(self.object_filter_exec_list)
            self.request.headers['crystal.filters'] = object_server_filters

        response = self.request.get_response(self.app)
        self._recover_size_and_etag(response)

        return response

    @public
    def PUT(self):
        """
        Handle HTTP PUT requests.
        """
        if self.proxy_filter_exec_list:
            self.logger.info('There are Filters to execute')
            self.logger.info(str(self.proxy_filter_exec_list))
            self._set_crystal_metadata()
            self._save_size_and_etag()
            self._build_pipeline(self.proxy_filter_exec_list)
        else:
            self.logger.info('No filters to execute')

        if self.object_filter_exec_list:
            object_server_filters = json.dumps(self.object_filter_exec_list)
            self.request.headers['crystal.filters'] = object_server_filters

        return self.request.get_response(self.app)

    @public
    def POSTorDELETE(self):
        """
        Handle HTTP POST or DELETE requests.
        """
        if self.proxy_filter_exec_list:
            self.logger.info('There are Filters to execute')
            self.logger.info(str(self.proxy_filter_exec_list))
            self._build_pipeline(self.proxy_filter_exec_list)
        else:
            self.logger.info('No filters to execute')

        if self.object_filter_exec_list:
            object_server_filters = json.dumps(self.object_filter_exec_list)
            self.request.headers['crystal.filters'] = object_server_filters

        return self.request.get_response(self.app)
