from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import public
import json


class CrystalObjectHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger):
        super(CrystalObjectHandler, self).__init__(request, conf,
                                                   app, logger)

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            3, 5, rest_with_last=True)
        return '0', acc, cont, obj

    def handle_request(self):
        if self.is_crystal_valid_request and hasattr(self, self.request.method):
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            self.logger.info('Request disabled for Crystal')
            return self.request.get_response(self.app)

    def _augment_filter_execution_list(self, filter_list):
        new_filter_list = {}

        # Reverse execution
        if filter_list:
            for key in reversed(sorted(filter_list)):
                launch_key = len(new_filter_list.keys())
                new_filter_list[launch_key] = filter_list[key]

        # Get filter list to execute from proxy server
        if 'crystal.filters' in self.request.headers:
            req_filter_list = json.loads(self.request.headers.pop('crystal.filters'))
            for key in sorted(req_filter_list, reverse=True):
                launch_key = len(new_filter_list.keys())
                new_filter_list[launch_key] = req_filter_list[key]

        return new_filter_list

    @public
    def GET(self):
        """
        GET handler on Object
        """
        response = self.request.get_response(self.app)

        if response.is_success:
            filter_list = None
            if 'X-Object-Sysmeta-Crystal' in response.headers:
                filter_list = eval(response.headers.pop('X-Object-Sysmeta-Crystal'))
            filter_exec_list = self._augment_filter_execution_list(filter_list)
            if filter_exec_list:
                self.logger.info('There are Filters to execute')
                self.logger.info(str(filter_exec_list))
                self._build_pipeline(filter_exec_list)
                response = self.request.get_response(self.app)
                response.headers.pop('X-Object-Sysmeta-Crystal')
            else:
                self.logger.info('No Filters to execute')

        return response

    @public
    def PUT(self):
        """
        PUT handler on Object Server
        """
        if 'crystal.filters' in self.request.headers:
            filter_exec_list = json.loads(self.request.headers['crystal.filters'])
            self._build_pipeline(filter_exec_list)

        return self.request.get_response(self.app)

    @public
    def POST(self):
        """
        POST handler on Object Server
        """
        if 'crystal.filters' in self.request.headers:
            filter_exec_list = json.loads(self.request.headers['crystal.filters'])
            self._build_pipeline(filter_exec_list)

        return self.request.get_response(self.app)

    @public
    def HEAD(self):
        """
        HEAD handler on Object Server
        """
        if 'crystal.filters' in self.request.headers:
            filter_exec_list = json.loads(self.request.headers['crystal.filters'])
            self._build_pipeline(filter_exec_list)

        return self.request.get_response(self.app)

    @public
    def DELETE(self):
        """
        DELETE handler on Object Server
        """
        if 'crystal.filters' in self.request.headers:
            filter_exec_list = json.loads(self.request.headers['crystal.filters'])
            self._build_pipeline(filter_exec_list)

        return self.request.get_response(self.app)
