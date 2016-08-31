from crystal_filter_middleware.handlers import CrystalBaseHandler
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import public
import json


class CrystalObjectHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger, filter_control):
        super(CrystalObjectHandler, self).__init__(request, conf,
                                                   app, logger,
                                                   filter_control)

        self.device = self.request.environ['PATH_INFO'].split('/', 2)[1]

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('0', acc, cont, obj)

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.request.params.get('multipart-manifest') == 'get'

    def handle_request(self):
        if self.is_crystal_object_put:
            return self.request.get_response(self.app)

        if hasattr(self, self.request.method):
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # un-defined method should be NOT ALLOWED
            # return HTTPMethodNotAllowed(request=self.request)

    def _augment_filter_execution_list(self, filter_list):
        new_storlet_list = {}

        # REVERSE EXECUTION
        if filter_list:
            for key in reversed(sorted(filter_list)):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = filter_list[key]

        # Get filter list to execute from proxy
        if 'CRYSTAL-FILTERS' in self.request.headers:
            req_filter_list = json.loads(
                self.request.headers.pop('CRYSTAL-FILTERS'))
            for key in sorted(req_filter_list, reverse=True):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = req_filter_list[key]

        return new_storlet_list

    def _format_crystal_metadata(self, crystal_md):
        for key in crystal_md["filter-exec-list"].keys():
            cfilter = crystal_md["filter-exec-list"][key]
            if cfilter['type'] != 'global' and cfilter['has_reverse']:
                current_params = cfilter['params']
                if current_params:
                    cfilter['params'] = current_params + ',' + 'reverse=True'
                else:
                    cfilter['params'] = 'reverse=True'

                cfilter['execution_server'] = cfilter[
                    'execution_server_reverse']
                cfilter.pop('execution_server_reverse')
            else:
                crystal_md["filter-exec-list"].pop(key)

        print crystal_md
        return crystal_md

    def _set_crystal_metadata(self):
        crystal_md = {}
        filter_exec_list = json.loads(
            self.request.headers['Filter-Executed-List'])
        crystal_md["original-etag"] = self.request.headers['Original-Etag']
        crystal_md["original-size"] = self.request.headers['Original-Size']
        crystal_md["filter-exec-list"] = filter_exec_list
        cmd = self._format_crystal_metadata(crystal_md)
        self.request.headers['X-Object-Sysmeta-Crystal'] = cmd

    @public
    def GET(self):
        """
        GET handler on Object
        """
        resp = self.request.get_response(self.app)
        if 'X-Object-Sysmeta-Crystal' in resp.headers:
            crystal_md = eval(resp.headers['X-Object-Sysmeta-Crystal'])
            if crystal_md:
                resp.headers['ETag'] = crystal_md['original-etag']
                resp.headers['Content-Length'] = crystal_md['original-size']

            exec_list = crystal_md.get('filter-exec-list', None)
            filter_exec_list = self._augment_filter_execution_list(exec_list)

            if filter_exec_list:
                resp = self.apply_filters_on_get(resp, filter_exec_list)
        return resp

    @public
    def PUT(self):
        """
        PUT handler on Object Server
        """
        # IF 'CRYSTAL-FILTERS' is in headers, means that is needed to run a
        # Filter on Object Server before store the object.
        if 'CRYSTAL-FILTERS' in self.request.headers:
            self.logger.info('Crystal Filters - There are filters to execute')
            filter_list = json.loads(self.request.headers['CRYSTAL-FILTERS'])
            self.apply_filters_on_put(filter_list)

        self._set_crystal_metadata()
        original_resp = self.request.get_response(self.app)
        # We need to restore the original ETAG to avoid checksum
        # verification of Swift clients
        original_resp.headers['ETag'] = self.request.headers['Original-Etag']

        return original_resp
