from crystal_filter_middleware.handlers import CrystalBaseHandler
from crystal_filter_middleware.utils.common import get_metadata, put_metadata
from swift.common.swob import HTTPMethodNotAllowed
from swift.common.utils import public
import json


class CrystalObjectHandler(CrystalBaseHandler):

    def __init__(self, request, conf, app, logger,filter_control):
        super(CrystalObjectHandler, self).__init__(request, conf, 
                                                     app, logger,
                                                     filter_control) 
        
        self.device = self.request.environ['PATH_INFO'].split('/',2)[1]

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
            req_filter_list = json.loads(self.request.headers.pop('CRYSTAL-FILTERS'))
            for key in sorted(req_filter_list, reverse=True):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = req_filter_list[key]

        return new_storlet_list

    def _get_crystal_metadata(self):
        crystal_md = {}
        filter_exec_list = json.loads(self.request.headers['Filter-Executed-List'])
        crystal_md["original-etag"] = self.request.headers['Original-Etag']
        crystal_md["original-size"] = self.request.headers['Original-Size']
        crystal_md["filter-exec-list"] = filter_exec_list

        return crystal_md

    @public
    def GET(self):
        """
        GET handler on Object
        If orig_resp is GET we will need to:
        - Take the object metadata info
        - Execute the storlets described in the metadata info
        - Execute the storlets described in redis
        - Return the result
        """

        resp = self.request.get_response(self.app)

        if (resp.status_int == 200 or resp.status_int == 201):
            crystal_md = get_metadata(resp)
            
            if crystal_md:
                resp.headers['ETag'] = crystal_md['original-etag']
                resp.headers['Content-Length'] = crystal_md['original-size']
            
            exec_list = crystal_md.get('filter-exec-list',None)
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
        
        original_resp = self.request.get_response(self.app)
        
        # 'Storlet-List' header is the list of all Storlets executed, both 
        # on Proxy and on Object servers. It is necessary to save the list 
        # in the extended metadata of the object for run reverse-Storlet on 
        # GET requests.
        if 'Filter-Executed-List' in self.request.headers:
            crystal_metadata = self._get_crystal_metadata()
            if not put_metadata(self.app, self.request, crystal_metadata):
                self.app.logger.error('Crystal Filters - Error writing'
                                      'metadata in an object')
                # TODO: Rise exception writting metadata
            # We need to restore the original ETAG to avoid checksum 
            # verification of Swift clients
            original_resp.headers['ETag'] = crystal_metadata['original-etag']
                
        return original_resp
