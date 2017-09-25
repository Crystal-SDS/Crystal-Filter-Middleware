class AbstractFilter(object):

    def __init__(self, global_conf, filter_conf, logger):
        self.logger = logger
        self.global_conf = global_conf
        self.filter_conf = filter_conf

    def execute(self, req_resp, data_iter, parameters):
        """
        Entry point (This method must be maintained unmodified: for all filters
        the entry point will be the same)

        :param req_resp: swift.common.swob.Request or swift.common.swob.Response instance
        :param crystal_iter: data iterator
        :param requets_data: request metadata
        :returns crystal_iter: data iterator
        """
        return self._apply_filter(req_resp, data_iter, parameters)

    def _apply_filter(self, req_resp, data_iter, parameters):
        """
        This method intercepts the Requests.

        :param response: swift.common.swob.Response instance
        :param crystal_iter: data iterator
        :returns: Data iterator (mandatory)
        """
        raise NotImplemented
