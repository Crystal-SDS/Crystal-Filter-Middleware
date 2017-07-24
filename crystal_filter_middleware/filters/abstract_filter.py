from eventlet import Timeout

TIMEOUT = 10  # Timeout while reading data chunks


class AbstractFilter(object):

    def __init__(self, global_conf, filter_conf, logger):
        self.logger = logger
        self.global_conf = global_conf
        self.filter_conf = filter_conf

    def execute(self, req_resp, crystal_iter, requets_data):
        """
        Entry method (This method must be maintained unmodified: for all filters
        the entry point will be the same)

        :param req_resp: swift.common.swob.Request or swift.common.swob.Response instance
        :param crystal_iter: data iterator
        :param requets_data: request metadata
        :returns crystal_iter: data iterator
        """
        self.request_data = requets_data
        method = self.request_data['method']

        if method == 'get':
            crystal_iter = self._get_object(req_resp, crystal_iter)

        elif method == 'put':
            crystal_iter = self._put_object(req_resp, crystal_iter)

        return crystal_iter

    def _get_object(self, response, crystal_iter):
        """
        This method intercepts the Response.

        :param response: swift.common.swob.Response instance
        :param crystal_iter: data iterator
        :returns: FilterIter instance (data iterator with the filter injected)
        """
        # TODO: Implement the logic of the filter
        return FilterIter(crystal_iter, TIMEOUT, self._filter_get)

    def _put_object(self, request, crystal_iter):
        """
        This method intercepts the Request.

        :param request: swift.common.swob.Request instance
        :param crystal_iter: data iterator
        :returns: FilterIter instance (data iterator with the filter injected)
        """
        # TODO: Implement the logic of the filter
        return FilterIter(crystal_iter, TIMEOUT, self._filter_put)

    def _filter_put(self, chunk):
        """
        Implementation of the filter (PUT request). This method will be
        injected to the request and will intercept the data flow.
        The filter will be applied chunk by chunk.

        :param chunk: data chunk: normally 64K of data
        :returns: filtered data chunk
        """
        # TODO: Implement filter
        return chunk

    def _filter_get(self, chunk):
        """
        Implementation of the filter (GET request). This method will be
        injected to the request and will intercept the data flow.
        The filter will be applied chunk by chunk.

        :param chunk: data chunk: normally 64K of data
        :returns: filtered data chunk
        """
        # TODO: Implement filter
        return chunk


class FilterIter(object):
    def __init__(self, obj_data, timeout, filter_method):
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.buf = b''

        self.filter = filter_method

    def __iter__(self):
        return self

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                if hasattr(self.obj_data, 'read'):
                    chunk = self.obj_data.read(size)
                else:
                    chunk = self.obj_data.next()
                chunk = self.filter(chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise

        return chunk

    def next(self, size=64 * 1024):
        if len(self.buf) < size:
            self.buf += self.read_with_timeout(size - len(self.buf))
            if self.buf == b'':
                self.close()
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=64 * 1024):
        self._close_check()
        return self.next(size)

    def readline(self, size=-1):
        self._close_check()

        # read data into self.buf if there is not enough data
        while b'\n' not in self.buf and \
              (size < 0 or len(self.buf) < size):
            if size < 0:
                chunk = self.read()
            else:
                chunk = self.read(size - len(self.buf))
            if not chunk:
                break
            self.buf += chunk

        # Retrieve one line from buf
        data, sep, rest = self.buf.partition(b'\n')
        data += sep
        self.buf = rest

        # cut out size from retrieved line
        if size >= 0 and len(data) > size:
            self.buf = data[size:] + self.buf
            data = data[:size]

        return data

    def readlines(self, sizehint=-1):
        self._close_check()
        lines = []
        try:
            while True:
                line = self.readline(sizehint)
                if not line:
                    break
                lines.append(line)
                if sizehint >= 0:
                    sizehint -= len(line)
                    if sizehint <= 0:
                        break
        except StopIteration:
            pass
        return lines

    def close(self):
        if self.closed:
            return
        try:
            self.obj_data.close()
        except AttributeError:
            pass
        self.closed = True

    def __del__(self):
        self.close()
