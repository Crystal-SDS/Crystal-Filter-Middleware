# Crystal Filter Middleware for OpenStack Swift

_Please visit [Crystal controller](https://github.com/Crystal-SDS/controller/) for an overview of all Crystal components._

This repository contains the code of the storage filters that intercept object flows and run computations or perform transformations on them.
 
## Requirements

* An OpenStack Swift deployment (this project was tested in Kilo version).

* A Crystal controller deployment.

* [Redis](http://redis.io/)

## Installation

To install the module you can run the next command in the root directory:
```sh
python setup.py install
```

After that, it is necessary to configure OpenStack Swift to add the middleware to the proxy and/or object servers.

* In the proxy servers, we need to add a new filter that must be called `crystal_filter_handler` in `/etc/swift/proxy-server.conf`. Copy the lines below to the bottom part of the file:
```ini
[filter:crystal_filter_handler]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = proxy
redis_host = changeme
redis_port = 6379
redis_db = 0
```

* In the object servers, we need to add a new filter that must be called `crystal_filter_handler` in `/etc/swift/object-server.conf`. Copy the lines below to the bottom part of the file:
```ini
[filter:crystal_filter_handler]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = object
redis_host = changeme
redis_port = 6379
redis_db = 0
```

* Also it is necessary to add this filter to the pipeline variable in the same files. This filter must be
added before `slo` filter and after `crystal_introspection_handler` filter.

* The last step is to restart the proxy-server/object-server service.

## Usage

There are two differentiated kinds of filters:
 
* [Storlet](https://github.com/openstack/storlets) filters: Java classes that implement the IStorlet interface and are able to intercept and modify the data flow of GET/PUT requests in a secure and isolated manner.

* Native filters: python classes that can intercept GET/PUT requests at all the possible life-cycle stages offered by Swift.

![alt text](http://crystal-sds.org/wp-content/uploads/2016/10/crystal_filters_diagram2_small.png "Crystal filters")

As depicted in the diagram above, filters can be installed both in the proxy and the object storage server. Several filters can be executed for the same request (e.g. compression+encryption). The execution order of filters can be configured and does not depend on the kind of filter: a storlet can be applied before a native filter and vice versa.

Filter classes must be registered through [Crystal controller API](https://github.com/Crystal-SDS/controller/), that also provides means to configure the filter pipeline and to control the server where they will be executed.  


A convenient [web dashboard](https://github.com/iostackproject/SDS-dashboard) is also available to simplify Crystal controller API calls.

There is a repository that includes some [filter samples](https://github.com/Crystal-SDS/filter-samples) for compression, encryption, caching, bandwidth differentiation, ...

### Storlet filters

In order to use Storlet filters, it is necessary to [install the storlet engine](http://storlets.readthedocs.io/en/latest/deployer_installation.html) to Swift.

The code below is an example of a storlet filter:

```java
public class ExampleStorlet implements IStorlet {
	/**
	 * The invoke method intercepts the data flow of GET/PUT requests, offering
	 * the input and output stream to perform calculations/modifications.
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
        
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		# The parameters map contains:
		# 1) the special parameter "reverse", that is "True" when the filtering process 
		#    should be reversed (e.g. decompression in the compression filter)
		# 2) Other custom parameters that can be configured through the Controller API
		String reverse = parameters.get("reverse");
		String customParam = parameters.get("custom_param");
				
		byte[] buffer = new byte[65536];
		int len;
		
		try {				
			while((len=is.read(buffer)) != -1) {
			    
			    // ...
			    // Filter code should be placed here to run calculations on data 
			    // or perform modifications
			    // ...
			
				os.write(buffer, 0, len);
			}
			is.close();
			os.close();
		} catch (IOException e) {
			logger.emitLog("Example Storlet - raised IOException: " + e.getMessage());
		}
	}
}
```

The `StorletInputStream` is used to stream object’s data into the storlet. An instance of the class is provided whenever the Storlet gets an object as an input. 
Practically, it is used in all storlet invocation scenarios to stream in the object’s data and metadata. 
To consume the data call `getStream()` to get a `java.io.InputStream` on which you can just `read()`. To consume the metadata call the `getMetadata()` method.

In all invocation scenarios the storlet is called with an instance of `StorletObjectOutputStream`.
Use the `setMetadata()` method to set the Object’s metadata. 
Use `getStream()` to get a `java.io.OutputStream` on which you can just `write()` the content of the object.
Notice that `setMetadata()` must be called. Also, it must be called before writing the data.

The `StorletLogger` class supports a single method called `emitLog()`, and accepts a String. 

For more information on writing and deploy Storlets, please refer to [Storlets documentation](http://storlets.readthedocs.io/en/latest/writing_and_deploying_java_storlets.html). 

### Native filters

The code below is an example of a native filter:

```python
class NativeFilterExample(object):
    
    def __init__(self, global_conf, filter_conf, logger):
        # The constructor receives the configuration parameters and the logger
        self.logger = logger
        self.cache = BlockCache()
    
    # This method is called by the middleware to allow filters to intercept GET/PUT requests life-cycle
    def execute(self, req_resp, crystal_iter, request_data):
        method = request_data['method']
        
        if method == 'get':
            if isinstance(req_resp, Request):
                # ...
                # Filter code for GET requests should be placed here
                # ...
            elif isinstance(req_resp, Response):
                # ...
                # Filter code for GET responses should be placed here (the response includes 
                # the object data in this phase)
                # ...
        elif method == 'put':
            if isinstance(req_resp, Request):
                # ...
                # Filter code for PUT requests should be placed here (the request includes 
                # the object data in this phase)
                # ...
            elif isinstance(req_resp, Response):
                # ...
                # Filter code for PUT responses should be placed here
                # ...

        return crystal_iter
```

The `execute()` method is called by the middleware at all life-cycle stages of the request/response. The `req_resp` parameter can be the swift.common.swob.Request or swift.common.swob.Response depending on the life-cycle phase the method is called.
Upon registering the filter through Crystal controller, you can specify which server and life-cycle phase the filter will be called at, depending on the type of required computation or data-manipulation. For example, a caching filter should be executed at proxy servers, intercepting both the PUT and GET requests before reaching the object server (at request phase).

The `crystal_iter` parameter is an iterator of the data stream to be processed.

The `request_data` parameter is a dictionary that contains the following keys:

- `'app'`: `'proxy-server'` or `'object-server'`
- `'api_version'`: the Swift API version
- `'account'`: the tenant name
- `'container'`: the container name
- `'object'`: the object name
- `'method'`: `'put'` or `'get'` 


## Support

Please [open an issue](https://github.com/Crystal-SDS/filter-middleware/issues/new) for support.

## Contributing

Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/Crystal-SDS/filter-middleware/compare/).
