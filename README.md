# Crystal Filter Middleware for OpenStack Swift

_Please visit [Crystal Installation](https://github.com/Crystal-SDS/INSTALLATION/) for an overview of all Crystal components._

This repository contains the code of the storage filters that intercept object flows and run computations or perform transformations on them.
 
## Requirements

* An OpenStack Swift deployment (this project was tested from Kilo to Pike OpenStack releases).

* A [Crystal controller](https://github.com/Crystal-SDS/controller) deployment.

* Optionally, in order to use Storlet filters, it is necessary to [install the storlet engine](http://storlets.readthedocs.io/en/latest/deployer_installation.html) to Swift.

## Installation

To install the module, clone the repository and run the installation command in the root directory:
```sh
git clone https://github.com/Crystal-SDS/filter-middleware
cd filter-middleware
sudo python setup.py install
```

After that, it is necessary to configure OpenStack Swift to add the middleware to the proxy and object servers.

### Proxy
Edit the `/etc/swift/proxy-server.conf` file in each Proxy Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `slo` filter.

```ini
[pipeline:main]
pipeline = catch_errors gatekeeper healthcheck proxy-logging cache container_sync bulk ratelimit authtoken crystal_acl keystoneauth container-quotas account-quotas crystal_metrics crystal_filters copy slo dlo proxy-logging proxy-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:
```ini
[filter:crystal_filters]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = proxy

# Redis Configuration
redis_host = controller
redis_port = 6379
redis_db = 0

# Storlets Configuration
storlet_container = storlet
storlet_dependency = dependency
storlet_logcontainer = storletlog
storlet_execute_on_proxy_only = false
storlet_gateway_module = docker
storlet_gateway_conf = /etc/swift/storlet_docker_gateway.conf
```

### Storage Node

Edit the `/etc/swift/object-server.conf` file in each Storage Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `object-server` filter.
```ini
[pipeline:main]
pipeline = healthcheck recon crystal_metrics crystal_filters object-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:
```ini
[filter:crystal_filters]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = object

# Redis Configuration
redis_host = controller
redis_port = 6379
redis_db = 0

# Storlets Configuration
storlet_container = storlet
storlet_dependency = dependency
storlet_logcontainer = storletlog
storlet_execute_on_proxy_only = false
storlet_gateway_module = docker
storlet_gateway_conf = /etc/swift/storlet_docker_gateway.conf
```

* Also it is necessary to add this filter to the pipeline variable in the same files. This filter must be added after `keystoneauth` and `crystal_metrics` filters and before `slo`, `proxy-logging` and `proxy-server` filters.

* The last step is to restart the proxy-server/object-server service:
```bash
sudo swift-init proxy restart
sudo swift-init object restart
```

## Usage

There are two differentiated kinds of filters:
 
* [Storlet](https://github.com/openstack/storlets) filters: Java classes that implement the IStorlet interface and are able to intercept and modify the data flow of GET/PUT requests in a secure and isolated manner.

* Native filters: python classes that can intercept all method requests at all the possible life-cycle stages offered by Swift.

![alt text](http://crystal-sds.org/wp-content/uploads/2016/10/crystal_filters_diagram2_small.png "Crystal filters")

As depicted in the diagram above, filters can be installed both in the proxy and the object storage server. Several filters can be executed for the same request (e.g. compression+encryption). The execution order of filters can be configured and does not depend on the kind of filter: a storlet can be applied before a native filter and vice versa.

Filter classes must be registered through [Crystal controller API](https://github.com/Crystal-SDS/controller/), that also provides means to configure the filter pipeline and to control the server where they will be executed.  


A convenient [web dashboard](https://github.com/iostackproject/SDS-dashboard) is also available to simplify Crystal controller API calls.

There is a repository that includes some [filter samples](https://github.com/Crystal-SDS/filter-samples) for compression, encryption, caching, bandwidth differentiation, etc.

### Native filters

Native filters are [Swift middlewares](https://docs.openstack.org/swift/latest/development_middleware.html), but dynamically managed by Crystal. The `parameters` parameter is a dictionary that contains the parameters introduced by the [Crystal dashboard](https://github.com/Crystal-SDS/dashboard).

The code below is an example of a native filter:

```python
class NopFilter(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(self.conf, log_route='nop_filter')
        self.filter_data = self.conf['filter_data']
        self.parameters = self.filter_data['params']

        self.register_info()

    def register_info(self):
        register_swift_info('nop_filter')

    @wsgify
    def __call__(self, req):
        return req.get_response(self.app)


	def filter_factory(global_conf, **local_conf):
	    conf = global_conf.copy()
	    conf.update(local_conf)
	
	    def noop_filter(app):
	        return NoopFilter(app, conf)
	    return noop_filter
```

### Storlet filters

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


## Support

Please [open an issue](https://github.com/Crystal-SDS/filter-middleware/issues/new) for support.

## Contributing

Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/Crystal-SDS/filter-middleware/compare/).
