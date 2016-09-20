# Crystal Filter Middleware for OpenStack Swift

## Installation

To install the module you can run the next command in the parent folder:
```python
python setup.py install
```

After that, it is necessary to configure OpenStack Swift to add the middleware to the proxy and the object servers.

- We need to add a new filter that must be called swift_sds in the ( `proxy-server.conf`): copy the next lines to the bottom part of the file:
```
[filter:crystal_filter_handler]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = proxy
```
- We need to add a new filter that must be called swift_sds in the ( `object-server.conf`): copy the next lines to the bottom part of the file:
```
[filter:crystal_filter_handler]
use = egg:swift_crystal_filter_middleware#crystal_filter_handler
execution_server = object
```
- Also it is necessary to add this filter to the pipeline variable. This filter must be
added before `slo` filter and after `crystal_introspection_handler` filter.

- The last step is to restart the proxy-server service. Now the middleware has been installed.
