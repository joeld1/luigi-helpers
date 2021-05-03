Luigi Helpers
===

Description
--

The following package is an incomplete collection of helpful Luigi snippets. I am not responsible for any errors this script might cause.

For the `luigi.Task` method injector please do the following:
- Read the Luigi docs in addition to the luigi.contrib packages for the packages you are interested in wrapping
- Familiarize yourself with the API that is being wrapped
- Step through the code 

Purpose
---
This package parameter injector was made in order to be able to inject parameters into `luigi.Task` instances and deeply nested objects. Specifically, provide a wrapper for deeply nested client methods.

Simply provide the path to the attribute (starting from the instance - i.e. `_client`) that contains client methods you want to wrap around (i.e. `_client.create_host_config`)

Example
---
Here's a working example where I'm able to inject `privileged = True` into the docker api client method `create_host_config`

[See the Docker Python SDK docs for reference](https://docker-py.readthedocs.io/en/stable/api.html?#docker.api.container.ContainerApiMixin.create_host_config)

[See the Luigi DockerTask source code for reference and compare it with the original](https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/docker_runner.html#DockerTask)

```python
from collections import defaultdict

import luigi
from luigi.contrib.docker_runner import DockerTask
from luigi.util import inherits

from luigi_helpers.task_method_injector import TaskMethodInjector, FlattenDictParams


@TaskMethodInjector(task_methods_to_wrap={"run": ["client_injections_for_hostconfig"]},
                    collect_args_kwargs={"run": ["arg_kwargs_to_collect_from"]})
@inherits(FlattenDictParams)
class StartWhaler(DockerTask):
    image = "pegleg/whaler"
    name = luigi.Parameter(default="whaler_nginx_latest")
    container_options = {"tty": True, "detach": False}
    binds = ["/var/run/docker.sock:/var/run/docker.sock:ro"]
    network_mode = "host"
    auto_remove = False  # same as -rm flag
    command = luigi.Parameter(default="nginx:latest")
    host_config_injection = luigi.DictParameter(default={"create_host_config": [{"name": "privileged",
                                                                                 "value": True,
                                                                                 "is_flattened_key": False,
                                                                                 "injection_type": "replace",
                                                                                 "param_location": "kwargs"},
                                                                                {"name": "publish_all_ports",
                                                                                 "value": True,
                                                                                 "is_flattened_key": False,
                                                                                 "injection_type": "replace",
                                                                                 "param_location": "kwargs"}, ]})
    client_injections_for_hostconfig = luigi.DictParameter(default={"_client": ['host_config_injection']})
    arg_kwargs_to_collect_from = ["_client"]
    arg_kwargs_collected = defaultdict(list)


if __name__ == "__main__":
    tasks = [StartWhaler(name="whaler_nginx_latest",command="nginx:latest")]
    rc = luigi.build(tasks, local_scheduler=True)
```
The above code pulls and runs the [Whaler](https://github.com/P3GLEG/Whaler) (Docker) on nginx.

Notes
---
- The highly nested code needs to be properly renamed (sorry, there were too many decorators and nested classes to track haha).

- TaskMethodInjector
    - `task_methods_to_wrap`
        - The keys are the `luigi.Task` methods (i.e. `run`, `requires`, `output`) to wrap
        - The values are the names of the `luigi.Parameter`s from a `luigi.Task` instance methods that contain`luigi.DictParameter`s to use for our parameter injections
            - The dicts contain keys and values
                - The keys are paths to attributes from the `luigi.Task` instance that contain methods that we want to wrap.
                - The values are names of `luigi.DictParameter`s
                    - The keys of these dictionaries are names of methods found after recursively finding the nested class from the `luigi.Task` instance
                    - The values of this `luigi.DictParameter` are lists of dicts; each dict corresponds to a parameter that we want to inject or override in our nested methods 
                        - **Currently the only tested values are 'replace' for 'injection_type' 'name','value', and 'kwargs' for 'param_location'** Do include them 
    - `collect_args_kwargs`
        - The keys are the `luigi.Task` methods (i.e. `run`, `requires`, `output`) to wrap
        - The values are the names of nested objects from the `luigi.Task` instance that contain methods we want to capture args and kwargs for

- `arg_kwargs_collected`
    - This parameter is required for `luigi.Task` classes wrapped `TaskMethodInjector` when the argument `collect_args_kwargs` is declared
- `@inherits(FlattenDictParams)`
    - This is a remnant that's required for luigi.Task classes wrapped `TaskMethodInjector`  when the argument `task_methods_to_wrap` is declared
    - Might be refactored out if I'm unable to figure out how to update nested dictionaries of different types 
                        