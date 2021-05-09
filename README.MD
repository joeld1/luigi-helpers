Luigi Helpers
===

Description
--

The following package is an incomplete collection of helpful Luigi snippets. I am not responsible for any errors this
script might cause.

For the `luigi.Task` method injector please do the following:

- Read the Luigi docs in addition to the luigi.contrib packages for the packages you are interested in wrapping
- Familiarize yourself with the API that is being wrapped
- Step through the code

Purpose
---
This package parameter injector was made in order to be able to inject parameters into `luigi.Task` instances and deeply
nested objects. Specifically, provide a wrapper for deeply nested client methods.

Simply provide the path to the attribute starting from the `luigi.Task` instance (i.e. if the method you want to wrap is
found in `self._client`, specify `_client`) that contains client method(s) you want to wrap/inject parameters into.

For example, if we noticed that we're missing a client parameter (i.e. `encryption` settings for `luigi.contrib.s3`
methods, or privilege settings for `DockerTask._client.create_host_config` method as shown below) in a nested client
call (i.e. `self._client.sub_class.another_class.yet_another_class.your_favorite_method_name`), we have to
specify `_client.sub_class.another_class.yet_another_class` and `your_favorite_method_name` as shown below (below
example only lists `_client` and `create_host_config`).

Example - Whaler
---
Here's a working example where I'm able to inject `privileged = True` into the docker api client
method `create_host_config`

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
    name = luigi.Parameter("whaler_nginx_latest")
    container_options = {"tty": True, "detach": False}
    binds = ["/var/run/docker.sock:/var/run/docker.sock:ro"]
    network_mode = "host"
    auto_remove = False  # same as -rm flag
    command = luigi.Parameter("nginx:latest")

    # Contains methods we want to inject into, on self._client, and params we want to use for that method
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

    # Feel free to create different and/or separate DictParameters with different names for different nested object
    # methods, but make sure to identify them in task_methods_to_wrap in the TaskMethodInjector decorator 
    client_injections_for_hostconfig = luigi.DictParameter(default={"_client": ['host_config_injection']})

    # Include bottom two if we want to collect and print params
    # but make sure (i.e. collect_args_kwargs is defined in TaskMethodInjector decorator) 
    arg_kwargs_to_collect_from = ["_client"]

    # This one can't be renamed and is required for the collect_args_kwargs in the TaskMethodInjector decorator
    arg_kwargs_collected = defaultdict(list)


if __name__ == "__main__":
    tasks = [StartWhaler(name="whaler_nginx_latest", command="nginx:latest")]
    rc = luigi.build(tasks, local_scheduler=True)
```

The above code pulls and runs the [Whaler](https://github.com/P3GLEG/Whaler) (Docker) on nginx.

Example - Selenium Hub on a Mac M1
---
```python
from collections import defaultdict

import luigi
from luigi.contrib.docker_runner import DockerTask
from luigi.util import inherits

from luigi_helpers.task_method_injector import FlattenDictParams, TaskMethodInjector


@TaskMethodInjector(task_methods_to_wrap={"run": ["client_injections"]},
                    collect_args_kwargs={"run": ["arg_kwargs_to_collect_from"]})
@inherits(FlattenDictParams)
class StartSeleniumHub(DockerTask):
    image = luigi.Parameter(default="selenium/hub")
    name = luigi.Parameter("selenium-hub")
    auto_remove = False
    container_options = {"detach": True}
    binds = ["/var/run/docker.sock:/var/run/docker.sock", "/tmp/videos:/home/seluser/videos", "/dev/shm:/dev/shm"]
    network_mode = "bridge"
    host_config_injection = luigi.DictParameter(default={"create_host_config": [
        {"name": "port_bindings",
         "value": {"4444": 4444},
         "is_flattened_key": False,
         "injection_type": "replace",
         "param_location": "kwargs"},
    ]})
    pull_injection = luigi.DictParameter(default={"pull": [{"name": "platform",
                                                            "value": "linux/amd64",
                                                            "is_flattened_key": False,
                                                            "injection_type": "replace",
                                                            "param_location": "kwargs"},
                                                           ]})

    # Feel free to create different and/or separate DictParameters with different names for different nested object
    # methods, but make sure to identify them in task_methods_to_wrap in the TaskMethodInjector decorator
    client_injections = luigi.DictParameter(default={"_client": ['host_config_injection', 'pull_injection']})
    # Include bottom two if we want to collect and print params
    # but make sure (i.e. collect_args_kwargs is defined in TaskMethodInjector decorator)
    arg_kwargs_to_collect_from = ["_client"]
    # This one can't be renamed and is required for the collect_args_kwargs in the TaskMethodInjector decorator
    arg_kwargs_collected = defaultdict(list)
    command = ['/usr/bin/bash', '-c',
               "export GRID_HUB_HOST=$(hostname -I | sed 's/ *$//g') && source /opt/bin/entry_point.sh"]


if __name__ == "__main__":
    tasks = [StartSeleniumHub()]
    rc = luigi.build(tasks, local_scheduler=True)
```

Running the command above builds a SeleniumHub container and is equivalent to

```
docker run -d -p 4444:4444 --name selenium-hub      \
-v /var/run/docker.sock:/var/run/docker.sock        \
-v /tmp/videos:/home/seluser/videos                 \
-v /dev/shm:/dev/shm                                \
--network=bridge                                    \
-e GRID_HUB_HOST=$(hostname -I | sed 's/ *$//g')    \
--platform=linux/amd64                              \
selenium/hub
 ```

Visiting `localhost:4444` in your web browser should show you SeleniumHub  

Notes
---

- The highly nested code needs to be properly renamed (sorry, there were too many decorators and nested classes to track
  haha).

- TaskMethodInjector
    - `task_methods_to_wrap`
        - The keys are the `luigi.Task` methods (i.e. `run`, `requires`, `output`) to wrap
        - The values are the names of the `luigi.Parameter`s from a `luigi.Task` instance methods that
          contain`luigi.DictParameter`s to use for our parameter injections
            - The `luigi.DictParameter` (i.e. `client_injections_for_hostconfig` in example) contain keys and values
                - The keys are paths to attributes from the `luigi.Task` instance that contain methods that we want to
                  wrap (`_client` or `_client.sub_class.another_class.yet_another_class`).
                - The values are names of `luigi.DictParameter`s
                    - The keys of these `luigi.DictParameter`a are names of methods found after recursively finding the
                      nested class from the `luigi.Task` instance (`create_host_config` or `your_favorite_method_name`)
                    - The values of this `luigi.DictParameter` are lists of dicts; each dict corresponds to a parameter
                      that we want to inject or override in our nested methods
                        - **Currently the only tested values are {"injection_type":"replace"} for "injection_type", any
                          value for 'name','value', and {"param_location":"kwargs"} for "param_location"** Do include
                          them
    - `collect_args_kwargs`
        - The keys are the `luigi.Task` methods (i.e. `run`, `requires`, `output`) to wrap
        - The values are the names of nested objects from the `luigi.Task` instance that contain methods we want to
          capture args and kwargs for (`_client` or `_client.sub_class.another_class.yet_another_class`) -> all
          callables found on these objects will be wrapped!

- `arg_kwargs_collected`
    - This parameter is required for `luigi.Task` classes with the `TaskMethodInjector` decorator when the
      argument `collect_args_kwargs` is declared
- `@inherits(FlattenDictParams)`
    - This is a remnant that's required for `luigi.Task` classes with the `TaskMethodInjector` decorator when the
      argument `task_methods_to_wrap` is declared
    - Might be refactored out if I'm unable to figure out how to update nested dictionaries of different types 
                        