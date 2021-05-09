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
