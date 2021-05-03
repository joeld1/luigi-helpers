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
