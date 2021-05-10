from functools import wraps

import luigi
from luigi.contrib.docker_runner import DockerTask


def complete(task_obj):
    task_obj.complete()
    return task_obj


def container_status(container_inspect_info, status="running"):
    prev_state = container_inspect_info.get('State', None)
    prev_status = ''
    if prev_state:
        prev_status = prev_state.get("Status", "")
    if prev_status == status:
        return True
    else:
        return False


def create_container(self_obj, *args0, **kwargs0):
    client_method = getattr(self_obj._client, "exec_start")

    @wraps(client_method)
    def wrapped_client_method(*args, **kwargs):
        return client_method(*args0, **kwargs0)

    return wrapped_client_method


class PruneContainers(luigi.ExternalTask, DockerTask):
    filters = luigi.DictParameter(default=None)
    is_complete = luigi.BoolParameter(default=False)

    def complete(self):
        if not self.is_complete:
            if self.filters:
                filters = self.filters
            else:
                filters = None
            dict_to_return = self._client.prune_containers(filters)
            self.is_complete = True
        return self.is_complete

    def run(self):
        pass


class PruneNetworks(luigi.ExternalTask, DockerTask):
    filters = luigi.DictParameter(default=None)
    is_complete = luigi.BoolParameter(False)

    def complete(self):
        if not self.is_complete:
            if self.filters:
                filters = self.filters
            else:
                filters = None
            dict_to_return = self._client.prune_networks(filters)
            self.is_complete = True
        return self.is_complete

    def run(self):
        pass


class ContainerExists(luigi.ExternalTask, DockerTask):
    name = luigi.Parameter(default="")
    container_exists = luigi.BoolParameter(False)
    check_complete_on_run = luigi.BoolParameter(default=True)

    def complete(self):
        pre_existing = self._client.containers(all=True, filters={"name": self.name})
        if len(pre_existing) >= 1:
            self.container_exists = True
            return True
        else:
            self.container_exists = False
            return True

    def run(self):
        self.complete()


class StopContainers(luigi.ExternalTask, DockerTask):
    is_complete = luigi.BoolParameter(default=False)
    check_complete_on_run = luigi.BoolParameter(default=True)
    all_containers_stopped = luigi.BoolParameter(default=False)

    def complete(self):
        all_containers = self._client.containers(all=True)
        if all_containers:
            all_stopped = all([c['State'] == "exited" for c in all_containers])
            if all_stopped:
                self.all_containers_stopped = True
                return True
            else:
                return False
        else:
            return True

    def run(self):
        if self.all_containers_stopped:
            pass
        else:
            if not self.is_complete:
                all_containers = self._client.containers(all=True)
                if all_containers:
                    all_stopped = all([c['State'] == "exited" for c in all_containers])
                    while not all_stopped:
                        for c in all_containers:
                            if c['State'] != "exited":
                                try:
                                    self._client.kill(c['Id'])
                                except Exception as e:
                                    print(e)
                        all_containers = self._client.containers(all=True)
                        all_stopped = all([c['State'] == "exited" for c in all_containers])
                    if all_stopped:
                        self.all_containers_stopped = True
                        self.is_complete = True


class ContainerIsRunning(luigi.ExternalTask, DockerTask):
    name = luigi.Parameter(default="")
    container_is_running = luigi.BoolParameter(False)
    is_complete = luigi.BoolParameter(False)
    check_complete_on_run = luigi.BoolParameter(default=True)

    def complete(self):
        container_exists = complete(ContainerExists(name=self.name)).container_exists
        if container_exists:
            prev_container_info = self._client.inspect_container(self.name)
            is_running = container_status(prev_container_info, status="running")
            if is_running:
                self.is_complete = True
                self.container_is_running = True
            else:
                self.is_complete = True
                self.container_is_running = False
        else:
            self.is_complete = True
            self.container_is_running = False
        return self.is_complete

    def run(self):
        self.complete()


class StartContainer(luigi.ExternalTask, DockerTask):
    name = luigi.Parameter(default="")
    container_is_running = luigi.BoolParameter(False)
    is_complete = luigi.BoolParameter(False)
    check_complete_on_run = luigi.BoolParameter(default=True)

    def complete(self):
        container_exists = complete(ContainerExists(name=self.name)).container_exists
        if container_exists:
            is_running = complete(ContainerIsRunning(name=self.name)).container_is_running
            if is_running:
                self.is_complete = True
                self.container_is_running = True
            else:
                self.is_complete = False
                self.container_is_running = False
        else:
            self.is_complete = False
            self.container_is_running = False
        return self.is_complete

    def run(self):
        if not self.is_complete:
            container_exists = complete(ContainerExists(name=self.name)).container_exists
            if container_exists:
                prev_container_info = self._client.inspect_container(self.name)
                is_exited = container_status(prev_container_info, status="exited")
                if is_exited:
                    self._client.start(prev_container_info['Id'])
                    while not self.container_is_running:
                        self.complete()


class CreateNetwork(luigi.ExternalTask, DockerTask):
    network_name = luigi.Parameter("grid")
    is_complete = luigi.BoolParameter(False)

    def complete(self):
        if not self.is_complete:
            network_info = self._client.create_network(self.network_name, check_duplicate=True)
            self.is_complete = True
            return self.is_complete
        else:
            return self.is_complete

    def run(self):
        pass
