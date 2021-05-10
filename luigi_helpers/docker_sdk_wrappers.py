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


def build_image_from_dockerfile(self_obj, *args0, **kwargs0):
    """
    path (str) – Path to the directory containing the Dockerfile
    fileobj – A file object to use as the Dockerfile. (Or a file-like object)
    tag (str) – A tag to add to the final image
    quiet (bool) – Whether to return the status
    nocache (bool) – Don’t use the cache when set to True

    rm (bool) – Remove intermediate containers. The docker build command now defaults to --rm=true, but we have kept
    the old default of False to preserve backward compatibility

    timeout (int) – HTTP timeout
    custom_context (bool) – Optional if using fileobj
    encoding (str) – The encoding for a stream. Set to gzip for compressing
    pull (bool) – Downloads any updates to the FROM image in Dockerfiles
    forcerm (bool) – Always remove intermediate containers, even after unsuccessful builds
    dockerfile (str) – path within the build context to the Dockerfile
    buildargs (dict) – A dictionary of build arguments
    container_limits (dict) –
    A dictionary of limits applied to each container created by the build process. Valid keys:

    memory (int): set memory limit for build
    memswap (int): Total memory (memory + swap), -1 to disable
    swap
    cpushares (int): CPU shares (relative weight)
    cpusetcpus (str): CPUs in which to allow execution, e.g., "0-3", "0,1"
    decode (bool) – If set to True, the returned stream will be decoded into dicts on the fly. Default False
    shmsize (int) – Size of /dev/shm in bytes. The size must be greater than 0. If omitted the system uses 64MB
    labels (dict) – A dictionary of labels to set on the image
    cache_from (list) – A list of images used for build cache resolution
    target (str) – Name of the build-stage to build in a multi-stage Dockerfile
    network_mode (str) – networking mode for the run commands during build
    squash (bool) – Squash the resulting images layers into a single layer.
    extra_hosts (dict) – Extra hosts to add to /etc/hosts in building containers, as a mapping of hostname to IP address.
    platform (str) – Platform in the format os[/arch[/variant]]
    isolation (str) – Isolation technology used during build. Default: None.

    use_config_proxy (bool) – If True, and if the docker client configuration file (~/.docker/config.json by default)
    contains a proxy configuration, the corresponding environment variables will be set in the container being built.

    :param self_obj:
    :param args0:
    :param kwargs0:
    :return:
    """
    client_method = getattr(self_obj._client, "build")

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
