import docker

from . import errors

def service_exists(srv_name):
    """
    Checks whether a service with the given name exists.

    Args:
        srv_name (str): Name of the service. Must be a valid Docker
            container name.

    Returns:
        bool: True iff the service exists
    """
    dc = docker.from_env()
    try:
        dc.containers.get(srv_name)
        return True
    except docker.errors.NotFound:
        return False

def get_service(srv_name, port):
    """
    Gets a service by name and port.

    Args:
        srv_name (str): Name that the service was created with. Must be
            a valid Docker container name.
        port (int): Port number that the service was created with.

    Returns:
        Service: The service, if it exists.

    Raises:
        ServiceNotFound: The requested service does not exist.
    """
    if not service_exists(srv_name):
        raise errors.ServiceNotFound()
    return Service(srv_name, port)

def service_from_dict(srv_dict):
    """
    Gets a service from a dictionary.

    The dictionary must have been created by a call to
    Service.to_dict().

    Args:
        srv_dict (dict): A dictionary describing the service.

    Returns:
        Service: The service, if it exists.

    Raises:
        ServiceNotFound: The requested service does not exist.
    """
    return get_service(srv_dict['name'], srv_dict['port'])

def create_service(srv_name, port, srv_type, user_name, password=''):
    """
    Creates a new service for a given user at a given port.

    Args:
        srv_name (str): A unique name for the service. Must be a valid
            Docker container name.
        port (int): A unique port number on which the service will be
            made available. It will listen only on localhost.
        srv_type (str): The type of service to launch. This is the name
            of the Docker image to use.
        user_name (str): The user name to use to connect to the compute
            resource.
        password (str): The password to use to connect to the compute
            resource.

    Returns:
        Service: The created service

    Raises:
        ServiceAlreadyExists: A service with this name already exists.
        PortNotAvailable: The requested port is occupied.
    """
    dc = docker.from_env()
    try:
        srv = dc.containers.get(srv_name)
        raise errors.ServiceAlreadyExists()
    except docker.errors.NotFound:
        pass

    try:
        dc.containers.run(
                srv_type,
                name=srv_name,
                ports={'29593/tcp': ('127.0.0.1', port) },
                environment={
                    'CERISE_USERNAME': user_name,
                    'CERISE_PASSWORD': password
                    },
                detach=True)
    except docker.errors.APIError as e:
        # Bit clunky, but it's all Docker gives us...
        if 'address already in use' in e.explanation:
            raise errors.PortNotAvailable(e)
        if 'port is already allocated' in e.explanation:
            raise errors.PortNotAvailable(e)
        raise

    return Service(srv_name, port)

class Service:
    def __init__(self, name, port):
        """
        Create a new Service object.

        Note that this does not actually create the Docker container;
        use create_service(), get_service() or service_from_dict() to
        obtain a Service object with an actual corresponding service.

        Args:
            name (str): The name for the service (and its corresponding
                Docker container).
            port (int): The port number on which the service runs.
        """
        self._name = name
        """The name of this service, and it's Docker container."""
        self._port = port
        """The port number on localhost that the service listens on."""

    def destroy(self):
        """
        Destroys the service.

        This will make the service unavailable, and delete all
        information about jobs (including input and output data)
        contained within it.
        """
        dc = docker.from_env()
        container = dc.containers.get(self._name)
        container.stop()
        container.remove()

    def start(self):
        """
        Start a stopped service.

        Does nothing if the service is already running.
        """
        dc = docker.from_env()
        container = dc.containers.get(self._name)
        container.start()

    def stop(self):
        """
        Stop a running service.

        This must be done before shutting down the computer, to ensure
        a clean shutdown. Does nothing if the service is already
        stopped.
        """
        dc = docker.from_env()
        container = dc.containers.get(self._name)
        container.stop()

    def is_running(self):
        """
        Checks whether the service is running.

        Returns:
            bool: True iff the service is running.
        """
        dc = docker.from_env()
        container = dc.containers.get(self._name)
        return container.status == 'running'

    def to_dict(self):
        """
        Saves the Service to a dictionary.

        The dictionary can later be used to recreate the Service object
        by passing it to service_from_dict(). The exact format of the
        dictionary is not given, but it contains only Python built-in
        types so that it can easily be stored or otherwise serialised.

        Returns:
            dict: A dictionary with information necessary to rebuild
                the Service object.
        """
        return {
                'name': self._name,
                'port': self._port
                }

    def create_job(self, job_name):
        """
        Creates a job to run on the given service.

        Args:
            job_name (str): The name of the job. This must be unique
                among all jobs on this service, and must consist of
                letters, digits, underscores and hyphens. A GUID or
                similar is recommended.

        Returns:
            Job: The new job.
        """
        pass

    def get_job_by_id(self, job_id):
        """
        Gets a job by its id.

        Args:
            job_id (str): The id of the job, as returned by job.id
                after it's been created.

        Returns:
            Job: The requested job.

        Raises:
            JobNotFound: The job is unknown to this service. Either
                it was deleted, or it never existed on this service.
        """
        pass
