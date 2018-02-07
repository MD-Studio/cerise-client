from . import errors
from .job import Job

import docker

import errno
import os
import requests
import tarfile
import tempfile
import time

# Creating and destroying services

def create_managed_service(srv_name, port, srv_type, user_name=None, password=None):
    """
    Creates a new managed service for a given user at a given port.

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

    environment = {}
    environment['CERISE_STORE_LOCATION_CLIENT'] = 'http://localhost:{}/files'.format(port)

    if user_name == '':
        user_name = None

    if user_name is not None:
        environment['CERISE_USERNAME'] = user_name
    if password is not None:
        environment['CERISE_PASSWORD'] = password

    try:
        dc.containers.run(
                srv_type,
                name=srv_name,
                ports={'29593/tcp': ('127.0.0.1', port) },
                environment=environment,
                detach=True)
    except docker.errors.APIError as e:
        # Bit clunky, but it's all Docker gives us...
        if 'address already in use' in e.explanation:
            raise errors.PortNotAvailable(e)
        if 'port is already allocated' in e.explanation:
            raise errors.PortNotAvailable(e)
        if 'Conflict. The container name' in e.explanation:
            raise errors.ServiceAlreadyExists()
        raise

    time.sleep(1)

    return Service(srv_name, port)

def destroy_managed_service(srv):
    """
    Destroys a managed service.

    This will make the service unavailable, and delete all
    jobs and information about them (including input and output
    data) in this service and on the compute resource.

    Args:
        srv (Service): A managed service.

    Raises:
        ServiceNotFound: A service with this name was not found.
    """
    dc = docker.from_env()
    try:
        container = dc.containers.get(srv._name)
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        raise errors.ServiceNotFound()

def managed_service_exists(srv_name):
    """
    Checks whether a managed service with the given name exists.

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

def get_managed_service(srv_name, port):
    """
    Gets a managed service by name and port.

    Args:
        srv_name (str): Name that the service was created with. Must be
            a valid Docker container name.
        port (int): Port number that the service was created with.

    Returns:
        Service: The service, if it exists.

    Raises:
        ServiceNotFound: The requested service does not exist.
    """
    if not managed_service_exists(srv_name):
        raise errors.ServiceNotFound()
    return Service(srv_name, port)

def require_managed_service(srv_name, port, srv_type, user_name=None, password=None):
    """
    Creates a new managed service for a given user at a given port, if
    it does not already exist.

    If a service with the given name already exists, it is returned
    instead and no new service is created.

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
        PortNotAvailable: The requested port is occupied.
    """
    try:
        return create_managed_service(srv_name, port, srv_type, user_name, password)
    except errors.ServiceAlreadyExists:
        return get_managed_service(srv_name, port)


# Starting and stopping managed services

def managed_service_is_running(srv):
    """
    Checks whether the managed service is running.

    Returns:
        bool: True iff the service is running.
    """
    dc = docker.from_env()
    container = dc.containers.get(srv._name)
    return container.status == 'running'

def start_managed_service(srv):
    """
    Start a stopped managed service.

    Does nothing if the service is already running.
    """
    dc = docker.from_env()
    container = dc.containers.get(srv._name)
    container.start()
    # Give it some time to start, so subsequent calls work
    time.sleep(1)

def stop_managed_service(srv):
    """
    Stop a running managed service.

    This must be done before shutting down the computer, to ensure
    a clean shutdown. Does nothing if the service is already
    stopped.
    """
    dc = docker.from_env()
    container = dc.containers.get(srv._name)
    container.stop()


# Serialisation of services

def service_to_dict(srv):
    """
    Saves the service to a dictionary.

    The dictionary can later be used to recreate the Service object
    by passing it to service_from_dict(). The exact format of the
    dictionary is not given, but it contains only Python built-in
    types so that it can easily be stored or otherwise serialised.

    Returns:
        dict: A dictionary with information necessary to rebuild
            the Service object.
    """
    return {
            'name': srv._name,
            'port': srv._port
            }

def service_from_dict(srv_dict):
    """
    Gets a service from a dictionary.

    The dictionary must have been created by a call to
    service_to_dict().

    Args:
        srv_dict (dict): A dictionary describing the service.

    Returns:
        Service: The service, if it exists.

    Raises:
        ServiceNotFound: The requested service does not exist.
    """
    return get_managed_service(srv_dict['name'], srv_dict['port'])


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
        """str: The name of this service, and its Docker container."""
        self._port = port
        """int: The port number on localhost that the service listens on."""

        self._srv_loc = 'http://localhost:' + str(self._port)
        self._filestore = self._srv_loc + '/files'
        self._jobs = self._srv_loc + '/jobs'

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

        Raises:
            JobAlreadyExists: A job with this name already exists
                on this service.
        """
        self._create_input_dir(job_name)
        return Job(self, job_name)

    def destroy_job(self, job):
        """
        Removes a job, including all its input and output data,
        from the service.

        Args:
            job (Job): The job to be removed.

        Raises:
            JobNotFound: No job with this name or id was found on this
                service. Did you destroy it twice?
            CommunicationError: There was an error communicating with
                the service.
        """
        self._delete_input_dir(job.name)
        r = requests.delete(self._jobs + '/' + job.id)

        if r.status_code == 404:
            raise errors.JobNotFound("Either the input directory or the job could not be found.")

        if r.status_code != 204:
            raise errors.CommunicationError(r, r2)


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
        job = self._get_job_from_service(job_id)
        declared_inputs = None
        return Job(self, job['name'], job['id'],
                declared_inputs, job['workflow'], job['input'])

    def get_job_by_name(self, job_name):
        """
        Gets a job by its name. Only jobs that have been submitted
        are on the service, so if job.run() has not been called,
        this will not find the job.

        Args:
            job_name (str): The name of the job, as set when it was
                created.

        Returns:
            Job: The requested job.

        Raises:
            JobNotFound: The job is unknown to this service. Either
                it was deleted, or it never existed. Did you run()
                the job?
        """
        job = [job for job in self.list_jobs() if job.name == job_name]
        if job == []:
            raise errors.JobNotFound()
        return job[0]

    def list_jobs(self):
        """
        List all the jobs known to the service. Note that only jobs
        that have been submitted are known to the service, if you have
        not called job.run() yet, it won't be in there.

        Returns:
            [Job]: A list of known jobs.

        Raises:
            CommunicationError: There was a problem communicating with
                the service. Is it running?
        """
        r = requests.get(self._jobs)
        if r.status_code != 200:
            raise errors.CommunicationError(r)
        jobs_json = r.json()
        return [Job(self, job['name'], job['id'], None,
                    job['workflow'], job['input'])
                for job in jobs_json]

    def get_log(self):
        """
        Get the internal Cerise log for this service. If things are not
        working as you expect them to (e.g. a job status of
        SystemError), the log may contain useful information on what
        went wrong.

        Returns:
            str: The job log
        """
        dc = docker.from_env()
        container = dc.containers.get(self._name)
        stream, stat = container.get_archive('/var/log/cerise/cerise_backend.log')
        with tempfile.TemporaryFile() as tmp:
            tmp.write(stream.read())
            tmp.seek(0)
            with tarfile.open(fileobj=tmp) as archive:
                # Scope guard does not work in Python 2
                logfile = archive.extractfile('cerise_backend.log')
                service_log = logfile.read().decode('utf-8')
                logfile.close()
        return service_log

    def _input_dir(self, job_name):
        """
        Returns the remote URL for the input data directory for the
        given job.

        Args:
            job_name (str): The name of the job to return the
                input directory for.
        Returns:
            str: The URL.
        """
        return self._filestore + '/input/' + job_name

    def _create_input_dir(self, job_name):
        """
        Create a remote input data directory for a job on this service.

        Args:
            job_name (str): The (unique!) name of the job to make a
                directory for.

        Raises:
            JobAlreadyExists: A directory named after this job already
                exists on this service.
        """
        # Note: WebDAV requires the trailing /
        r = requests.request('MKCOL', self._input_dir(job_name) + '/')
        if r.status_code == 405:
            raise errors.JobAlreadyExists()

    def _upload_file(self, job_name, local_file_path):
        """
        Upload a local file to a remote input data directory for a
        given job. Directory must have been made first using
        _create_jobdir().

        If a file with the same name already exists, it is overwritten
        silently.

        Args:
            job_name (str): The name of the job to which this file
                belongs.
            local_file_path (str): The path to the local file to
                upload.

        Returns:
            str: The remote URL where the file was stored.

        Raises:
            FileNotFound: The local file could not be opened.
        """
        base_name = os.path.basename(local_file_path)
        remote_url = self._input_dir(job_name) + '/' + base_name

        try:
            with open(local_file_path, 'rb') as local_file:
                requests.put(remote_url, data=local_file.read())
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise errors.FileNotFound()

        return remote_url

    def _run_job(self, job_desc):
        """
        Start a job on the service.

        Args:
            job_desc (dict): The job description. Must contain keys
                'name', 'workflow' and 'input' with the name of the
                job, the URL to the workflow, and the input description
                dict respectively.

        Returns:
            str: The id given to this job by the service.

        Raises:
            InvalidJob: The job was invalid, perhaps because the
                workflow has not been set.
        """
        r = requests.post(self._jobs, json=job_desc)
        if r.status_code == 400:
            raise errors.InvalidJob()
        return r.json()['id']

    def _cancel_job(self, job_id):
        """
        Cancel a job running on the service.

        Args:
            job_id (str): The id of the job.

        Raises:
            JobNotFound: The job was not found on the service
            CommunicationError: There was a problem communicating with
                the service.
        """
        r = requests.post(self._jobs + '/' + job_id + '/cancel')
        if r.status_code == 404:
            raise errors.JobNotFound()
        if r.status_code != 200:
            raise CommunicationError()


    def _job_state(self, job_id):
        """
        Get the state of the given job.

        Args:
            job_id (str): The server-side job id.

        Returns:
            str: The state that the job is in.

        Raises:
            JobNotFound: The job is unknown to the service.
            CommunicationError: There was a problem communicating with
                the service.
        """
        job = self._get_job_from_service(job_id)
        return job['state']

    def _get_job_log(self, job_id):
        """
        Get the log of the given job.

        Args:
            job_id (str): The server-side job id.

        Returns:
            str: The log as text.

        Raises:
            JobNotFound: The job is unknown to the service.
            CommunicationError: There was a problem communicating with
                the service.
        """
        r = requests.get(self._jobs + '/' + job_id + '/log')
        if r.status_code == 404:
            raise errors.JobNotFound()
        if r.status_code != 200:
            raise errors.CommunicationError(r)
        return r.text

    def _get_outputs(self, job_id):
        """
        Get the outputs of the given job.

        Args:
            job_id (str): The server-side job id.

        Returns:
            dict: The CWL output dictionary.

        Raises:
            JobNotFound: The job is unknown to the service.
            CommunicationError: There was a problem communicating with
                the service.
        """
        job = self._get_job_from_service(job_id)
        return job['output']

    def _delete_input_dir(self, job_name):
        """
        Delete the input directory of the job with the given name.

        Args:
            job_name (str): The client-side name of the job.

        Raises:
            JobNotFound: The input directory did not exist.
            CommunicationError: There was a problem communicating with
                the service.
        """
        import defusedxml.ElementTree as ET

        input_dir = self._input_dir(job_name)
        r = requests.request('PROPFIND', input_dir)
        xml_props = ET.fromstring(r.text)

        file_list = [path_el.text for path_el in xml_props.iter('{DAV:}href')]
        file_list.sort(key=lambda name: -len(name))

        for file_path in file_list:
            r = requests.delete(self._srv_loc + file_path)
            if r.status_code == 404:
                raise errors.JobNotFound(r)
            if r.status_code != 204:
                raise errors.CommunicationError(r)

    def _get_job_from_service(self, job_id):
        """
        Gets a JSON job object for the given job from the service.

        Args:
            job_id (str): The id of the job, as returned by job.id
                after it's been created.

        Returns:
            Job: The requested job.

        Raises:
            JobNotFound: The job is unknown to this service. Either
                it was deleted, or it never existed on this service.
            CommunicationError: There was an error communicating with
                the service.
        """
        r = requests.get(self._jobs + '/' + job_id)
        if r.status_code == 404:
            raise errors.JobNotFound()
        if r.status_code != 200:
            raise error.CommunicationError(r)
        return r.json()

