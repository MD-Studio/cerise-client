from . import errors
from .job import Job

import docker

import os
import requests
import time

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

def create_service(srv_name, port, srv_type, user_name=None, password=None):
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
        _ = dc.containers.get(srv_name)
        raise errors.ServiceAlreadyExists()
    except docker.errors.NotFound:
        pass

    if user_name == '':
        user_name = None

    environment = {}
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
        raise

    time.sleep(1)

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
        """The name of this service, and its Docker container."""
        self._port = port
        """The port number on localhost that the service listens on."""

        self._srv_loc = 'http://localhost:' + str(self._port)
        self._filestore = self._srv_loc + '/files'
        self._jobs = self._srv_loc + '/jobs'

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
        # Give it some time to start, so subsequent calls work
        time.sleep(1)

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

        Raises:
            JobAlreadyExists: A job with this name already exists
                on this service.
        """
        self._create_input_dir(job_name)
        return Job(self, job_name)

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
                declared_inputs, job['workflow'], job['input'],
                job['output'])

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
                    job['workflow'], job['input'], job['output'])
                for job in jobs_json]

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

        with open(local_file_path, 'rb') as local_file:
            requests.put(remote_url, data=local_file.read())

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

    def _get_log(self, job_id):
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

    def _delete_job(self, job_name, job_id):
        """
        Delete a job from the service.

        Args:
            job_name (str): The client-side name of the job.
            job_id (str): The server-side job id.
            input_files ([str]): A list of file names in the input
                directory.

        Raises:
            JobNotFound: The job is unknown to the service.
            CommunicationError: There was a problem communicating with
                the service.
        """
        self._delete_input_dir(job_name)
        r = requests.delete(self._jobs + '/' + job_id)

        if r.status_code == 404:
            raise errors.JobNotFound("Either the input directory or the job could not be found.")

        if r.status_code != 204:
            raise errors.CommunicationError(r, r2)

    def _delete_input_dir(self, job_name):
        import defusedxml.ElementTree as ET

        input_dir = self._input_dir(job_name)
        r = requests.request('PROPFIND', input_dir)
        xml_props = ET.fromstring(r.text)

        file_list = [path_el.text for path_el in xml_props.iter('{DAV:}href')]
        file_list.sort(key=lambda name: -len(name))

        for file_path in file_list:
            r = requests.delete(self._srv_loc + file_path)
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

