from cerise_client import (CommunicationError, FileNotFound, InvalidJob, Job,
                           JobAlreadyExists, JobNotFound)

import docker

import errno
from io import BytesIO
import os
import requests
import tarfile
import tempfile
import time


class Service:
    def __init__(self, host='localhost', port=29593):
        """
        Create a new Service object representing an actual service.

        Note that this does not connect to the service, that is done
        as needed when the methods are called. So it will not raise
        even if the given service does not exist or you have no
        network connection.

        Args:
            host (str): The hostname of the service we are connecting
                to. May be prepended by 'http://' or 'https://', with
                https the default if a bare hostname is given.
            port (int): The port number on which the service runs.
        """
        if not host.startswith('http://') and not host.startswith('https://'):
            host = 'https://{}'.format(host)
        self._srv_loc = '{}:{}'.format(host, port)
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
            raise JobNotFound("The job could not be found.")

        if r.status_code != 204:
            raise CommunicationError(r)


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
            raise JobNotFound()
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
            raise CommunicationError(r)
        jobs_json = r.json()
        return [Job(self, job['name'], job['id'], None,
                    job['workflow'], job['input'])
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
            raise JobAlreadyExists()

    def _upload_file(self, job_name, file_desc):
        """
        Upload a local file to a remote input data directory for a
        given job. Directory must have been made first using
        _create_jobdir().

        If a file with the same name already exists, it is overwritten
        silently.

        For the file_desc argument, the following types are legal:

        str: A path to a file, whose contents and name will be used.
        Tuple[str, FileLike]: A file name to store under, and a
                file-like object to read content from, and a filename
                to store under.
        Tuple[str, bytes]: A file name to store under, and a bytes
                object with content.

        If a file-like object is passed, it will be closed after having
        been read.

        Args:
            job_name (str): The name of the job to which this file
                belongs.
            file_desc: The file's content and name, see above.

        Returns:
            str: The remote URL where the file was stored.
        """
        if isinstance(file_desc, str):
            file_name = os.path.basename(file_desc)
            try:
                content = open(file_desc, 'rb')
            except IOError as e:
                if e.errno == errno.ENOENT:
                    raise FileNotFound()
        else:
            file_name, content = file_desc
            if isinstance(content, bytes):
                content = BytesIO(content)

        try:
            remote_url = self._input_dir(job_name) + '/' + file_name
            requests.put(remote_url, data=content)
        finally:
            content.close()

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
            raise InvalidJob()
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
            raise JobNotFound()
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
            raise JobNotFound()
        if r.status_code != 200:
            raise CommunicationError(r)
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
                raise JobNotFound(r)
            if r.status_code != 204:
                raise CommunicationError(r)

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
            raise JobNotFound()
        if r.status_code != 200:
            raise error.CommunicationError(r)
        return r.json()

