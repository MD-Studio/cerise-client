from .output_file import OutputFile
from . import errors

import os

class Job:
    def __init__(self, service, name, job_id=None, inputs=None,
            workflow=None, input_desc=None):
        """
        Create a new Job object.

        This will not actually make a job on the service. Use
        Service.create_job() to make a job.

        Args:
            service (Service): The service this job will run on.
            name (str): The name for the job.
        """
        self.id = job_id
        """str: The service-assigned id of this job."""

        self.name = name
        """str: The name of this job."""

        self._service = service
        """Service: The service that this job runs on."""

        self._inputs = inputs
        """[str]: List of declared inputs of the workflow."""
        if self._inputs is None: self._inputs = []

        self._workflow_url = workflow
        """str: The remote path to the uploaded workflow file."""

        self._input_desc = input_desc
        """dict: The input description object to be submitted."""
        if self._input_desc is None: self._input_desc = {}

        self._outputs = None
        """dict: Cached results."""

    @property
    def state(self):
        """
        The state that this job is in.

        None if the job has not been started yet. One of 'Waiting',
        'Running', 'Success', 'Cancelled', 'TemporaryFailure',
        'PermanentFailure', or 'SystemError'.
        """
        if self.id is None:
            return None
        return self._service._job_state(self.id)

    @property
    def log(self):
        """
        str: The job's log, as produced by the service.
        """
        if self.id is not None:
            return self._service._get_job_log(self.id)
        return None

    def set_workflow(self, file_path):
        """
        Sets the workflow file.

        Only one workflow file may be submitted; if this function is
        called repeatedly, the last workflow file is used.

        Args:
            file_path (str): The path to the CWL file defining the
                workflow to be run. The file's name must not equal
                that of any file added using add_input_file().

        Raises:
            FileNotFound: The workflow file was not found at the
                given path.
        """
        # scan workflow for inputs and store them in the object
        # list of (name, type) tuples? or {name: type}?
        # dict is better, need to search by name when adding

        # upload the workflow
        self._workflow_url = self._service._upload_file(self.name, file_path)

        # maybe make subdirs for input files and workflow, to allow
        # same names?

    def add_input_file(self, input_name, file_path):
        """
        Adds an input file for the given workflow input.

        The set_workflow() function must be called before this one.
        The input name must correspond to an input defined in the
        workflow. The file's name must not equal that of any other
        file added using this function or add_secondary_file(), or
        that of the workflow itself.

        The file_path argument may be an array of strings, in which
        case an array of files will be passed as the value of this
        workflow input.

        If this function is called repeatedly with the same input
        name, the last file(s) will be used.

        Note that this function will upload the input file(s) to the
        service for this job. If the file is large and/or the
        connection slow, then this will take a while.

        Args:
            input_name (str): The name of a workflow input.
            file_path (Union[str, str[]]): The path to the input file \
                    or files to use.

        Raises:
            UnknownInput: The input name does not match any in this
                workflow, or the workflow was not yet set.
            FileNotFound: A file to be used was not found.
        """
        # TODO: check against inputs
        if not isinstance(file_path, list):
            file_paths = [file_path]
        else:
            file_paths = file_path

        input_descs = []
        for path in file_paths:
            remote_url = self._service._upload_file(self.name, path)
            input_descs.append({
                "class": "File",
                "location": remote_url,
                "basename": os.path.basename(path)
                })

        if isinstance(file_path, list):
            self._input_desc[input_name] = input_descs
        else:
            self._input_desc[input_name] = input_descs[0]

    def add_secondary_file(self, input_name, file_path):
        """
        Adds a secondary file for the given workflow input.

        A primary file must be set using add_input_file() first.

        The input name must correspond to an input defined in the
        workflow. The file's name must not equal that of any other
        file added using this function or add_secondary_file(), or
        that of the workflow itself.

        Note that this function will upload the input file to the
        service for this job. If the file is large and/or the
        connection slow, then this will take a while.

        Args:
            input_name (str): The name of a workflow input.
            file_path (str): The path to the input file to use.

        Raises:
            UnknownInput: The input name does not match any in this
                workflow, or the workflow was not yet set.
            FileNotFound: The file to be used was not found.
            NoPrimaryFile: No primary file was set yet via \
                add_input_file().
        """
        if not input_name in self._input_desc:
            raise errors.NoPrimaryFile('Primary file not set yet')

        remote_url = self._service._upload_file(self.name, file_path)

        if not 'secondaryFiles' in self._input_desc[input_name]:
            self._input_desc[input_name]['secondaryFiles'] = []

        self._input_desc[input_name]['secondaryFiles'].append({
                "class": "File",
                "location": remote_url,
                "basename": os.path.basename(file_path)
                })

    def set_input(self, input_name, value):
        """
        Sets the value of a workflow input.

        Use this function for setting the value of numerical or string
        inputs. For File inputs, use add_input_file.

        Args:
            input_name (str): The name of a workflow input.
            value (Union[str,int,double]): The value to set.

        Raises:
            UnknownInput: The input name does not match any in this
                workflow, or the workflow was not yet set.
        """
        # TODO: check against inputs
        self._input_desc[input_name] = value

    def run(self):
        """
        Starts the job running on the associated service.

        Returns:
            str: The id given to this job by the service.
        """
        if self.id is not None:
            raise errors.JobAlreadyExists()

        job_desc = {
                'name': self.name,
                'workflow': self._workflow_url,
                'input': self._input_desc
                }
        self.id = self._service._run_job(job_desc)
        return self.id

    def is_running(self):
        """
        Returns True when the job is still running, False when it is
        done.

        Returns:
            boolean: Whether the job is running.
        """
        state = self.state      # fetch only once
        return state == 'Waiting' or state == 'Running'

    def cancel(self):
        """
        Cancel this job; stop it running at the compute service.
        After this function is called, the job's state will eventually
        become 'Cancelled', unless it was already complete, in which
        case its state will be its normal final state.
        """
        if self.id is not None:
            self._service._cancel_job(self.id)

    @property
    def outputs(self):
        """
        Returns a dictionary of output objects. Keys are taken from
        the names of the outputs in the submitted workflows, the
        values are the corresponding results.

        If an output is of type File, an object of class
        OutputFile is returned as the value.

        If no outputs are available, returns None.

        Returns:
            Union[dict, None]: Output values or None.
        """
        if self._outputs is None:
            outputs = self._service._get_outputs(self.id)
            if outputs != {}:
                self._outputs = {}
                for key, value in outputs.items():
                    if isinstance(value, dict):
                        if value.get('class', '') == 'File':
                            self._outputs[key] = OutputFile(value['location'])
                    else:
                        self._outputs[key] = value

        return self._outputs
