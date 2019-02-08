from cerise_client import JobAlreadyExists, NoPrimaryFile, OutputFile

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

    def set_workflow(self, workflow):
        """
        Sets the workflow file.

        Only one workflow file may be submitted; if this function is
        called repeatedly, the last workflow file is used.

        Valid types for workflow are:

        str: A path to a local CWL file containing the workflow.
        bytes: A bytes object containing a CWL workflow definition.
        File-like object: A stream to read the workflow content from.

        If you have the workflow content in string format, use
        set_workflow(wf.encode('utf-8')) (i.e. convert to bytes first).

        Args:
            workflow: Either a str containing the path to a file to
                    use, or a File-like object to read content from,
                    or a bytes object containing the workflow contents.
        """
        # scan workflow for inputs and store them in the object
        # list of (name, type) tuples? or {name: type}?
        # dict is better, need to search by name when adding

        # get file object from input
        if not isinstance(workflow, str):
            workflow = 'workflow.cwl', workflow

        # upload the workflow
        self._workflow_url = self._service._upload_file(self.name, workflow)

        # maybe make subdirs for input files and workflow, to allow
        # same names?

    def add_input_file(self, input_name, file_desc):
        """
        Adds an input file for the given workflow input.

        The set_workflow() function must be called before this one.
        The input name must correspond to an input defined in the
        workflow. The file's name must not equal that of any other
        file added using this function or add_secondary_file(), or
        `workflow.cwl`.

        The file_desc argument may be a string, a tuple of a string and
        a file-like object, a tuple of a string and a bytes object,
        or a list containing any of the above.

        If file_desc is a string, it is assumed to contain a path, and
        the corresponding file is uploaded and added as an input file
        with its original name. If file_desc is a tuple of a string and
        a file-like object, the string is used as the file name, and
        the content is read from the object. If file_desc is a tuple
        of a string and a bytes object, the bytes object is used as the
        content and the string as the name. If a list is supplied, each
        list item is processed as above, and the input's value is set
        to a corresponding array of Files.

        If this function is called repeatedly with the same input
        name, the last file(s) will be used.

        Note that this function will upload the input file(s) to the
        service for this job. If the file is large and/or the
        connection slow, then this will take a while.

        Args:
            input_name (str): The name of a workflow input.
            file_desc: A description of the file(s) to add, see above.

        Raises:
            UnknownInput: The input name does not match any in this
                workflow, or the workflow was not yet set.
            FileNotFound: A file to be used was not found.
        """
        # TODO: check against inputs
        if not isinstance(file_desc, list):
            file_descs = [file_desc]
        else:
            file_descs = file_desc

        input_descs = []
        for desc in file_descs:
            remote_url = self._service._upload_file(self.name, desc)
            if isinstance(desc, str):
                basename = os.path.basename(desc)
            elif isinstance(desc, tuple):
                basename = desc[0]
            input_descs.append({
                "class": "File",
                "location": remote_url,
                "basename": basename
                })

        if isinstance(file_desc, list):
            self._input_desc[input_name] = input_descs
        else:
            self._input_desc[input_name] = input_descs[0]

    def add_secondary_file(self, input_name, file_desc):
        """
        Adds a secondary file for the given workflow input.

        A primary file must be set using add_input_file() first.

        The input name must correspond to an input defined in the
        workflow. The file's name must not equal that of any other
        file added using this function or add_secondary_file(), or
        that of the workflow itself.

        The file_desc argument may be a string, a tuple of a string and
        a file-like object, a tuple of a string and a bytes object,
        or a list containing any of the above.

        If file_desc is a string, it is assumed to contain a path, and
        the corresponding file is uploaded and added as a secondary file
        with its original name. If file_desc is a tuple of a string and
        a file-like object, the string is used as the file name, and
        the content is read from the object. If file_desc is a tuple
        of a string and a bytes object, the bytes object is used as the
        content and the string as the name.

        Note that this function will upload the input file to the
        service for this job. If the file is large and/or the
        connection slow, then this will take a while.

        Args:
            input_name (str): The name of a workflow input.
            file_desc: The file to set, see above.

        Raises:
            UnknownInput: The input name does not match any in this
                workflow, or the workflow was not yet set.
            FileNotFound: The file to be used was not found.
            NoPrimaryFile: No primary file was set yet via \
                add_input_file().
        """
        if not input_name in self._input_desc:
            raise NoPrimaryFile('Primary file not set yet')

        remote_url = self._service._upload_file(self.name, file_desc)

        if isinstance(file_desc, str):
            basename = os.path.basename(file_desc)
        elif isinstance(file_desc, tuple):
            basename = file_desc[0]

        if not 'secondaryFiles' in self._input_desc[input_name]:
            self._input_desc[input_name]['secondaryFiles'] = list()

        self._input_desc[input_name]['secondaryFiles'].append({
                "class": "File",
                "location": remote_url,
                "basename": basename
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
            raise JobAlreadyExists()

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
