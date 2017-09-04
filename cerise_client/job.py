class Job:
    def __init__(self, service, name):
        """
        Create a new Job object.

        This will not actually make a job on the service. Use
        Service.create_job() to make a job.

        Args:
            service (Service): The service this job will run on.
            name (str): The name for the job.

        Raises:
            JobAlreadyExists: A job with the given name already exists
                on this service.
        """
        pass

    def set_workflow(self, file_path):
        """
        Sets the workflow file.

        Only one workflow file may be submitted, if this function is
        called repeatedly, the last workflow file is used.

        Args:
            file_path (str): The path to the CWL file defining the
                workflow to be run. The file's name must not equal
                that of any file added using add_input_file().

        Raises:
            FileNotFound: The workflow file was not found at the given
                path.
        """
        pass

    def add_input_file(self, input_name, file_path):
        """
        Adds an input file for the given workflow input.

        The set_workflow() function must be called before this one.
        The input name must correspond to an input defined in the
        workflow. The file's name must not equal that of any other
        file added using this function, or that of the workflow
        itself.

        If this function is called repeatedly with the same input
        name, the last file will be used.

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
        """
        pass

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
        pass

    def run():
        """
        Starts the job running on the associated service.
        """
        pass
