class JobNotFound(Exception):
    """
    The given job does not exist on this service. Either it never
    existed because it hasn't been submitted yet (did you call
    job.run()?), or it was deleted.
    """
    pass

class JobAlreadyExists(Exception):
    """
    You tried to create a job with a name that already existed on this
    service. Jobs must have a unique name, and cannot be submitted
    twice.
    """
    pass

class InvalidJob(Exception):
    """
    You submitted an invalid job. Did you forget to add a workflow?
    """
    pass

class UnknownInput(Exception):
    """
    You tried to set the value for an input that is not in the workflow
    for the job you are creating. Did you add the workflow first?
    """
    pass

class NoPrimaryFile(Exception):
    """
    You tried to set a secondary file for an input for which no primary
    file is set. Use add_input_file() first, then add_secondary_file()
    with the same input name.
    """

class MissingOutput(Exception):
    """
    The output returned by the service does not refer to an existing
    file. Maybe the job was deleted?
    """

class FileNotFound(Exception):
    """
    The file you tried to set as a workflow input was not found. Is the
    path correct? Does the file exist?
    """

class CommunicationError(Exception):
    """
    There was an error communicating with the service.
    """
    pass
