class ServiceNotFound(Exception):
    """
    The given service does not exist on this machine. Either it was
    never created, or it was destroyed, or the Docker container running
    it was destroyed by an outside influence.
    """
    pass

class JobNotFound(Exception):
    """
    The given job does not exist on this service. Either it never
    existed, or it was deleted.
    """
    pass

class JobAlreadyExists(Exception):
    """
    You tried to create a job with a name that already existed on this
    service. Jobs must have a unique name.
    """
    pass

class UnknownInput(Exception):
    """
    You tried to set the value for an input that is not in the workflow
    for the job you are creating.
    """
