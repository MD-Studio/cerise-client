class OutputFile:
    def __init__(self, uri):
        """
        Creates a new OutputFile object.

        Args:
            uri (str): The URI at which the file is available.
        """
        pass

    def save_as(self, file_path):
        """
        Downloads the file and saves it to disk.

        Args:
            file_path (str): The path to save the file to.
        Raises:
            FileNotFound: The given path references a directory that
                does not exist.
        """
        pass

    @property
    def text(self):
        """
        Returns the file in text form.

        Autodetects the encoding and converts to a standard Python
        unicode str.

        Returns:
            str: The contents of the file as text.
        """
        pass

    @property
    def content(self):
        """
        Returns the file in binary form.

        Returns:
            bytes: The contents of the file as raw bytes.
        """
        pass
