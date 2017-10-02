import requests

class OutputFile:
    def __init__(self, uri):
        """
        Creates a new OutputFile object.

        Args:
            uri (str): The URI at which the file is available.
        """
        self._uri = uri
        """The URI at which the file is available."""

    def save_as(self, file_path):
        """
        Downloads the file and saves it to disk.

        Args:
            file_path (str): The path to save the file to.
        Raises:
            FileNotFound: The given path references a directory that
                does not exist.
        """
        with open(file_path, 'wb') as f:
            f.write(self._get_file().content)

    @property
    def text(self):
        """
        Returns the file in text form.

        Autodetects the encoding and converts to a standard Python
        unicode str.

        Returns:
            str: The contents of the file as text.
        """
        return self._get_file().text

    @property
    def content(self):
        """
        Returns the file in binary form.

        Returns:
            bytes: The contents of the file as raw bytes.

        Raises:
            FileNotFound: The given path references a directory that
                does not exist.
        """
        return self._get_file().content

    def _get_file(self):
        """
        Returns a requests.Response object with the remote file.

        Returns:
            (requests.Response): The remote file
        Raises:
            FileNotFound: The given path references a directory that
                does not exist.
        """
        r = requests.get(self._uri)
        if r.status_code == 404:
            raise FileNotFoundError()
        return r
