import docker
import os
import pytest

import cerise_client.service as cs

@pytest.fixture()
def docker_client(request):
    return docker.from_env()

@pytest.fixture()
def test_image(request):
    """Get a plain cerise image for testing.

    Ignores errors; we may have a local image available already,
    in which case we want to continue, otherwise the other
    tests will fail.
    """
    docker_client = docker.from_env()
    try:
        docker_client.images.pull('mdstudio/cerise:develop')
    except docker.errors.APIError:
        pass
    return 'mdstudio/cerise:develop'

@pytest.fixture()
def test_service(request, test_image):
    from .clean_up import clean_up_service
    clean_up_service('cerise_client_test_service')
    srv = cs.create_service('cerise_client_test_service', 29593,
            test_image)

    yield srv

    clean_up_service('cerise_client_test_service')

@pytest.fixture()
def this_dir(request):
    return os.path.dirname(__file__)

