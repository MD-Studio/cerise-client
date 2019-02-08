import docker
from pathlib import Path
import pytest
import requests
import time

import cerise_client.service as cs


def clean_up_service(srv_name):
    dc = docker.from_env()
    try:
        test_srv = dc.containers.get(srv_name)
        test_srv.stop()
        test_srv.remove()
    except docker.errors.NotFound:
        pass


# Clean up any mess left over from previous failed tests.
@pytest.fixture(scope="session")
def clean_up():
    clean_up_service('cerise_client_test_service')
    clean_up_service('cerise_client_test_service2')


@pytest.fixture(scope="session")
def docker_client(request):
    return docker.from_env()


@pytest.fixture(scope="session")
def test_image(clean_up, docker_client):
    """Get a plain cerise image for testing.

    Ignores errors; we may have a local image available already,
    in which case we want to continue, otherwise the other
    tests will fail.
    """
    try:
        docker_client.images.pull('mdstudio/cerise:develop')
    except docker.errors.APIError:
        pass
    return 'mdstudio/cerise:develop'


@pytest.fixture(scope="session")
def test_service(test_image, docker_client):
    env = {'CERISE_STORE_LOCATION_CLIENT': 'http://localhost:29593/files'}
    docker_client.containers.run(
            test_image, name='cerise_client_test_service',
            ports={'29593/tcp': ('127.0.0.1', 29593) },
            environment=env, detach=True)
    time.sleep(1)
    srv = cs.Service('http://localhost', 29593)
    yield srv
    clean_up_service('cerise_client_test_service')


@pytest.fixture()
def this_dir():
    return Path(__file__).parent


@pytest.fixture()
def test_container(test_image, docker_client):
    try:
        container = docker_client.containers.get('cerise_client_test_service')
    except docker.errors.NotFound:
        image = docker_client.images.get(test_image)

        container = docker_client.containers.run(
                image,
                name='cerise_client_test_service',
                ports={'29593/tcp': ('127.0.0.1', 29593) },
                detach=True)

    yield container

    container.stop()
    container.remove()


@pytest.fixture()
def test_service_dict():
    return {
            'name': 'cerise_client_test_service',
            'port': 29593
            }


def create_test_job(test_service, this_dir, name):
    job = test_service.create_job(name)
    r = requests.get('http://localhost:29593/files/input/' + name + '/')
    assert r.status_code == 200

    # run job to create some outputs
    job.set_workflow(str(this_dir / 'test_workflow2.cwl'))
    job.add_input_file('input_file', str(this_dir / 'test_workflow2.cwl'))
    job.run()
    while job.is_running():
        time.sleep(0.1)
    assert job.state == 'Success'

    counts = job.outputs['counts']
    assert counts.text != ''
    return job
