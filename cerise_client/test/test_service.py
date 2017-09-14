import docker
import json
import os
import pytest
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
print(sys.path)

import cerise_client.service as cs
import cerise_client.errors as ce

def _clean_up_service(srv_name):
    dc = docker.from_env()
    try:
        test_srv = dc.containers.get(srv_name)
        test_srv.stop()
        test_srv.remove()
    except docker.errors.NotFound:
        pass

# Clean up any mess left over from previous failed tests.
_clean_up_service('cerise_client_test_service')
_clean_up_service('cerise_client_test_service2')


@pytest.fixture()
def docker_client(request):
    return docker.from_env()

@pytest.fixture()
def test_image(request, docker_client):
    """Get a plain cerise image for testing.

    Ignores errors; we may have a local image available already,
    in which case we want to continue, otherwise the other
    tests will fail.
    """
    try:
        docker_client.images.pull('mdstudio/cerise:develop')
    except:
        pass
    return 'mdstudio/cerise:develop'

@pytest.fixture()
def test_container(request, test_image, docker_client):
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
def test_service(request, test_image, docker_client):
    _clean_up_service('cerise_client_test_service')
    srv = cs.create_service('cerise_client_test_service', 29593,
            test_image, '', password='')

    yield srv

    _clean_up_service('cerise_client_test_service')

@pytest.fixture()
def test_service_dict(request):
    return {
            'name': 'cerise_client_test_service',
            'port': 29593
            }

def test_service_exists(test_container):
    exists = cs.service_exists('cerise_client_test_service')
    assert isinstance(exists, bool)
    assert exists

def test_service_does_not_exist():
    exists = cs.service_exists('cerise_client_test_service')
    assert isinstance(exists, bool)
    assert not exists

def test_get_service(test_container):
    srv = cs.get_service('cerise_client_test_service', 29593)
    assert isinstance(srv, cs.Service)

def test_get_missing_service():
    with pytest.raises(ce.ServiceNotFound):
        cs.get_service('cerise_client_test_service', 29593)

def test_service_from_dict(test_container, test_service_dict):
    srv = cs.service_from_dict(test_service_dict)
    assert isinstance(srv, cs.Service)

def test_missing_service_from_dict(test_service_dict):
    with pytest.raises(ce.ServiceNotFound):
        srv = cs.service_from_dict(test_service_dict)

def test_create_service(docker_client):
    srv = cs.create_service('cerise_client_test_service', 29593,
            'mdstudio/cerise:develop', '', password='')
    assert isinstance(srv, cs.Service)
    _clean_up_service('cerise_client_test_service')

def test_create_existing_service(test_container):
    with pytest.raises(ce.ServiceAlreadyExists):
        srv = cs.create_service('cerise_client_test_service', 29593,
                'mdstudio/cerise:develop', '', password='')

def test_create_service_port_occupied(test_container):
    with pytest.raises(ce.PortNotAvailable):
        srv = cs.create_service('cerise_client_test_service2', 29593,
                'mdstudio/cerise:develop', '', password='')
        _clean_up_service('cerise_client_test_service2')

def test_create_service_object():
    srv = cs.Service('cerise_client_test_service', 29593)
    assert srv._name == 'cerise_client_test_service'
    assert srv._port == 29593

def test_destroy_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'

    test_service.destroy()

    with pytest.raises(docker.errors.NotFound):
        docker_client.containers.get('cerise_client_test_service')

def test_start_running_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'

    test_service.start()

    container.reload()
    assert container.status == 'running'

def test_start_stopped_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'
    container.stop()
    container.reload()
    assert container.status == 'exited'

    test_service.start()

    container.reload()
    assert container.status == 'running'

def test_stop_running_service(docker_client, test_service):
    test_service.stop()

    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'exited'

def test_stop_stopped_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    container.stop()
    container.reload()
    assert container.status == 'exited'

    test_service.stop()

    container.reload()
    assert container.status == 'exited'

def test_is_running(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'
    assert test_service.is_running()

def test_is_not_running(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    container.stop()
    container.reload()
    assert container.status == 'exited'

    assert not test_service.is_running()

def test_service_to_dict(test_service):
    dict_ = test_service.to_dict()
    assert dict_['name'] == 'cerise_client_test_service'
    assert dict_['port'] == 29593

def test_service_serialisation(test_service):
    dict_ = test_service.to_dict()
    json_dict = json.dumps(dict_)
    dict2 = json.loads(json_dict)
    srv = cs.service_from_dict(dict2)

    assert srv.is_running()
    assert srv._name == 'cerise_client_test_service'
    assert srv._port == 29593
