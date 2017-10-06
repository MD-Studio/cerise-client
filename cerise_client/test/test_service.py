import docker
import json
import os
import pytest
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
print(sys.path)

# clean up any mess left over from previous failed tests
from .clean_up import clean_up
clean_up()

import cerise_client.service as cs
import cerise_client.errors as ce
from .clean_up import clean_up_service

from .fixtures import docker_client, test_image, test_service, this_dir

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
        cs.service_from_dict(test_service_dict)

def test_create_service(docker_client):
    srv = cs.create_service('cerise_client_test_service', 29593,
            'mdstudio/cerise:develop')
    assert isinstance(srv, cs.Service)
    clean_up_service('cerise_client_test_service')

def test_create_existing_service(test_container):
    with pytest.raises(ce.ServiceAlreadyExists):
        cs.create_service('cerise_client_test_service', 29593,
                'mdstudio/cerise:develop')

def test_create_service_port_occupied(test_container):
    with pytest.raises(ce.PortNotAvailable):
        cs.create_service('cerise_client_test_service2', 29593,
                'mdstudio/cerise:develop')
    clean_up_service('cerise_client_test_service2')

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

def test_create_job(test_service):
    job = test_service.create_job('test_job')
    assert job.name == 'test_job'

def test_create_job_twice(test_service):
    test_service.create_job('test_job')
    with pytest.raises(ce.JobAlreadyExists):
        test_service.create_job('test_job')

def test_get_job_by_id(test_service, this_dir):
    job = test_service.create_job('test_get_job_by_id')
    job.set_workflow(os.path.join(this_dir, 'test_workflow2.cwl'))
    job.add_input_file('input_file', os.path.join(this_dir, 'test_workflow2.cwl'))
    job.run()
    job2 = test_service.get_job_by_id(job.id)
    assert job.id == job2.id
    assert job.name == job2.name
    assert job._service == job2._service
    assert job._inputs == job2._inputs
    assert job._workflow_url == job2._workflow_url
    assert job._input_desc == job2._input_desc
    assert job._outputs == job2._outputs

def test_nonexistent_job_by_id(test_service):
    with pytest.raises(ce.JobNotFound):
        test_service.get_job_by_id('surely_this_id_does_not_exist')

def test_list_jobs(test_service, this_dir):
    job_list = test_service.list_jobs()
    assert len(job_list) == 0

    job = test_service.create_job('test_list_jobs1')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 1)
    job.run()

    job_list = test_service.list_jobs()
    assert len(job_list) == 1
    assert job_list[0].name == 'test_list_jobs1'

    job2 = test_service.create_job('test_list_jobs2')
    job2.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job2.set_input('time', 2)
    job2.run()

    job_list = test_service.list_jobs()
    assert len(job_list) == 2
    assert 'test_list_jobs1' in [j.name for j in job_list]
    assert 'test_list_jobs2' in [j.name for j in job_list]

    time.sleep(2)
    job.delete()
    time.sleep(3)

    job_list = test_service.list_jobs()
    assert len(job_list) == 1
    assert job_list[0].name == 'test_list_jobs2'
