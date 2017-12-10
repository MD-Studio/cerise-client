import docker
import json
import os
import pytest
import requests
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
print(sys.path)

# clean up any mess left over from previous failed tests
from .clean_up import clean_up
clean_up()

import cerise_client.service as cs
import cerise_client.errors as ce
import cerise_client.job as cj
from .clean_up import clean_up_service

from .fixtures import docker_client, test_image, test_service, this_dir
from .fixtures import create_test_job

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

def test_managed_service_exists(test_container):
    exists = cs.managed_service_exists('cerise_client_test_service')
    assert isinstance(exists, bool)
    assert exists

def test_managed_service_does_not_exist():
    exists = cs.managed_service_exists('cerise_client_test_service')
    assert isinstance(exists, bool)
    assert not exists

def test_get_managed_service(test_container):
    srv = cs.get_managed_service('cerise_client_test_service', 29593)
    assert isinstance(srv, cs.Service)

def test_get_missing_managed_service():
    with pytest.raises(ce.ServiceNotFound):
        cs.get_managed_service('cerise_client_test_service', 29593)

def test_service_from_dict(test_container, test_service_dict):
    srv = cs.service_from_dict(test_service_dict)
    assert isinstance(srv, cs.Service)

def test_missing_service_from_dict(test_service_dict):
    with pytest.raises(ce.ServiceNotFound):
        cs.service_from_dict(test_service_dict)

def test_create_managed_service(docker_client):
    srv = cs.create_managed_service('cerise_client_test_service', 29593,
            'mdstudio/cerise:develop')
    assert isinstance(srv, cs.Service)
    clean_up_service('cerise_client_test_service')

def test_create_existing_managed_service(test_container):
    with pytest.raises(ce.ServiceAlreadyExists):
        cs.create_managed_service('cerise_client_test_service', 29593,
                'mdstudio/cerise:develop')

def test_create_managed_service_port_occupied(test_container):
    with pytest.raises(ce.PortNotAvailable):
        cs.create_managed_service('cerise_client_test_service2', 29593,
                'mdstudio/cerise:develop')
    clean_up_service('cerise_client_test_service2')

def test_create_managed_service_object():
    srv = cs.Service('cerise_client_test_service', 29593)
    assert srv._name == 'cerise_client_test_service'
    assert srv._port == 29593

def test_destroy_managed_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'

    cs.destroy_managed_service(test_service)

    with pytest.raises(docker.errors.NotFound):
        docker_client.containers.get('cerise_client_test_service')

def test_destroy_missing_managed_service(docker_client):
    srv = cs.Service('non_existing_service', 29593)
    with pytest.raises(ce.ServiceNotFound):
        cs.destroy_managed_service(srv)

def test_require_managed_service(docker_client):
    srv = cs.require_managed_service('cerise_client_test_service', 29593,
            'mdstudio/cerise:develop')
    assert isinstance(srv, cs.Service)
    clean_up_service('cerise_client_test_service')

def test_require_existing_managed_service(docker_client, test_service):
    srv = cs.require_managed_service('cerise_client_test_service', 29593,
            'mdstudio/cerise:develop')
    assert isinstance(srv, cs.Service)
    assert srv._name == 'cerise_client_test_service'
    assert srv._port == 29593

def test_require_managed_server_occupied_port(docker_client, test_service):
    with pytest.raises(ce.PortNotAvailable):
        srv = cs.require_managed_service('cerise_client_test_service2', 29593,
                'mdstudio/cerise:develop')

def test_start_running_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'

    cs.start_managed_service(test_service)

    container.reload()
    assert container.status == 'running'

def test_start_stopped_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'
    container.stop()
    container.reload()
    assert container.status == 'exited'

    cs.start_managed_service(test_service)

    container.reload()
    assert container.status == 'running'

def test_stop_running_managed_service(docker_client, test_service):
    cs.stop_managed_service(test_service)

    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'exited'

def test_stop_stopped_managed_service(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    container.stop()
    container.reload()
    assert container.status == 'exited'

    cs.stop_managed_service(test_service)

    container.reload()
    assert container.status == 'exited'

def test_managed_service_is_running(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    assert container.status == 'running'
    assert cs.managed_service_is_running(test_service)

def test_is_not_running(docker_client, test_service):
    container = docker_client.containers.get('cerise_client_test_service')
    container.stop()
    container.reload()
    assert container.status == 'exited'

    assert not cs.managed_service_is_running(test_service)

def test_service_to_dict(test_service):
    dict_ = cs.service_to_dict(test_service)
    assert dict_['name'] == 'cerise_client_test_service'
    assert dict_['port'] == 29593

def test_service_serialisation(test_service):
    dict_ = cs.service_to_dict(test_service)
    json_dict = json.dumps(dict_)
    dict2 = json.loads(json_dict)
    srv = cs.service_from_dict(dict2)

    assert cs.managed_service_is_running(srv)
    assert srv._name == 'cerise_client_test_service'
    assert srv._port == 29593

def test_create_job(test_service):
    job = test_service.create_job('test_create_job')
    assert job.name == 'test_create_job'

def test_create_job_twice(test_service):
    test_service.create_job('test_create_job_twice')
    with pytest.raises(ce.JobAlreadyExists):
        test_service.create_job('test_create_job_twice')

def test_destroy_job(test_service, this_dir):
    job = create_test_job(test_service, this_dir, 'test_destroy_job')
    counts = job.outputs['counts']

    test_service.destroy_job(job)

    # check that inputs are gone
    r = requests.get('http://localhost:29593/files/input/test_job_delete/')
    assert r.status_code == 404

    # check that outputs are gone, after the back-end has had time to respond
    time.sleep(2)
    with pytest.raises(ce.MissingOutput):
        _ = counts.text

    # check that the job is gone
    with pytest.raises(ce.JobNotFound):
        _ = job.state

def test_destroy_nonexistant_job(test_service):
    job = cj.Job(test_service, 'test_destroy_nonexistant_job', 'nonexistant_id')
    with pytest.raises(ce.JobNotFound):
        test_service.destroy_job(job)

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

    _ = job.outputs
    job3 = test_service.get_job_by_id(job.id)
    assert job._outputs == job3._outputs

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
    test_service.destroy_job(job)
    time.sleep(3)

    job_list = test_service.list_jobs()
    assert len(job_list) == 1
    assert job_list[0].name == 'test_list_jobs2'

def test_get_job_by_name(test_service, this_dir):
    job = test_service.create_job('test_find_job_by_name')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 1)
    job.run()

    job2 = test_service.get_job_by_name('test_find_job_by_name')
    assert job.id == job2.id

    with pytest.raises(ce.JobNotFound):
        job3 = test_service.get_job_by_name('no_such_job')

def test_get_missing_job_by_name(test_service):
    with pytest.raises(ce.JobNotFound):
        job = test_service.get_job_by_name('does_not_exist')

def test_get_log(test_service):
    # Give it a bit of time to start up, esp. on Travis
    time.sleep(5)
    log = test_service.get_log()
    assert isinstance(log, str) or isinstance(log, unicode)
    assert log != ''
