import docker
import json
import os
import pytest
import requests
import sys
import time

from cerise_client import Job, JobAlreadyExists, JobNotFound, MissingOutput

from cerise_client.test.conftest import create_test_job


def test_create_job(test_service):
    job = test_service.create_job('test_create_job')
    assert job.name == 'test_create_job'

def test_create_job_twice(test_service):
    test_service.create_job('test_create_job_twice')
    with pytest.raises(JobAlreadyExists):
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
    with pytest.raises(MissingOutput):
        _ = counts.text

    # check that the job is gone
    with pytest.raises(JobNotFound):
        _ = job.state

def test_destroy_nonexistant_job(test_service):
    job = Job(test_service, 'test_destroy_nonexistant_job', 'nonexistant_id')
    with pytest.raises(JobNotFound):
        test_service.destroy_job(job)

def test_get_job_by_id(test_service, this_dir):
    job = test_service.create_job('test_get_job_by_id')
    job.set_workflow(str(this_dir / 'test_workflow2.cwl'))
    job.add_input_file('input_file', str(this_dir / 'test_workflow2.cwl'))
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
    with pytest.raises(JobNotFound):
        test_service.get_job_by_id('surely_this_id_does_not_exist')

def test_list_jobs(test_service, this_dir):
    job_list = test_service.list_jobs()
    for job in job_list:
        test_service.destroy_job(job)
    time.sleep(3)
    job_list = test_service.list_jobs()
    assert len(job_list) == 0

    job = test_service.create_job('test_list_jobs1')
    job.set_workflow(str(this_dir / 'test_workflow3.cwl'))
    job.set_input('time', 1)
    job.run()

    job_list = test_service.list_jobs()
    assert len(job_list) == 1
    assert job_list[0].name == 'test_list_jobs1'

    job2 = test_service.create_job('test_list_jobs2')
    job2.set_workflow(str(this_dir / 'test_workflow3.cwl'))
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
    job.set_workflow(str(this_dir / 'test_workflow3.cwl'))
    job.set_input('time', 1)
    job.run()

    job2 = test_service.get_job_by_name('test_find_job_by_name')
    assert job.id == job2.id

    with pytest.raises(JobNotFound):
        job3 = test_service.get_job_by_name('no_such_job')

def test_get_missing_job_by_name(test_service):
    with pytest.raises(JobNotFound):
        job = test_service.get_job_by_name('does_not_exist')
