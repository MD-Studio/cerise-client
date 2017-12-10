import os
import pytest
import requests
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# clean up any mess left over from previous failed tests
from .clean_up import clean_up
clean_up()

import cerise_client.job as cj
import cerise_client.errors as ce

from .fixtures import test_image, test_service, this_dir
from .fixtures import create_test_job


def test_create_job_object(test_service):
    _ = cj.Job(test_service, 'test_job')

def test_set_workflow(test_service, this_dir):
    job = test_service.create_job('test_set_workflow')
    workflow_path = os.path.join(this_dir, 'test_workflow.cwl')
    job.set_workflow(workflow_path)
    # check that it's on the service?
    assert job._workflow_url == 'http://localhost:29593/files/input/test_set_workflow/test_workflow.cwl'
    r = requests.get('http://localhost:29593/files/input/test_set_workflow/test_workflow.cwl')
    assert r.status_code == 200

def test_set_missing_workflow(test_service, this_dir):
    job = test_service.create_job('test_set_missing_workflow')
    workflow_path = os.path.join(this_dir, 'does_not_exist')
    with pytest.raises(ce.FileNotFound):
        job.set_workflow(workflow_path)

def test_set_workflow_repeatedly(test_service, this_dir):
    job = test_service.create_job('test_set_workflow_repeatedly')
    job.set_workflow(os.path.join(this_dir, 'test_workflow.cwl'))
    job.set_workflow(os.path.join(this_dir, 'test_workflow2.cwl'))
    assert job._workflow_url == 'http://localhost:29593/files/input/test_set_workflow_repeatedly/test_workflow2.cwl'
    r = requests.get('http://localhost:29593/files/input/test_set_workflow_repeatedly/test_workflow2.cwl')
    assert r.status_code == 200

def test_add_input_file(test_service, this_dir):
    job = test_service.create_job('test_add_input_file')
    job.set_workflow(os.path.join(this_dir, 'test_workflow2.cwl'))
    job.add_input_file('input_file', os.path.join(this_dir, 'test_job.py'))

    assert 'input_file' in job._input_desc
    assert 'class' in job._input_desc['input_file']
    assert 'location' in job._input_desc['input_file']
    assert 'basename' in job._input_desc['input_file']
    assert job._input_desc['input_file']['basename'] == 'test_job.py'

    r = requests.get('http://localhost:29593/files/input/test_add_input_file/test_job.py')
    assert r.status_code == 200

def test_add_missing_input_file(test_service, this_dir):
    job = test_service.create_job('test_add_missing_input_file')
    job.set_workflow(os.path.join(this_dir, 'test_workflow2.cwl'))
    with pytest.raises(ce.FileNotFound):
        job.add_input_file('input_file', os.path.join(this_dir, 'does_not_exist'))


# TODO: check incorrect type and non-existent input, when implemented

def test_add_secondary_file(test_service, this_dir):
    job = test_service.create_job('test_add_secondary_file')
    job.set_workflow(os.path.join(this_dir, 'test_workflow.cwl'))
    job.add_input_file('input_file', os.path.join(this_dir, 'test_job.py'))
    job.add_secondary_file('input_file', os.path.join(this_dir, 'test_workflow2.cwl'))
    job.add_secondary_file('input_file', os.path.join(this_dir, 'test_workflow3.cwl'))

    assert 'secondaryFiles' in job._input_desc['input_file']
    sf = job._input_desc['input_file']['secondaryFiles']
    assert isinstance(sf, list)
    assert sf[0]['class'] == 'File'
    assert sf[0]['location'] == 'http://localhost:29593/files/input/test_add_secondary_file/test_workflow2.cwl'
    assert sf[0]['basename'] == 'test_workflow2.cwl'
    assert sf[1]['class'] == 'File'
    assert sf[1]['location'] == 'http://localhost:29593/files/input/test_add_secondary_file/test_workflow3.cwl'
    assert sf[1]['basename'] == 'test_workflow3.cwl'

def test_add_orphan_secondary_file(test_service, this_dir):
    job = test_service.create_job('test_add_orphan_secondary_file')
    job.set_workflow(os.path.join(this_dir, 'test_workflow.cwl'))
    with pytest.raises(ce.NoPrimaryFile):
        job.add_secondary_file('input_file', os.path.join(this_dir, 'test_workflow2.cwl'))

def test_add_missing_secondary_file(test_service, this_dir):
    job = test_service.create_job('test_add_missing_secondary_file')
    job.set_workflow(os.path.join(this_dir, 'test_workflow.cwl'))
    job.add_input_file('input_file', os.path.join(this_dir, 'test_job.py'))
    with pytest.raises(ce.FileNotFound):
        job.add_secondary_file('input_file', os.path.join(this_dir, 'does_not_exist'))

def test_set_input(test_service, this_dir):
    job = test_service.create_job('test_set_input')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 10)
    assert job._input_desc['time'] == 10

def test_run_job(test_service, this_dir):
    job = test_service.create_job('test_run_job')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 10)
    job_id = job.run()
    r = requests.get('http://localhost:29593/jobs/' + job_id)
    assert r.status_code == 200

def test_run_invalid_job(test_service):
    job = test_service.create_job('test_run_invalid_job')
    with pytest.raises(ce.InvalidJob):
        _ = job.run()

def test_no_rerun_job(test_service, this_dir):
    job = test_service.create_job('test_no_rerun_job')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 2)
    job_id = job.run()
    with pytest.raises(ce.JobAlreadyExists):
        _ = job.run()

def test_job_is_running(test_service, this_dir):
    job = test_service.create_job('test_job_is_running')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 1)
    _ = job.run()
    assert job.is_running()
    while job.is_running():
        time.sleep(0.1)
    assert not job.is_running()

def test_job_state(test_service, this_dir):
    job = test_service.create_job('test_job_state')
    assert job.state is None
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 1)
    job_id = job.run()
    assert job.state == 'Waiting'
    while job.state == 'Waiting':
        time.sleep(0.1)
    while job.state == 'Running':
        time.sleep(0.1)
    assert job.state == 'Success'

def test_job_state_error(test_service):
    job = test_service.create_job('test_job_state_error')
    job.id = 'nonexistent'
    with pytest.raises(ce.JobNotFound):
        _ = job.state

def test_cancel_job(test_service, this_dir):
    job = test_service.create_job('test_cancel_job')
    job.set_workflow(os.path.join(this_dir, 'test_workflow3.cwl'))
    job.set_input('time', 10)
    job_id = job.run()
    job.cancel()
    time.sleep(3)
    assert job.state == 'Cancelled'

def test_job_outputs(test_service, this_dir, tmpdir):
    job = test_service.create_job('test_job_outputs')
    job.set_workflow(os.path.join(this_dir, 'test_workflow2.cwl'))
    job.add_input_file('input_file', os.path.join(this_dir, 'test_workflow2.cwl'))
    job.run()
    while job.is_running():
        time.sleep(0.1)
    assert job.state == 'Success'

    outfile = str(tmpdir.join('counts.txt'))
    job.outputs['counts'].save_as(outfile)
    assert 'test_workflow2.cwl' in open(outfile).read()

def test_job_log(test_service, this_dir):
    job = create_test_job(test_service, this_dir, 'test_job_log')
    assert 'Final process status is success' in job.log

def test_nonexistent_job_log(test_service, this_dir):
    job = create_test_job(test_service, this_dir, 'test_nonexistent_job_log')
    job.id = 'nonexistent'
    with pytest.raises(ce.JobNotFound):
        _ = job.log
