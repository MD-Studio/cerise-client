import os
import pytest
import requests
import sys

import cerise_client.output_file as cf


@pytest.fixture()
def this_dir(request):
    return os.path.dirname(__file__)

@pytest.fixture()
def test_file(request, test_service, this_dir):
    requests.put(
            'http://localhost:29593/files/input/test_workflow.cwl',
            data=open(os.path.join(this_dir, 'test_workflow.cwl')))
    return 'http://localhost:29593/files/input/test_workflow.cwl'

@pytest.fixture()
def test_output_file(request, test_file):
    return cf.OutputFile(test_file)

def test_create_output_file_object(test_file):
    output_file = cf.OutputFile(test_file)
    assert output_file._uri == test_file

def test_save_output_file(test_output_file, tmpdir, this_dir):
    local_file = str(tmpdir.join('saved_workflow.cwl'))
    test_output_file.save_as(local_file)
    assert os.path.exists(local_file)

    orig_file = os.path.join(this_dir, 'test_workflow.cwl')
    with open(orig_file, 'rb') as f2:
        ref_contents = f2.read()
        with open(local_file, 'rb') as f1:
            test_contents = f1.read()
            assert test_contents == ref_contents

def test_output_file_text(test_output_file):
    assert test_output_file.text == (
        "cwlVersion: v1.0\n"
        "class: Workflow\n"
        "\n"
        "inputs: []\n"
        "outputs:\n"
        "  count: File\n"
        "\n"
        "steps:\n"
        "  step1:\n"
        "    run: cerise/test/hostname.cwl\n"
        "    in: []\n"
        "    out: [output]\n")

def test_output_file_content(test_output_file):
    assert test_output_file.content == bytes(
        "cwlVersion: v1.0\n"
        "class: Workflow\n"
        "\n"
        "inputs: []\n"
        "outputs:\n"
        "  count: File\n"
        "\n"
        "steps:\n"
        "  step1:\n"
        "    run: cerise/test/hostname.cwl\n"
        "    in: []\n"
        "    out: [output]\n", 'utf-8')
