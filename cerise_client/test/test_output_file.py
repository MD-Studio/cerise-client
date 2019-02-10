import os
import pytest
import requests
import sys

from cerise_client import OutputFile


def test_create_output_file_object(test_file):
    output_file = OutputFile(test_file)
    assert output_file._uri == test_file

def test_save_output_file(test_output_file, tmpdir, this_dir):
    local_file = tmpdir / 'saved_workflow.cwl'
    test_output_file.save_as(str(local_file))
    assert local_file.exists()

    orig_file = this_dir / 'test_workflow.cwl'
    with orig_file.open('rb') as f2:
        ref_contents = f2.read()
        with local_file.open('rb') as f1:
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
