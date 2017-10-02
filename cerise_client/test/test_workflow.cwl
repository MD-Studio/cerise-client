cwlVersion: v1.0
class: Workflow

inputs: []
outputs:
  count: File

steps:
  step1:
    run: cerise/test/hostname.cwl
    in: []
    out: [output]
