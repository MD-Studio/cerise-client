cwlVersion: v1.0
class: Workflow

inputs:
  input_file: File

outputs:
  counts:
    type: File
    outputSource: wc/output

steps:
  wc:
    run: cerise/test/wc.cwl
    in:
      file: input_file
    out: [output]
