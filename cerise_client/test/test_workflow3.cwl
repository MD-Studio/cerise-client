cwlVersion: v1.0
class: Workflow

inputs:
  time: int

outputs: []

steps:
  step1:
    run: cerise/test/sleep.cwl
    in:
      delay: time
    out: []
