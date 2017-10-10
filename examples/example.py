import cerise_client.service as cc
import time

srv = cc.require_managed_service(
            'my-cerise-service', 29593,
            'mdstudio/cerise:develop')
cc.start_managed_service(srv)

job = srv.create_job('example_job')
job.set_workflow('./example_workflow.cwl')
job.add_input_file('input_file', __file__)
job.run()

# Give the service a chance to stage the job
while job.status == 'Waiting':
    time.sleep(1)

persisted_srv = srv.to_dict()
persisted_job = job.id

cc.stop_managed_service(srv)


# store the dict somewhere in persistent storage
# stop client software, shut down computer, etc.

# start computer and client software again
# get dict from persistent storage

srv = cc.service_from_dict(persisted_srv)
job = srv.get_job_by_id(persisted_job)

while job.is_running():
    time.sleep(10)

if job.state == 'Success':
    job.outputs['counts'].save_as('counts.txt')
else:
    print('There was an error: ' + job.state)
    job.log.save_as('job.log')
    print('Job log saved as job.log')

srv.destroy_job(job)

cc.destroy_managed_service(srv)

