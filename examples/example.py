import cerise_client as cc
import time

if cc.service_exists('cerise-mdstudio-das5-myuser'):
    srv = cc.get_service('cerise-mdstudio-das5-myuser', 29593)
else:
    srv = cc.create_service(
            'cerise-mdstudio-das5-myuser', 29593,
            'cerise-mdstudio-das5',
            'user_name',
            password='password'
            )

srv.start()

job = srv.create_job('example_job')
job.set_workflow('./gromacs_energy.cwl')
job.add_input_file('protein_pdb', './CYP19A1vs.pdb')
job.add_input_file('ligand_pdb', './BHC89.pdb')
job.add_input_file('ligand_top', './BHC89.itp')
job.set_input('force_field', 'amberSB99')
job.set_input('sim_time', 0.0001)

job.run()

persisted_srv = srv.to_dict()
persisted_job = job.id

srv.stop()


# store the dict somewhere in persistent storage
# stop client software, shut down computer, etc.

# start computer and client software again
# get dict from persistent storage

srv = cc.service_from_dict(persisted_srv)
job = srv.get_job_by_id(persisted_job)

while job.is_running():
    time.sleep(10)

if job.state == 'Success':
    job.outputs['trajectory'].save_as('CYP19A1vs_BHC53.trr')
    job.outputs['gromitout'].save_as('gromitout.log')
    job.outputs['gromiterr'].save_as('gromiterr.log')
    job.outputs['gromacslog'].save_as('gromacs.log')
else:
    print('There was an error: ' + job.state)
    job.log.save_as('job.log')
    print('Job log saved as job.log')

job.delete()

srv.destroy()

