import docker

def clean_up_service(srv_name):
    dc = docker.from_env()
    try:
        test_srv = dc.containers.get(srv_name)
        test_srv.stop()
        test_srv.remove()
    except docker.errors.NotFound:
        pass

# Clean up any mess left over from previous failed tests.
def clean_up():
    clean_up_service('cerise_client_test_service')
    clean_up_service('cerise_client_test_service2')


