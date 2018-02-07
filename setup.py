from setuptools import setup
setup(
        name = 'cerise_client',
        packages = ['cerise_client'],
        version = 'develop',
        description = 'Client library for the Cerise CWL job running service',
        author = 'Lourens Veen',
        author_email = 'l.veen@esciencecenter.nl',
        url = 'https://github.com/MD-Studio/cerise-client',
        download_url = 'https://github.com/MD-Studio/cerise-client/archive/develop.tar.gz',
        license = 'Apache License 2.0',
        python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
        install_requires=[
                'docker>=2.3.0,<3',
                'defusedxml',
                'future',
                'requests'
            ],
        keywords = ['CWL', 'workflow', 'HPC'],
        classifiers = [
            'Development Status :: 4 - Beta',
            'License :: OSI Approved :: Apache Software License',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Topic :: System :: Distributed Computing'],
        )
