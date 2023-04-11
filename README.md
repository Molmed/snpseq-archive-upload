[![Build Status](https://travis-ci.org/Molmed/snpseq-archive-upload.svg?branch=master)](https://travis-ci.org/Molmed/snpseq-archive-upload)

Archive Upload
=================

A Tornado REST service that creates archives and uploads them with IBM's TSM backup/archive client dsmc. Requires that TSM is installed separately.

Trying it out
-------------

    # create venv
    virtualenv -p python2.7 venv/   

    # activate venv
    source venv/bin/activate

    # install dependencies
    pip install -e . -r ./requirements/dev

    # start the dev server
    python server.py --debug --port=8333 --configroot='./config'

And then you can find a simple api documentation with:

    curl http://localhost:8333/api

To run the tests:

    nosetests tests/

To run the app in production mode:

    # install dependencies
    pip install -U -r requirements/prod .

    # start the server
    archive-upload-ws --config=config/ --port=8181 --debug
    
Docker container
----------------

For testing purposes, you can also build a [Docker](https://docker.com) container using the `Dockerfile` and configs in
the `docker/` folder:

    # build Docker container
    docker build -t archive-upload:latest -f docker/Dockerfile .

This will build a Docker container that runs a [nginx](https://nginx.com) proxy server which will listen to connections
on ports `8181` and `8182` and forward traffic to the `archive-upload` service running internally. API calls to port 
`8181` are done as described in the api documentation mentioned above. API calls to port `8182` emulate how calls to the
service running on Uppmax is done (i.e., going through a gateway). The first path element for these calls should be 
`upload/` (see example below).

    # start Docker container
    docker run -d -p 127.0.0.1:8181:8181 -p 127.0.0.1:8182:8182 archive-upload:latest

    # interact with archive-upload service on port 8181
    curl 127.0.0.1:8181/api/1.0/version
        # {"version": "1.0.4"}

    # interact with archive-upload service on port 8182
    curl 127.0.0.1:8182/upload/api/1.0/version
        # {"version": "1.0.4"}

In addition, the `archive-upload` service in the container is running with the TSM mocking enabled. In the container, 
there are two folders that can be used for testing, `test_1_upload` and `test_2_upload`.

    # create archive dir
    curl -X POST -d '{}' 127.0.0.1:8181/api/1.0/create_dir/test_1_upload
        # {"state": "done", "service_version": "1.0.4"}

    # compress achive dir
    curl -X POST 127.0.0.1:8181/api/1.0/compress_archive/test_1_upload_archive
        # {"state": "done", "service_version": "1.0.4"}

    # generate checksums
    curl -X POST 127.0.0.1:8181/api/1.0/gen_checksums/test_1_upload_archive
        # {
        #   "state": "started",
        #   "link": "http://127.0.0.1:8181/api/1.0/status/1",
        #   "job_id": 1,
        #   "service_version": "1.0.4"
        # }

    # check status
    curl http://127.0.0.1:8181/api/1.0/status/1
        # {"state": "done"}

    # upload archive dir
    curl -X POST 127.0.0.1:8181/api/1.0/upload/test_1_upload_archive
        # {
        #   "dsmc_log_dir": "/tmp/archive-upload//dsmc_test_1_upload_archive",
        #   "archive_path": "/data/mm-xart002/runfolders/test_1_upload_archive",
        #   "state": "started",
        #   "archive_host": "b2dbb6de3079",
        #   "link": "http://127.0.0.1:8181/api/1.0/status/999",
        #   "job_id": 999,
        #   "service_version": "1.0.4",
        #   "message": "tsm_mock_enabled",
        #   "archive_description": "61a1551e-0ef6-41f1-911d-2998c5c478dd"
        # }

    # check status
    curl 127.0.0.1:8181/api/1.0/status/999
        # {"state": "done"}
