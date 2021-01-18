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

    curl http://localhost:8181/api

To run the tests:

    nosetests tests/

To run the app in production mode:

    # install dependencies
    pip install -U -r requirements/prod .

    # start the server
    archive-upload-ws --config=config/ --port=8181 --debug
    
