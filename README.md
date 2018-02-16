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
    pip install -U -r requirements/prod .

Try running it:

     archive-upload-ws --config=config/ --port=8181 --debug

And then you can find a simple api documentation with:

    curl http://localhost:8181/api
