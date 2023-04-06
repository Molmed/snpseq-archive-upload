#! /bin/sh

/archive-upload/.venv/bin/archive-upload-ws --config=/archive-upload/config/ --debug &
nginx &

wait
