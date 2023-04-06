FROM python:2.7-slim

COPY . /archive-upload
WORKDIR /archive-upload

RUN \
  apt-get update && \
  apt-get install -y git nano nginx && \
  cp docker/nginx.conf /etc/nginx/nginx.conf && \
  pip install virtualenv && \
  virtualenv .venv && \
  .venv/bin/pip install --upgrade pip && \
  .venv/bin/pip install -U -e . -r requirements/dev . && \
  sed -i -re 's/(tsm_mock_enabled:).*/\1 True/' config/app.config && \ 
  mkdir -p /tmp/archive-upload

RUN \
  .venv/bin/nosetests tests/

RUN \
  mkdir -p /data/mm-xart002/runfolders/test_1_upload && \
  mkdir -p /data/mm-xart002/runfolders/test_2_upload && \
  cd /data/mm-xart002/runfolders && \
  dd if=/dev/urandom of=test_1_upload/test_1_data_a count=4000 bs=1024 && \
  dd if=/dev/urandom of=test_1_upload/test_1_data_b count=4000 bs=1024 && \
  dd if=/dev/urandom of=test_1_upload/test_1_data_c count=4000 bs=1024 && \
  dd if=/dev/urandom of=test_2_upload/test_2_data_d count=4000 bs=1024 && \
  dd if=/dev/urandom of=test_2_upload/test_2_data_e count=4000 bs=1024 && \
  dd if=/dev/urandom of=test_2_upload/test_2_data_f count=4000 bs=1024 && \
  cd /archive-upload

CMD [ "docker/start.sh" ]
