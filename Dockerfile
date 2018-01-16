# our base image
FROM alpine:3.7

# Set one or more individual labels
LABEL maintainer="mushkevych@gmail.com"
LABEL synergy_scheduler.docker.version="0.1"

# add python
RUN apk add --no-cache python3 && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
    if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
    rm -r /root/.cache

# add bash & shell utils
RUN apk add --no-cache bash gawk sed grep bc coreutils

COPY . /opt/synergy_scheduler
WORKDIR /opt/synergy_scheduler/

RUN /opt/synergy_scheduler/launch.py install

# run the application
ENTRYPOINT ["python", "/opt/synergy_scheduler/launch.py", "start", "Scheduler"]

# tell the port number the container should expose
EXPOSE 5000
