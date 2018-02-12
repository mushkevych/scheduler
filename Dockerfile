# docker commands:
# docker build . --tag mushkevych/synergy-scheduler:2.0
# docker run --detach --name syn-scheduler --publish 5000:5000 mushkevych/synergy-scheduler:2.0
# docker -D run -it mushkevych/synergy-scheduler:2.0 /bin/bash

# list of alpine packages: https://pkgs.alpinelinux.org/packages
FROM alpine:3.7

LABEL maintainer="mushkevych@gmail.com"
LABEL synergy_scheduler.docker.version="0.1"

# OS-level required packages:
#   * dumb-init: a proper init system for containers, to reap zombie children
#   * linux-headers: commonly needed, and an unusual package name from Alpine.
#   * build-base: include basic development packages (gcc, g++, etc)
#   * bash gawk sed grep bc coreutils: bash & shell utils
#   * ca-certificates: for SSL verification during Pip and easy_install
ENV PACKAGES="\
  dumb-init \
  linux-headers \
  build-base \
  bash gawk sed grep bc curl coreutils \
  ca-certificates \
"
RUN apk add --no-cache ${PACKAGES}

# add python3 and python3 dev
RUN apk add --no-cache python3 python3-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
#    pip3 install --upgrade pip setuptools && \
    if [[ ! -e /usr/bin/pip ]]; then ln -s pip3 /usr/bin/pip; fi && \
    if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
    if [ -d /root/.cache ]; then rm -r /root/.cache; fi

COPY . /opt/synergy_scheduler
WORKDIR /opt/synergy_scheduler/

RUN mkdir -p /var/log/synergy-scheduler/

RUN /opt/synergy_scheduler/launch.py install

# start Synergy Scheduler daemon
#ENTRYPOINT ["python", "/opt/synergy_scheduler/launch.py", "start", "Scheduler"]

# port number the container should expose
EXPOSE 5000
