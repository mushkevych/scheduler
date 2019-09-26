#!/bin/bash

RABBITMQ_HOST=${RABBITMQ_HOST:-"syn-rabbitmq"}

attempts=0
until wget http://${RABBITMQ_HOST}:15672/cli/rabbitmqadmin -O /tmp/rabbitmqadmin || [[ ${attempts} -eq 5 ]]; do
   sleep 10
   attempts=$((attempts + 1))
done

for i in "/" "unit_test"; do
    python3 /tmp/rabbitmqadmin --host ${RABBITMQ_HOST} declare vhost name=${i}
    python3 /tmp/rabbitmqadmin --host ${RABBITMQ_HOST} declare exchange --vhost=${i} name=ex_utils type=direct
    python3 /tmp/rabbitmqadmin --host ${RABBITMQ_HOST} declare exchange --vhost=${i} name=ex_managed_worker type=direct
    python3 /tmp/rabbitmqadmin --host ${RABBITMQ_HOST} declare exchange --vhost=${i} name=ex_freerun_worker type=direct
done
