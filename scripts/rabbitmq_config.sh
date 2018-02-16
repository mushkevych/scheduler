#!/bin/bash

wget http://127.0.0.1:15672/cli/rabbitmqadmin -O /tmp/rabbitmqadmin

python3 /tmp/rabbitmqadmin declare vhost name=unit_test
for i in "/" "unit_test"; do
  python3 /tmp/rabbitmqadmin declare exchange --vhost=${i} name=ex_utils type=direct
  python3 /tmp/rabbitmqadmin declare exchange --vhost=${i} name=ex_managed_worker type=direct
  python3 /tmp/rabbitmqadmin declare exchange --vhost=${i} name=ex_freerun_worker type=direct
done
