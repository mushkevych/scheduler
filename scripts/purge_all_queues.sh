#!/bin/bash

# author: Bohdan Mushkevych
# script clears all the queues in the Rabbit MQ

# List of queues to purge
QUEUES="all"

cd ..
for queue in ${QUEUES}
do
  .ve/bin/python -m scripts.purge_queue ${queue}
done

exit 0
