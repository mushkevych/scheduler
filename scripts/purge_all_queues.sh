#!/bin/bash

#Created on 2011-04-11
#author: Bohdan Mushkevych
# script clears all the queues in the Rabbit MQ

# List of queues to purge
QUEUES="queue_alert_daily
queue_raw_data
queue_vertical_site_hourly
queue_vertical_site_monthly
queue_vertical_site_yearly
queue_vertical_site_daily"

cd ..
for queue in $QUEUES
do
  .ve/bin/python -m scripts.purge_queue $queue
done

exit 0
