#!/bin/bash

# Stop all containers
containers=`docker ps -a -q`
if [ -n "${containers}" ] ; then
        docker stop ${containers}
fi

docker system prune -a --volumes
