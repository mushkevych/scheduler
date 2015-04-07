#!/bin/bash

ENVIRONMENT="%ENVIRONMENT%"

SCRIPTS_NAMES=("mongodb_workers_schema")

for SINGLE_SCRIPT in ${SCRIPTS_NAMES[@]}; do
   mongo ${SINGLE_SCRIPT}.js
done

if [ -n "${ENVIRONMENT}" ]; then
    # if ENVIRONMENT was defined

    for SINGLE_SCRIPT in ${SCRIPTS_NAMES[@]}; do
        if [ -a ${SINGLE_SCRIPT}_${ENVIRONMENT}.js ]; then
            # if the file exist
            echo "Executing ${ENVIRONMENT}-specific script: ${SINGLE_SCRIPT}_${ENVIRONMENT}.js"
            mongo ${SINGLE_SCRIPT}_${ENVIRONMENT}.js
        fi
    done

fi
