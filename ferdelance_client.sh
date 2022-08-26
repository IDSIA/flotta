#!/bin/bash

# config
PYTHON_ENV="Spearhead_env"
REQUIREMENTS="requirements.txt"

# TODO: include whole environment in application installer instead of searching for a local python and venv

# python executable
PYTHON="/usr/bin/python"
PYTHON_VERSION=`${PYTHON} --version`
PYTHON_OUT=$?

if [ ${PYTHON_OUT} -ne 0 ]; then
    echo "Python executable not found"
    exit 1
fi

echo "Python version: ${PYTHON_VERSION}"

if [ -d ${PYTHON_ENV} ]; then
    # activate existing environment
    echo "Activating virtual environment ${PYTHON_ENV}"
    source ./${PYTHON_ENV}/bin/activate
else
    # create new environment
    echo "Environment folder not ${PYTHON_ENV} found. "
    
    ${PYTHON} -m venv ${PYTHON_ENV}

    source ./${PYTHON_ENV}/bin/activate

    pip install -r ${REQUIREMENTS}
fi

EXIT_CODE=1

while [ ${EXIT_CODE} -ne 0 ]; do
    # main loop
    ${PYTHON} ferdelance/client.py
    
    EXIT_CODE=$?

    if [ ${EXIT_CODE} -eq 2 ]; then
        # install new requirements and relaunch
        pip install -r ${REQUIREMENTS}
    fi

done
