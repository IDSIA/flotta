#!/bin/bash

# config
VIRTUAL_ENV="Spearhead_env"

# TODO: include whole environment in application installer instead of searching for a local python and venv

# fake virtual env activation
PATH="$VIRTUAL_ENV/bin:$PATH"

# python executable
PYTHON_VERSION=`${PYTHON} --version`
PYTHON_OUT=$?

if [ ${PYTHON_OUT} -ne 0 ]; then
    echo "Python executable not found"
    exit 1
fi

echo "Python version: ${PYTHON_VERSION}"

EXIT_CODE=1

while [ ${EXIT_CODE} -ne 0 ]; do
    echo 'Launching ferdelance client'

    # main loop
    ${PYTHON} ferdelance/client "$@"

    EXIT_CODE=$?

    if [ ${EXIT_CODE} -eq 1 ]; then
        # install new environment and relaunch
        rm ${PYTHON_ENV}
        unzip `cat .update`
    fi

done
