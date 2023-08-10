#!/bin/bash

# check python executable
PYTHON="python"

PYTHON_VERSION=`${PYTHON} --version`
PYTHON_OUT=$?

if [ ${PYTHON_OUT} -ne 0 ]; then
    echo "Python executable not found"
    exit 1
fi

echo "Python version: ${PYTHON_VERSION}"

run_client () {
    # EXIT_CODE=0

    # while [ ${EXIT_CODE} -eq 0 ]; do
    #     echo "Launching ferdelance client"

    #     # main loop
    ${PYTHON} -m ferdelance.client "$@"

    #     EXIT_CODE=$?

    #     echo "exit_code=${EXIT_CODE}"

    #     if [ ${EXIT_CODE} -eq 1 ]; then
    #         # install new environment and relaunch
    #         echo "updating packages"

    #         pip install --index-url http://${REPOSITORY_HOST}/simple/ --trusted-host ${REPOSITORY_HOST} ferdelance

    #         echo "relaunch with exit_code=0"
    #         EXIT_CODE=0
    #     fi

    # done
}

run_server () {
    ${PYTHON} -m ferdelance.server "$@"
}

POSITIONAL_ARGS=()

while [ $# -gt 0 ] ; do
    case $1 in
        -t|--target)
            W="$2"
            shift
            shift
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done

set -- "${POSITIONAL_ARGS[@]}"

case $W in
    client)
        run_client "$@"
        ;;
    server)
        run_server "$@"
        ;;
esac
