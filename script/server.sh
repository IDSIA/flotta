#!/bin/bash

while [ $# -gt 0 ] ; do
    case $1 in
        -t | --target) W="$2" ;;
    esac
    shift
done

case $W in
    worker)
        cd ferdelance
        celery \
            --broker ${CELERY_BROKER_URL} \
            --result-backend ${CELERY_BACKEND_URL} \
            --app ferdelance.worker.celery \
            worker \
            --loglevel INFO
        ;;
    server)
        uvicorn \
            ferdelance.server.api:api \
            --host 0.0.0.0 \
            --port 1456 \
            --log-level critical
        ;;
esac