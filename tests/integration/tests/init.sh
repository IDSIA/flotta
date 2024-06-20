#!/bin/bash

echo Server:     ${SERVER}
echo Repository: ${REPOSITORY_HOST}

python -m venv /venv

PATH="/venv/bin:${PATH}"

pip install --upgrade pip

pip install --index-url http://${REPOSITORY_HOST}/simple/ --trusted-host ${REPOSITORY_HOST} flotta

python /tests/health_check.py

EXIT_CODE=$?

if [[ $EXIT_CODE -ne 0 ]]; then

    echo "Health check failed: could not create environment"

    exit -1

fi

TESTS_PASSED=0
TESTS_FAILED=0

for TEST in /tests/test_*.py
do

    echo "================================================================================"
    echo Testing file: $TEST
    echo "================================================================================"

    python $TEST

    TEST_CODE=$?

    if [[ $TEST_CODE -ne 0 ]]; then
        echo "TEST FAILED!"
        TESTS_FAILED=$((TESTS_FAILED+1))
    else
        echo "TESTS_PASSED!"
        TESTS_PASSED=$((TESTS_PASSED+1))
    fi

    echo ""

done

echo "================================================================================"
echo
echo "--------------------------------------------------------------------------------"
echo "PASSED: " $TESTS_PASSED
echo "FAILED: " $TESTS_FAILED
echo "--------------------------------------------------------------------------------"

python /tests/results.py
