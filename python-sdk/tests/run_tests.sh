#!/bin/bash

docker-compose up -d

serverReady=false
while [ "$serverReady" != true ]; do
    output=$(curl --silent --fail http://localhost:8900/extractors | jq '.extractors | length' 2>/dev/null)
    if [[ $? -eq 0 && "$output" -ge 2 ]]; then
        echo "Server is ready with 2 extractors."
        serverReady=true
    else
        printf 'Waiting for server to start with 2 extractors...\n'
        sleep 5
    fi
done


pytest integration_test.py::TestIntegrationTest
pytest_exit_status=$?

docker-compose down

exit $pytest_exit_status
