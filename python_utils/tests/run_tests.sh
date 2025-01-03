#!/bin/bash

if [[ -z "$INDEXIFY_URL" ]]; then
    echo "Please set INDEXIFY_URL environment variable to specify"\
    "Indexify Server you are testing." \
    "Example: 'export INDEXIFY_URL=http://localhost:8900'" 1>&2
    exit 1
fi

# cd to the script's directory.
cd "$(dirname "$0")"
rm -rf dist
poetry install
poetry build

# Run each test file one by one sequentially. Returns non zero status
# code if any of the test commands return non zero status code. Doesn't
# stop if a test command fails.
find src/tests -name 'test_*.py' | xargs -L1 poetry run python
TESTS_EXIT_CODE=$?

if [ $TESTS_EXIT_CODE -eq 0 ]; then
  echo "All tests passed!"
else
  echo "One or more tests failed. Please check output log for details."
fi

exit $TESTS_EXIT_CODE
