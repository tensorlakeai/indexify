#!/bin/bash

if [[ -z "$INDEXIFY_URL" ]]; then
    echo "Please set INDEXIFY_URL environment variable to specify"\
    "Indexify Server you are testing." \
    "Example: 'export INDEXIFY_URL=http://localhost:8900'" 1>&2
    exit 1
fi

run_test_suite() {
  local test_files=$1
  local test_suite_name=$2

  # Run each test file one by one sequentially. Returns non zero status
  # code if any of the test commands return non zero status code. Doesn't
  # stop if a test command fails.
  #
  # Run the tests from Indexify pyenv so Executor and SDK client are in the same pyenv.
  echo "$test_files" | xargs -L1 poetry run python
  test_suite_exit_code=$?
  if [ $test_suite_exit_code -ne 0 ]; then
    echo "One or more tests failed in ${test_suite_name} test suite. Please check output log for details."
  fi
  return $test_suite_exit_code
}

# cd to the script's directory.
cd "$(dirname "$0")"

# Indexify tests.
indexify_test_files=$(find . -name 'test_*.py')
# Tensorlke SDK tests verify user visible functionality end-to-end.
tensorlake_sdk_test_files=$(find ../../tensorlake/tests/tensorlake -name 'test_*.py')

run_test_suite "$indexify_test_files" "Indexify"
tests_exit_code=$?
run_test_suite "$tensorlake_sdk_test_files" "Tensorlake SDK"
tests_exit_code=$((tests_exit_code || $?))

if [ $tests_exit_code -eq 0 ]; then
  echo "All tests passed!"
else
  echo "One or more tests failed. Please check output log for details."
fi

exit $tests_exit_code