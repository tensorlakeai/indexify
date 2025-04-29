#!/bin/bash

if [[ -z "$INDEXIFY_URL" ]]; then
    echo "Please set INDEXIFY_URL environment variable to specify"\
    "Indexify Server you are testing." \
    "Example: 'export INDEXIFY_URL=http://localhost:8900'" 1>&2
    exit 1
fi

tests_exit_code=0

run_test_suite() {
  local test_files=$1
  local test_suite_name=$2
  local test_suite_exit_code=0

  # Run each test file one by one sequentially. Set $tests_exit_code to non zero
  # value if any of the test commands return non zero status code. Don't
  # stop if a test command fails.
  for test_file in $test_files; do
    echo "Running $test_file for $test_suite_name test suite"
    poetry run python $test_file
    local test_file_exit_code=$?
    if [ $test_file_exit_code -ne 0 ]; then
      echo "One or more tests failed in $test_file for $test_suite_name test suite." | tee -a $summary_file
    fi
    tests_exit_code=$((tests_exit_code || test_file_exit_code))
  done
}

# cd to the script's directory.
cd "$(dirname "$0")"

summary_file=".run_tests_summary.txt"
rm -f $summary_file

# Indexify tests.
indexify_test_files=$(find . -name 'test_*.py')
# Tensorlke SDK tests verify user visible functionality end-to-end.
tensorlake_sdk_test_files=$(find ../../tensorlake/tests/tensorlake -name 'test_*.py')

run_test_suite "$indexify_test_files" "Indexify"
run_test_suite "$tensorlake_sdk_test_files" "Tensorlake SDK"

if [ $tests_exit_code -eq 0 ]; then
  echo "All tests passed!" >> $summary_file
else
  echo "One or more tests failed. Please check output log for details." >> $summary_file
fi

cat $summary_file
exit $tests_exit_code