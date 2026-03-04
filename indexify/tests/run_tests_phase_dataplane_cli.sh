#!/bin/bash
# Phase 1: Self-contained dataplane_cli tests (no external dataplane needed).
# Each test spawns and manages its own dataplane processes.

set -euo pipefail

if [[ -z "$TENSORLAKE_API_URL" ]]; then
    echo "Please set TENSORLAKE_API_URL environment variable to specify"\
    "Tensorlake SDK you are testing." \
    "Example: 'export TENSORLAKE_API_URL=http://localhost:8900'" 1>&2
    exit 1
fi

tests_exit_code=0

run_test_suite() {
  local test_files=$1
  local test_suite_name=$2

  for test_file in $test_files; do
    echo ""
    echo "========================================"
    echo "Running: $test_file ($test_suite_name)"
    echo "========================================"
    python3 $test_file
    local test_file_exit_code=$?
    if [ $test_file_exit_code -ne 0 ]; then
      echo "FAILED: $test_file ($test_suite_name)" | tee -a $summary_file
    else
      echo "PASSED: $test_file ($test_suite_name)"
    fi
    tests_exit_code=$((tests_exit_code || test_file_exit_code))
  done
}

# cd to the script's directory.
cd "$(dirname "$0")"

summary_file=".run_tests_summary.txt"
rm -f $summary_file

echo ""
echo "############################################"
echo "# Phase 1: dataplane_cli tests             #"
echo "############################################"
dataplane_cli_tests=$(find ./dataplane_cli -name 'test_*.py' -not -name 'test_network_partition.py' | sort)
run_test_suite "$dataplane_cli_tests" "Dataplane CLI"

echo ""
echo "========================================"
echo "SUMMARY"
echo "========================================"
if [ $tests_exit_code -eq 0 ]; then
  echo "All tests passed!" | tee -a $summary_file
else
  echo "One or more tests failed. Please check output log for details." | tee -a $summary_file
fi
cat $summary_file
exit $tests_exit_code
