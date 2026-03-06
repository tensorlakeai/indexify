#!/bin/bash
# Phase 2: executor + features tests.
# Starts a shared background dataplane, runs tests, then stops the dataplane.

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

# --- Start background dataplane ---
echo ""
echo "############################################"
echo "# Starting background dataplane            #"
echo "############################################"

DATAPLANE_BIN="${DATAPLANE_BIN:-$(which indexify-dataplane 2>/dev/null || echo "")}"
DATAPLANE_PID=""

DATAPLANE_CONFIG="$(dirname "$0")/dataplane_config.yaml"
if [ -n "$DATAPLANE_BIN" ] && [ -x "$DATAPLANE_BIN" ]; then
  $DATAPLANE_BIN --config "$DATAPLANE_CONFIG" > /tmp/indexify-dataplane.log 2>&1 &
  DATAPLANE_PID=$!
  echo "Started background dataplane with PID: $DATAPLANE_PID"
  sleep 5
else
  echo "WARNING: indexify-dataplane binary not found at $DATAPLANE_BIN"
  echo "Executor and features tests may hang without a running dataplane."
fi

# --- Phase 2: executor + features tests ---
echo ""
echo "############################################"
echo "# Phase 2: executor + features tests       #"
echo "############################################"
# test_http_handlers.py tests old Python CLI executor HTTP monitoring endpoints
# which are not part of the dataplane.
executor_tests=$(find ./executor -name 'test_*.py' -not -name 'test_http_handlers.py' | sort)
features_tests=$(find ./features -name 'test_*.py' | sort)
run_test_suite "$executor_tests $features_tests" "Indexify"

# --- Stop the background dataplane ---
if [ -n "$DATAPLANE_PID" ]; then
  echo "Stopping background dataplane (PID: $DATAPLANE_PID)"
  # The dataplane is terminated intentionally; ignore non-zero exit codes from
  # kill/wait (e.g. 143 when reaped after SIGTERM) so they don't mask test
  # results.
  kill "$DATAPLANE_PID" 2>/dev/null || true
  wait "$DATAPLANE_PID" 2>/dev/null || true
  pkill -f function-executor 2>/dev/null || true
fi

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
