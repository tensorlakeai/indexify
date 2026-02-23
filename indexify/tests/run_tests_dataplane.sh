#!/bin/bash

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

# --- Phase 1: Self-contained dataplane_cli tests (no external dataplane needed) ---
echo ""
echo "############################################"
echo "# Phase 1: dataplane_cli tests             #"
echo "############################################"
dataplane_cli_tests=$(find ./dataplane_cli -name 'test_*.py' -not -name 'test_network_partition.py' | sort)
run_test_suite "$dataplane_cli_tests" "Dataplane CLI"

# --- Start background dataplane for Phase 2 + Phase 3 ---
echo ""
echo "############################################"
echo "# Starting background dataplane            #"
echo "############################################"

DATAPLANE_BIN="${DATAPLANE_BIN:-$(which indexify-dataplane 2>/dev/null || echo "")}"
DATAPLANE_PID=""

DATAPLANE_CONFIG="$(dirname "$0")/dataplane_config.yaml"
if [ -n "$DATAPLANE_BIN" ] && [ -x "$DATAPLANE_BIN" ]; then
  $DATAPLANE_BIN --config "$DATAPLANE_CONFIG" &
  DATAPLANE_PID=$!
  echo "Started background dataplane with PID: $DATAPLANE_PID"
  # Wait for it to connect to the server.
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

# --- Phase 3: Tensorlake SDK tests (same dataplane keeps running) ---
if [[ -z "${TENSORLAKE_SDK_SKIP_TESTS}" ]]; then
  echo ""
  echo "############################################"
  echo "# Phase 3: Tensorlake SDK tests            #"
  echo "############################################"
  tensorlake_sdk_test_files=$(find ../../tensorlake/tests/applications -name 'test_*.py' | sort)
  run_test_suite "$tensorlake_sdk_test_files" "Tensorlake SDK"
fi

# Stop the background dataplane.
if [ -n "$DATAPLANE_PID" ]; then
  echo "Stopping background dataplane (PID: $DATAPLANE_PID)"
  kill $DATAPLANE_PID 2>/dev/null
  wait $DATAPLANE_PID 2>/dev/null
  # Also kill any orphaned function executors.
  pkill -f function-executor 2>/dev/null
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
