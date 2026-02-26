#!/bin/bash

if [[ -z "$TENSORLAKE_API_URL" ]]; then
    echo "Please set TENSORLAKE_API_URL environment variable to specify"\
    "Tensorlake SDK you are testing." \
    "Example: 'export TENSORLAKE_API_URL=http://localhost:8900'" 1>&2
    exit 1
fi

# Optional --phase flag: dataplane-cli | executor-features | sdk | all (default).
# Used by CI to run each phase in a separate parallel job.
RUN_PHASE="all"
while [[ $# -gt 0 ]]; do
  case $1 in
    --phase)
      RUN_PHASE="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

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

# Runs all test files in parallel, collecting output per-file and printing it
# after each process finishes so logs stay readable.
run_test_suite_parallel() {
  local test_files=$1
  local test_suite_name=$2
  local pids=()
  local log_files=()
  local file_list=()

  for test_file in $test_files; do
    local log_file
    log_file=$(mktemp)
    echo "Starting: $test_file ($test_suite_name)"
    python3 "$test_file" > "$log_file" 2>&1 &
    pids+=("$!")
    log_files+=("$log_file")
    file_list+=("$test_file")
  done

  for i in "${!pids[@]}"; do
    wait "${pids[$i]}"
    local test_file_exit_code=$?
    local test_file="${file_list[$i]}"
    local log_file="${log_files[$i]}"

    echo ""
    echo "========================================"
    echo "Output: $test_file ($test_suite_name)"
    echo "========================================"
    cat "$log_file"
    rm -f "$log_file"

    if [ $test_file_exit_code -ne 0 ]; then
      echo "FAILED: $test_file ($test_suite_name)" | tee -a "$summary_file"
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

if [[ "$RUN_PHASE" == "all" || "$RUN_PHASE" == "dataplane-cli" ]]; then
  # --- Phase 1: Self-contained dataplane_cli tests (no external dataplane needed) ---
  # Each test manages its own dataplane instance (unique ports via find_free_port),
  # so they are safe to run in parallel.
  echo ""
  echo "############################################"
  echo "# Phase 1: dataplane_cli tests             #"
  echo "############################################"
  dataplane_cli_tests=$(find ./dataplane_cli -name 'test_*.py' -not -name 'test_network_partition.py' | sort)
  run_test_suite_parallel "$dataplane_cli_tests" "Dataplane CLI"
fi

if [[ "$RUN_PHASE" == "all" || "$RUN_PHASE" == "executor-features" || "$RUN_PHASE" == "sdk" ]]; then
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
fi

if [[ "$RUN_PHASE" == "all" || "$RUN_PHASE" == "executor-features" ]]; then
  # --- Phase 2: executor + features tests ---
  # Each test file deploys its own uniquely-named application (keyed on __file__),
  # so concurrent runs don't interfere with each other's tasks or executors.
  echo ""
  echo "############################################"
  echo "# Phase 2: executor + features tests       #"
  echo "############################################"
  # test_http_handlers.py tests old Python CLI executor HTTP monitoring endpoints
  # which are not part of the dataplane.
  executor_tests=$(find ./executor -name 'test_*.py' -not -name 'test_http_handlers.py' | sort)
  features_tests=$(find ./features -name 'test_*.py' | sort)
  run_test_suite_parallel "$executor_tests $features_tests" "Indexify"
fi

if [[ "$RUN_PHASE" == "all" || "$RUN_PHASE" == "sdk" ]]; then
  # --- Phase 3: Tensorlake SDK tests (same dataplane keeps running) ---
  if [[ -z "${TENSORLAKE_SDK_SKIP_TESTS}" ]]; then
    echo ""
    echo "############################################"
    echo "# Phase 3: Tensorlake SDK tests            #"
    echo "############################################"
    tensorlake_sdk_test_files=$(find ../../tensorlake/tests/applications -name 'test_*.py' | sort)
    run_test_suite_parallel "$tensorlake_sdk_test_files" "Tensorlake SDK"
  fi
fi

if [[ "$RUN_PHASE" == "all" || "$RUN_PHASE" == "executor-features" || "$RUN_PHASE" == "sdk" ]]; then
  # Stop the background dataplane.
  if [ -n "$DATAPLANE_PID" ]; then
    echo "Stopping background dataplane (PID: $DATAPLANE_PID)"
    kill $DATAPLANE_PID 2>/dev/null
    wait $DATAPLANE_PID 2>/dev/null
    # Also kill any orphaned function executors.
    pkill -f function-executor 2>/dev/null
  fi
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
