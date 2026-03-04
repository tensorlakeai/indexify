#!/bin/bash
# Runs all 3 dataplane test phases sequentially.
# For CI, each phase runs as a separate parallel job.
# This script is for local development convenience.

set -euo pipefail

SCRIPT_DIR="$(dirname "$0")"

bash "$SCRIPT_DIR/run_tests_phase_dataplane_cli.sh"
bash "$SCRIPT_DIR/run_tests_phase_executor_features.sh"

if [[ -z "${TENSORLAKE_SDK_SKIP_TESTS:-}" ]]; then
  bash "$SCRIPT_DIR/run_tests_phase_tensorlake_sdk.sh"
fi
