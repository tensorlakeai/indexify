#!/bin/bash
#
# Network partition acceptance tests.
# These tests manage their own dataplanes and proxy infrastructure.
# Run as a separate CI job in parallel with run_tests_dataplane.sh.
#
# Prerequisites:
#   - Server running with DATAPLANE_FUNCTIONS_ENABLED=true
#   - TENSORLAKE_API_URL set (e.g. http://localhost:8900)
#   - DATAPLANE_BIN set to the dataplane binary path
#   - Python venv activated (function-executor must be on PATH)

set -euo pipefail

if [[ -z "$TENSORLAKE_API_URL" ]]; then
    echo "Please set TENSORLAKE_API_URL environment variable." \
    "Example: 'export TENSORLAKE_API_URL=http://localhost:8900'" 1>&2
    exit 1
fi

cd "$(dirname "$0")"

echo "############################################"
echo "# Network partition acceptance tests       #"
echo "############################################"
echo ""

python3 ./dataplane_cli/test_network_partition.py
exit_code=$?

# Clean up any lingering processes from partition tests.
pkill -f function-executor 2>/dev/null || true
pkill -f indexify-dataplane 2>/dev/null || true

exit $exit_code
