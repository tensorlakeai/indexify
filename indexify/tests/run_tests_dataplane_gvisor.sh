#!/bin/bash
set -e

if [[ -z "$TENSORLAKE_API_URL" ]]; then
    echo "Please set TENSORLAKE_API_URL" >&2
    exit 1
fi

cd "$(dirname "$0")"

# Start dataplane with Docker + gVisor config
DATAPLANE_BIN="${DATAPLANE_BIN:-$(which indexify-dataplane 2>/dev/null || echo "")}"
if [ -z "$DATAPLANE_BIN" ] || [ ! -x "$DATAPLANE_BIN" ]; then
    echo "ERROR: indexify-dataplane binary not found" >&2
    exit 1
fi

$DATAPLANE_BIN --config dataplane_gvisor_config.yaml &
DATAPLANE_PID=$!
echo "Started dataplane (gVisor) PID: $DATAPLANE_PID"
sleep 10  # Wait for connection + container readiness

tests_exit_code=0
for test_file in $(find ./dataplane_gvisor -name 'test_*.py' | sort); do
    echo ""
    echo "========================================"
    echo "Running: $test_file"
    echo "========================================"
    python3 $test_file
    test_exit=$?
    if [ $test_exit -ne 0 ]; then
        echo "FAILED: $test_file"
    else
        echo "PASSED: $test_file"
    fi
    tests_exit_code=$((tests_exit_code || test_exit))
done

kill $DATAPLANE_PID 2>/dev/null
wait $DATAPLANE_PID 2>/dev/null
exit $tests_exit_code
