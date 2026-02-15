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

# The server stores blobs at {cwd}/indexify_storage by default.
# SERVER_WORK_DIR should point to the directory where indexify-server was started.
SERVER_WORK_DIR="${SERVER_WORK_DIR:-$(cd ../../ && pwd)}"
BLOB_STORE_DIR="${SERVER_WORK_DIR}/indexify_storage"

# Generate config with bind mount so containers can access the blob store.
# The mount uses the same path inside and outside the container so that
# file:// URIs from the server resolve correctly.
CONFIG_FILE="/tmp/dataplane_gvisor_config.yaml"
cat > "$CONFIG_FILE" << EOF
env: local
server_addr: "http://localhost:8901"
driver:
  type: docker
  runtime: runsc
  binds:
    - "${BLOB_STORE_DIR}:${BLOB_STORE_DIR}"
monitoring:
  port: 7100
default_function_image: "indexify-test-function:latest"
EOF

echo "Generated dataplane config:"
cat "$CONFIG_FILE"
echo ""

DATAPLANE_LOG="/tmp/indexify-dataplane.log"
RUST_LOG=info $DATAPLANE_BIN --config "$CONFIG_FILE" > "$DATAPLANE_LOG" 2>&1 &
DATAPLANE_PID=$!
echo "Started dataplane (gVisor) PID: $DATAPLANE_PID  (logs: $DATAPLANE_LOG)"
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
