# AGENTS.md - AI Agent Instructions for Indexify

This document provides instructions for AI agents working on the Indexify codebase.

## Project Overview

Indexify is a compute engine for building data platforms in Python. It consists of:

- **server** (`crates/server/`) - Central orchestration server managing workflows, executors, and state
- **dataplane** (`crates/dataplane/`) - Manages function execution via containers (Docker or fork-exec)
- **container-daemon** (`crates/container-daemon/`) - PID1 process manager inside function executor containers
- **proto-api** (`crates/proto-api/`) - Protocol buffer definitions compiled to Rust code

## Architecture

```
┌─────────────────┐         gRPC          ┌─────────────────────────────────┐
│                 │◄────────────────────► │           dataplane             │
│  indexify-server│                       │  ┌───────────────────────────┐  │
│   (port 8901)   │                       │  │  Sandbox Proxy (port 9000)│  │
└─────────────────┘                       │  └─────────────┬─────────────┘  │
                                          └────────────────┼────────────────┘
                                                           │
                                                  ┌────────┴────────┐
                                                  │                 │
                                            ┌─────▼─────┐    ┌──────▼─────┐
                                            │  Docker   │    │  ForkExec  │
                                            │  Driver   │    │   Driver   │
                                            └─────┬─────┘    └──────┬─────┘
                                                  │                 │
                                            ┌─────▼─────────────────▼─────┐
                                            │   container-daemon (PID1)   │
                                            │   - gRPC server (port 9500) │
                                            │   - HTTP server (port 9501) │
                                            └─────────────────────────────┘
```

---

## 1. Adding New Dependencies

### Rust Dependencies

**IMPORTANT**: All Rust dependencies MUST be added to the workspace root `Cargo.toml`, not individual crate `Cargo.toml` files.

1. Add to `[workspace.dependencies]` in root `Cargo.toml`:
   ```toml
   new-crate = { version = "1.0.0", features = ["feature1"] }
   ```

2. Reference in crate's `Cargo.toml`:
   ```toml
   [dependencies]
   new-crate.workspace = true
   ```

### Proto Dependencies

When modifying `.proto` files:

1. Edit files in `/proto/` directory
2. Rust code is auto-generated via `crates/proto-api/build.rs`
3. **Regenerate Python stubs**: `cd indexify && make build_proto`

---

## 2. Running Tests

**IMPORTANT**: Always test in debug mode, not release mode. Debug builds are faster and provide better error messages.

```bash
# Unit tests
cargo test --workspace

# Server integration tests
cargo test -p indexify-server

# Docker integration tests (requires Docker, cross-compiles daemon on macOS)
RUN_DOCKER_TESTS=1 cargo test -p indexify-dataplane --test docker_integration_test
```

### End-to-End Testing with Docker

```bash
# 1. Build with Linux daemon (required on macOS)
RUN_DOCKER_TESTS=1 cargo build -p indexify-dataplane

# 2. Start server
cargo run -p indexify-server

# 3. Start dataplane with Docker driver
cat > /tmp/docker-config.yaml << EOF
env: local
server_addr: "http://localhost:8901"
function_driver:
  type: docker
sandbox_driver:
  type: docker
EOF

# IMPORTANT: On macOS, always use RUN_DOCKER_TESTS=1 when running with Docker driver
# Otherwise cargo may recompile without the Linux daemon binary
RUN_DOCKER_TESTS=1 cargo run -p indexify-dataplane -- --config /tmp/docker-config.yaml

# 4. Create and test a sandbox
curl -X POST http://localhost:8900/v1/namespaces/test-ns/applications/test-app/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.11-slim", "timeout_secs": 300}'
```

---

## 3. After Writing Code

**IMPORTANT**: Always run these commands after making changes:

```bash
# Format code (required - CI will fail without this)
just fmt

# Lint (required - must pass before committing)
just lint-rust

# Run tests
cargo test --workspace
```

If linting fails, fix the issues. Note: `just fmt` requires `poetry` for Python formatting - if not installed, run `rustup run nightly cargo fmt` directly for Rust formatting.

**IMPORTANT**: Do not leave code for future use. Only implement what is needed now. Remove unused constants, functions, and commented-out code.

---

## 4. Key Files

| File | Purpose |
|------|---------|
| `Cargo.toml` (root) | Workspace config, ALL dependencies |
| `proto/executor_api.proto` | Server <-> Dataplane API |
| `proto/container_daemon.proto` | Daemon API |
| `crates/dataplane/src/driver/` | Process drivers (Docker, ForkExec) |
| `crates/server/src/routes/` | HTTP endpoints |

---

## 5. Ports

| Service | Port |
|---------|------|
| Server HTTP | 8900 |
| Server gRPC | 8901 |
| Dataplane Sandbox Proxy | 9000 |
| Container Daemon gRPC | 9500 |
| Container Daemon HTTP | 9501 |

---

## 6. Sandbox HTTP API

When a sandbox is running, its HTTP API is exposed on a dynamically assigned port (returned in `sandbox_http_address`). The API provides:

### Process Management
```bash
# Start a process
curl -X POST "http://<sandbox_addr>/api/v1/processes" \
  -H "Content-Type: application/json" \
  -d '{"command": "python", "args": ["-c", "print(1+1)"], "working_dir": "/app"}'

# Get process status
curl "http://<sandbox_addr>/api/v1/processes/{pid}"

# Get process output
curl "http://<sandbox_addr>/api/v1/processes/{pid}/output"

# Kill a process
curl -X DELETE "http://<sandbox_addr>/api/v1/processes/{pid}"
```

### File Operations
```bash
# Read a file
curl "http://<sandbox_addr>/api/v1/files?path=/app/file.txt"

# Write a file
curl -X PUT "http://<sandbox_addr>/api/v1/files?path=/app/file.txt" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "file contents"

# List directory
curl "http://<sandbox_addr>/api/v1/files/list?path=/app"
```

### Health Check
```bash
curl "http://<sandbox_addr>/api/v1/health"
# Returns: {"healthy": true}
```

---

## 7. Running Dataplane in a Container

When the dataplane runs inside a container, it needs access to host resources to properly schedule function containers. By default, containers only see their cgroup limits, not actual host resources.

### Host Resource Detection

The dataplane automatically detects host resources when `/host/proc` is mounted. This follows the same pattern used by Kubernetes kubelet/cAdvisor.

### Required Volume Mounts

```bash
docker run \
  -v /proc:/host/proc:ro \
  -v /var/run/docker.sock:/var/run/docker.sock \
  indexify-dataplane
```

| Mount | Purpose |
|-------|---------|
| `/proc:/host/proc:ro` | Host CPU and memory info (`/proc/cpuinfo`, `/proc/meminfo`) |
| `/var/run/docker.sock` | Required for Docker driver to spawn containers |

### How It Works

1. Dataplane checks if `/host/proc` exists at startup
2. If present, reads `/host/proc/meminfo` and `/host/proc/cpuinfo` for host resources
3. If not present, falls back to `sysinfo` crate (sees container limits)

### Docker Compose Example

```yaml
services:
  dataplane:
    image: indexify-dataplane
    volumes:
      - /proc:/host/proc:ro
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - INDEXIFY_SERVER_ADDR=http://server:8901
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: indexify-dataplane
spec:
  template:
    spec:
      containers:
      - name: dataplane
        image: indexify-dataplane
        volumeMounts:
        - name: host-proc
          mountPath: /host/proc
          readOnly: true
        - name: docker-sock
          mountPath: /var/run/docker.sock
      volumes:
      - name: host-proc
        hostPath:
          path: /proc
      - name: docker-sock
        hostPath:
          path: /var/run/docker.sock
```

### Verifying Host Detection

Check dataplane logs at startup for:
```
INFO Probed host resources from mounted /host/proc cpu_count=16 memory_bytes=68719476736 source="host_mount"
```

If you see `source="sysinfo"` instead, the host mounts are not configured correctly.

---

## 8. Sandbox Proxy

The dataplane includes a built-in HTTP proxy server that routes requests to sandbox containers. This allows external access to any port running inside sandbox containers.

### URL Format

```text
http://<dataplane>:9000/{sandbox_id}/{port}/{path...}
```

For example, to access port 8080 on sandbox `sb-abc123`:
```bash
curl http://localhost:9000/sb-abc123/8080/api/users
```

### Proxy Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Proxy health check |
| `GET /_internal/sandboxes` | List all running sandboxes |
| `/{sandbox_id}/{port}/*` | Proxy to container port |

### Configuration

The proxy can be configured in the dataplane config file:

```yaml
proxy:
  enabled: true      # Enable/disable proxy (default: true)
  port: 9000         # Listen port (default: 9000)
  listen_addr: "0.0.0.0"  # Listen address (default: 0.0.0.0)
```

### How It Works

1. Requests arrive at `/{sandbox_id}/{port}/{path...}`
2. Proxy looks up the sandbox's container IP from `FunctionContainerManager`
3. Forwards the request to `http://{container_ip}:{port}/{remaining_path}`
4. Returns the response from the container

### Key Files

| File | Purpose |
|------|---------|
| `crates/dataplane/src/proxy.rs` | Proxy server implementation |
| `crates/dataplane/src/config.rs` | `ProxyConfig` definition |
| `crates/dataplane/src/function_container_manager.rs` | `get_sandbox_address()`, `list_sandboxes()` |
