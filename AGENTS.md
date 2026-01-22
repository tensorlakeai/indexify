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
┌─────────────────┐         gRPC          ┌─────────────────┐
│                 │◄────────────────────► │                 │
│  indexify-server│                       │    dataplane    │
│   (port 8901)   │                       │                 │
└─────────────────┘                       └────────┬────────┘
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
driver:
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
