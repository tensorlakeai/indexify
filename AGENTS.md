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
cargo run -p indexify-dataplane -- --config /tmp/docker-config.yaml

# 4. Create and test a sandbox
curl -X POST http://localhost:8900/v1/namespaces/test-ns/applications/test-app/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.11-slim", "timeout_secs": 300}'
```

---

## 3. After Writing Code

**IMPORTANT**: Always run these commands after making changes:

```bash
# Lint (required - must pass before committing)
just lint-rust

# Run tests
cargo test --workspace
```

If linting fails, fix the issues. Use `just lint-fix` to auto-fix some problems.

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
