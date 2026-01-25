# Indexify Dataplane

The Indexify Dataplane is a service that runs on worker nodes to manage function executor and sandbox containers. It communicates with the Indexify Server via gRPC to receive desired state and report container status.

## Architecture

```
                    ┌─────────────────────┐
                    │   Indexify Server   │
                    │   (Control Plane)   │
                    └──────────┬──────────┘
                               │ gRPC (8901)
                               │
                    ┌──────────▼──────────┐
                    │  Indexify Dataplane │
                    │                     │
                    │  ┌───────────────┐  │
                    │  │ TLS Proxy     │◄─┼──── External Traffic (9443)
                    │  │ (SNI routing) │  │
                    │  └───────┬───────┘  │
                    │          │          │
                    │  ┌───────▼───────┐  │
                    │  │ Container     │  │
                    │  │ Manager       │  │
                    │  └───────┬───────┘  │
                    │          │          │
                    └──────────┼──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
       ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
       │  Container  │  │  Container  │  │  Container  │
       │  (Sandbox)  │  │  (Function) │  │  (Sandbox)  │
       │ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │
       │ │ Daemon  │ │  │ │ Daemon  │ │  │ │ Daemon  │ │
       │ └─────────┘ │  │ └─────────┘ │  │ └─────────┘ │
       └─────────────┘  └─────────────┘  └─────────────┘
```

### Components

- **Service**: Main service loop handling heartbeats and desired state streaming
- **Container Manager**: Creates, monitors, and terminates containers based on desired state
- **TLS Proxy**: SNI-based routing for external access to sandbox containers
- **Process Drivers**: Docker or ForkExec backends for container/process management
- **Daemon Client**: gRPC client for communicating with the in-container daemon
- **Network Rules**: iptables-based network policy enforcement for sandboxes

## Configuration

### Local Development (No Config Required)

For local development, simply run the dataplane without any configuration:

```bash
indexify-dataplane
```

The dataplane will:
- Connect to `http://localhost:8901` (default server address)
- Auto-generate self-signed TLS certificates for the TLS proxy
- Log the resolved configuration on startup via structured logging

### Custom Configuration

For custom settings, pass a YAML config file:

```bash
indexify-dataplane --config /etc/indexify/dataplane.yaml
```

### Production Configuration

Production deployments require TLS proxy configuration with proper certificates:

```yaml
env: production
server_addr: "http://indexify.example.com:8901"
driver:
  type: docker
tls_proxy:
  cert_path: "/etc/certs/sandboxes.pem"
  key_path: "/etc/certs/sandboxes-key.pem"
  advertise_address: "worker-1.example.com:9443"
  proxy_domain: "sandboxes.example.com"
```

**Note:** In production (`env` != `local`), `cert_path` and `key_path` are required.

### Full Configuration Reference

```yaml
# Environment name (affects logging format)
# "local" = human-readable logs, anything else = JSON structured logs
env: production

# Unique executor identifier (auto-generated UUID if not specified)
executor_id: worker-node-1

# Indexify Server gRPC address
server_addr: "http://indexify.example.com:8901"

# Telemetry configuration (optional)
telemetry:
  # Enable OTLP metrics export
  enable_metrics: true
  # OpenTelemetry collector endpoint
  endpoint: "http://otel-collector:4317"
  # Tracing exporter: "otlp" or "stdout"
  tracing_exporter: otlp
  # Metrics export interval in seconds
  metrics_interval: 10
  # Instance ID for metrics attribution
  instance_id: "dataplane-prod-worker-1"

# Process driver configuration
driver:
  # Options: "fork_exec" (default) or "docker"
  type: docker
  # Docker daemon address (optional, uses default socket if not specified)
  address: "unix:///var/run/docker.sock"

# State file for persisting container state across restarts (optional)
state_file: "/var/lib/indexify/dataplane-state.json"

# TLS proxy for external access to sandboxes
# Required in production (cert_path and key_path must be provided)
# Auto-generated in local mode if not provided
tls_proxy:
  port: 9443
  listen_addr: "0.0.0.0"
  # Address advertised to server (used by sandbox-proxy for routing)
  advertise_address: "worker-1.example.com:9443"
  # TLS certificate (should be wildcard cert for *.sandboxes.example.com)
  cert_path: "/etc/certs/sandboxes.pem"
  key_path: "/etc/certs/sandboxes-key.pem"
  # Domain suffix for sandbox routing (default: 127.0.0.1.nip.io for local dev)
  proxy_domain: "sandboxes.example.com"
```

## Production Deployment

### Prerequisites

1. **Docker**: Required for the Docker driver (recommended for production)
2. **Network Access**:
   - Outbound: gRPC to Indexify Server (default: 8901)
   - Inbound: TLS proxy port (default: 9443) if using sandbox external access
3. **Certificates** (optional, only if using TLS proxy):
   - Wildcard certificate for TLS proxy (e.g., `*.sandboxes.example.com`)

### Docker Deployment

```dockerfile
FROM rust:1.75-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p indexify-dataplane

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates iptables && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/indexify-dataplane /usr/local/bin/
ENTRYPOINT ["indexify-dataplane"]
```

```bash
docker run -d \
  --name indexify-dataplane \
  --privileged \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v /proc:/host/proc:ro \
  -v /etc/indexify:/etc/indexify:ro \
  -v /var/lib/indexify:/var/lib/indexify \
  -p 9443:9443 \
  indexify-dataplane:latest \
  --config /etc/indexify/dataplane.yaml
```

**Important flags:**
- `--privileged`: Required for iptables network policy enforcement
- `-v /var/run/docker.sock`: Docker socket for container management
- `-v /proc:/host/proc:ro`: Enables accurate host resource detection from within container

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: indexify-dataplane
spec:
  selector:
    matchLabels:
      app: indexify-dataplane
  template:
    metadata:
      labels:
        app: indexify-dataplane
    spec:
      hostNetwork: true
      containers:
      - name: dataplane
        image: indexify-dataplane:latest
        args: ["--config", "/etc/indexify/dataplane.yaml"]
        securityContext:
          privileged: true
        volumeMounts:
        - name: docker-sock
          mountPath: /var/run/docker.sock
        - name: host-proc
          mountPath: /host/proc
          readOnly: true
        - name: config
          mountPath: /etc/indexify
        - name: state
          mountPath: /var/lib/indexify
        ports:
        - containerPort: 9443
          hostPort: 9443
      volumes:
      - name: docker-sock
        hostPath:
          path: /var/run/docker.sock
      - name: host-proc
        hostPath:
          path: /proc
      - name: config
        configMap:
          name: dataplane-config
      - name: state
        hostPath:
          path: /var/lib/indexify
```

### Resource Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 0.5 cores | 2 cores |
| Memory | 256 MB | 1 GB |
| Disk | 1 GB | 10 GB (for state and logs) |

The dataplane itself is lightweight; resources are primarily consumed by managed containers.

## Metrics

Metrics are exported via OTLP when `telemetry.enable_metrics: true`. All metrics are prefixed with `indexify.dataplane.`.

### Counters

| Metric | Labels | Description |
|--------|--------|-------------|
| `containers.started` | `container_type` | Number of containers started |
| `containers.terminated` | `container_type`, `reason` | Number of containers terminated |
| `desired_state.received` | - | Desired state messages from server |
| `desired_state.function_executors` | - | Total function executors in desired state |
| `desired_state.allocations` | - | Total allocations in desired state |
| `heartbeat.success` | - | Successful heartbeats |
| `heartbeat.failures` | - | Failed heartbeats |
| `stream.disconnections` | `reason` | Stream disconnections |

### Gauges (Observable)

| Metric | Unit | Description |
|--------|------|-------------|
| `containers.running.functions` | count | Running function containers |
| `containers.running.sandboxes` | count | Running sandbox containers |
| `containers.pending.functions` | count | Pending function containers |
| `containers.pending.sandboxes` | count | Pending sandbox containers |
| `resources.free_cpu_percent` | % | Available CPU percentage |
| `resources.free_memory_bytes` | bytes | Available memory |
| `resources.free_disk_bytes` | bytes | Available disk space |

### Resource Attributes

All metrics include these attributes:
- `service.namespace`: `indexify`
- `service.name`: `indexify-dataplane`
- `service.version`: Package version
- `indexify.instance.id`: Instance identifier
- `indexify.executor.id`: Executor identifier

### Example Prometheus Queries

```promql
# Container startup rate
rate(indexify_dataplane_containers_started_total[5m])

# Running containers by type
indexify_dataplane_containers_running_sandboxes
indexify_dataplane_containers_running_functions

# Heartbeat failure rate
rate(indexify_dataplane_heartbeat_failures_total[5m]) / rate(indexify_dataplane_heartbeat_success_total[5m])

# Memory utilization
1 - (indexify_dataplane_resources_free_memory_bytes / total_memory_bytes)
```

## Logs

### Log Format

- **Local environment** (`env: local`): Human-readable compact format
- **Production** (`env: production`): JSON structured logs

### Log Levels

Control via `RUST_LOG` environment variable:

```bash
# Default: INFO level
RUST_LOG=info indexify-dataplane --config config.yaml

# Debug level for troubleshooting
RUST_LOG=debug indexify-dataplane --config config.yaml

# Specific module debug
RUST_LOG=indexify_dataplane::function_container_manager=debug indexify-dataplane --config config.yaml
```

### Key Log Events

| Event | Level | Description |
|-------|-------|-------------|
| `image_pull_started` | INFO | Docker image pull started |
| `image_pull_completed` | INFO | Docker image pull completed (includes `duration_ms`) |
| `image_pull_failed` | ERROR | Docker image pull failed (includes `duration_ms`, `error`) |
| `container_creating` | INFO | Container creation started |
| `container_started` | INFO | Container successfully started |
| `container_startup_failed` | ERROR | Container failed to start |
| `container_stopping` | INFO | Container stop initiated |
| `container_killing` | INFO | Container force kill after grace period |
| `container_terminated` | INFO | Container terminated |

### Structured Log Fields

Container lifecycle events include:
- `container_id`: Unique container identifier (same as sandbox ID for sandboxes)
- `namespace`: Namespace name
- `app`: Application name
- `fn_name`: Function name
- `app_version`: Application version
- `container_type`: `function` or `sandbox`
- `startup_duration_ms`: Time to start container
- `run_duration_ms`: Container runtime duration

Note: The Docker container name is `indexify-{container_id}`, making it easy to correlate logs with Docker commands.

### Example Log Output (JSON)

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "INFO",
  "message": "Container started with daemon",
  "container_id": "sb-abc123",
  "namespace": "default",
  "app": "my-app",
  "fn_name": "process",
  "app_version": "1.0",
  "container_type": "sandbox",
  "startup_duration_ms": 2345,
  "event": "container_started"
}
```

The Docker container name would be `indexify-sb-abc123` for this example.

## Local Development

### Overview

For local development, the dataplane uses [nip.io](https://nip.io) for automatic DNS resolution. This eliminates the need for `/etc/hosts` entries or custom DNS setup.

**How it works:**
- Default `proxy_domain`: `127.0.0.1.nip.io`
- Any hostname like `{sandbox_id}.127.0.0.1.nip.io` automatically resolves to `127.0.0.1`
- The dataplane auto-generates self-signed TLS certificates for `*.127.0.0.1.nip.io`

### Prerequisites

1. **Docker Desktop** or **OrbStack** (recommended for macOS - provides direct container IP access)
2. **Indexify Server** running locally

### Quick Start

```bash
# Terminal 1: Start Indexify Server
cargo run -p indexify-server

# Terminal 2: Start Dataplane (default config works out of the box)
cargo run -p indexify-dataplane
```

The dataplane will:
- Connect to the server at `http://localhost:8901`
- Start TLS proxy on port `9443`
- Auto-generate self-signed certs for `*.127.0.0.1.nip.io`
- Use `127.0.0.1.nip.io` as the proxy domain

### Accessing Sandboxes Locally

When you create a sandbox, the API returns a `sandbox_url` like:
```
https://k9f8o1jh95d076uth02d.127.0.0.1.nip.io:9443
```

**Important: Accepting Self-Signed Certificates**

Before making API calls from JavaScript/browser, you must accept the self-signed certificate:

1. Navigate directly to the sandbox URL in your browser
2. Accept the security warning ("Proceed to site" / "Accept the risk")
3. After accepting, JavaScript `fetch()` requests will work

```bash
# Test from command line (use -k to skip cert verification)
curl -k https://k9f8o1jh95d076uth02d.127.0.0.1.nip.io:9443/api/v1/processes
```

### Sandbox ID Format

Sandbox IDs use a DNS-safe alphabet (alphanumeric only, no underscores or special characters) since they're used in hostnames. Example: `k9f8o1jh95d076uth02d`

### Custom TLS Certificates (Optional)

For custom domains or to avoid self-signed cert warnings, generate your own certificates:

```bash
# Generate wildcard cert for your domain
openssl req -x509 -newkey rsa:4096 -keyout sandbox-key.pem -out sandbox-cert.pem \
  -days 365 -nodes -subj "/CN=*.sandboxes.local" \
  -addext "subjectAltName=DNS:*.sandboxes.local"
```

Run with custom certs:

```bash
cargo run -p indexify-dataplane -- --config - <<EOF
env: local
server_addr: "http://localhost:8901"
driver:
  type: docker
tls_proxy:
  port: 9443
  cert_path: ./sandbox-cert.pem
  key_path: ./sandbox-key.pem
  proxy_domain: sandboxes.local
EOF
```

### Creating a Test Sandbox

```bash
# Create namespace
curl -X PUT http://localhost:8900/v1/namespaces/default

# Create application (multipart form)
curl -X POST http://localhost:8900/v1/namespaces/default/applications \
  -F 'application={"name":"test-app","functions":[]}'

# Create sandbox
curl -X POST http://localhost:8900/v1/namespaces/default/applications/test-app/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.11-slim"}'
```

### Testing TLS Proxy Routing

```bash
# Get sandbox ID from response above, then test routing
SANDBOX_ID=<sandbox_id>

# With nip.io (automatic DNS resolution, no --resolve needed)
curl -k "https://${SANDBOX_ID}.127.0.0.1.nip.io:9443/api/v1/processes"

# Route to custom port (e.g., 8080) - port prefix is optional
curl -k "https://8080-${SANDBOX_ID}.127.0.0.1.nip.io:9443/"

# With custom domain (requires --resolve or /etc/hosts)
curl -k --resolve "${SANDBOX_ID}.sandboxes.local:9443:127.0.0.1" \
  "https://${SANDBOX_ID}.sandboxes.local:9443/health"
```

### Running Tests

```bash
# Unit tests
cargo test -p indexify-dataplane

# With logging
RUST_LOG=debug cargo test -p indexify-dataplane -- --nocapture
```

### Debugging Tips

1. **View container logs**:
   ```bash
   docker logs indexify-<container_id>
   ```

2. **Check dataplane state file**:
   ```bash
   cat ./dataplane-state.json | jq
   ```

3. **List managed containers**:
   ```bash
   docker ps --filter "label=indexify.managed=true"
   ```

4. **Inspect network rules** (Linux only):
   ```bash
   sudo iptables -L DOCKER-USER -n -v
   sudo iptables -L INDEXIFY-SB-<container_prefix> -n -v
   ```

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Heartbeat failures | Server unreachable | Check `server_addr` and network connectivity |
| Container startup timeout | Slow image pull | Pre-pull images or increase timeout |
| TLS handshake failed | Certificate mismatch | Verify cert covers `*.{proxy_domain}` |
| Network rules not applied | Missing privileges | Run with `--privileged` or `NET_ADMIN` capability |
| Resource detection wrong | Running in container | Mount `/proc:/host/proc:ro` |
| ERR_CERT_AUTHORITY_INVALID | Self-signed certificate | Navigate to sandbox URL directly and accept cert first |
| Browser fetch fails | Cert not accepted | Accept self-signed cert in browser before making JS requests |

### Health Checks

```bash
# Check if dataplane is sending heartbeats (server logs)
grep "heartbeat" /var/log/indexify-server.log

# Check container manager state
curl http://localhost:9500/debug/containers  # (if debug endpoint enabled)

# Verify Docker connectivity
docker ps
```
