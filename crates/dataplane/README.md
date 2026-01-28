# Indexify Dataplane

The Indexify Dataplane is a service that runs on worker nodes to manage function
executor and sandbox containers. It communicates with the Indexify Server via
gRPC to receive desired state and report container status.

## Architecture

```
                         ┌─────────────────────┐
                         │   Indexify Server   │
                         │   (Control Plane)   │
                         └──────────┬──────────┘
                                    │ gRPC (8901)
                                    │
  Sandbox-Proxy (8095)              │
  ─────────────────────┐            │
  Headers:             │            │
  • Tensorlake-Sandbox-Id           │
  • Tensorlake-Sandbox-Port         │
                       │            │
                       │ ┌──────────▼──────────┐
                       │ │  Indexify Dataplane │
                       │ │                     │
                       │ │  ┌───────────────┐  │
                       └─┼─►│  HTTP Proxy   │  │
                         │  └───────┬───────┘  │
                         │          │          │
                         │  ┌───────▼───────┐  │
                         │  │   Container   │  │
                         │  │    Manager    │  │
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
- **Container Manager**: Creates, monitors, and terminates containers based on
  desired state
- **HTTP Proxy**: Header-based routing for sandbox requests (receives
  `Tensorlake-Sandbox-Id` from sandbox-proxy)
- **Process Drivers**: Docker or ForkExec backends for container/process
  management
- **Daemon Client**: gRPC client for communicating with the in-container daemon
- **Network Rules**: iptables-based network policy enforcement for sandboxes

## Configuration

### Local Development (No Config Required)

For local development, simply run the dataplane without any configuration:

```bash
cargo run -p indexify-dataplane
```

The dataplane will:

- Connect to `http://localhost:8901` (default server address)
- Start HTTP proxy on port `8095`
- Log the resolved configuration on startup via structured logging

### Custom Configuration

For custom settings, pass a YAML config file:

```bash
cargo run -p indexify-dataplane -- --config /etc/indexify/dataplane.yaml
```

### Production Configuration

```yaml
env: production
server_addr: 'http://indexify.example.com:8901'
driver:
  type: docker
http_proxy:
  port: 8095
  listen_addr: '0.0.0.0'
  advertise_address: 'worker-1.example.com:8095'
```

### Full Configuration Reference

```yaml
# Environment name (affects logging format)
# "local" = human-readable logs, anything else = JSON structured logs
env: production

# Unique executor identifier (auto-generated UUID if not specified)
executor_id: worker-node-1

# Indexify Server gRPC address
server_addr: 'http://indexify.example.com:8901'

# TLS configuration for server gRPC connection (optional)
tls:
  enabled: false
  ca_cert_path: '/etc/certs/ca.pem'
  client_cert_path: '/etc/certs/client.pem'
  client_key_path: '/etc/certs/client-key.pem'
  domain_name: 'indexify.example.com'

# Telemetry configuration (optional)
telemetry:
  # OpenTelemetry tracing exporter: "otlp" or "stdout"
  tracing_exporter: otlp
  # OpenTelemetry collector endpoint
  endpoint: 'http://otel-collector:4317'
  # Instance ID for metrics attribution
  instance_id: 'dataplane-prod-worker-1'

# Process driver configuration
driver:
  # Options: "fork_exec" (default) or "docker"
  type: docker
  # Docker daemon address (optional, uses default socket if not specified)
  # address: "unix:///var/run/docker.sock"

# HTTP proxy for sandbox routing
# Receives requests from sandbox-proxy with Tensorlake-Sandbox-Id header
http_proxy:
  port: 8095
  listen_addr: '0.0.0.0'
  # Address advertised to server (sandbox-proxy uses this to forward requests)
  advertise_address: 'worker-1.example.com:8095'
```

## HTTP Proxy

The HTTP proxy receives requests from the sandbox-proxy service and routes them
to the appropriate container based on headers.

### Headers

| Header                    | Required | Description                                    |
| ------------------------- | -------- | ---------------------------------------------- |
| `Tensorlake-Sandbox-Id`   | Yes      | The sandbox container ID to route to           |
| `Tensorlake-Sandbox-Port` | No       | Container port (defaults to 9501 - daemon API) |

### Response Codes

| Code            | Meaning                                                    |
| --------------- | ---------------------------------------------------------- |
| 2xx/3xx/4xx/5xx | Forwarded from container                                   |
| 400             | Missing `Tensorlake-Sandbox-Id` header                     |
| 404             | Sandbox not found                                          |
| 503             | Sandbox exists but not running (includes state in message) |
| 502             | Failed to connect to container                             |

### Example Request Flow

```
sandbox-proxy                          dataplane
     │                                     │
     │  POST /api/v1/processes             │
     │  Host: abc123.sandbox.tensorlake.ai │
     │  Tensorlake-Sandbox-Id: abc123      │
     │  Tensorlake-Sandbox-Port: 9501      │
     │ ──────────────────────────────────► │
     │                                     │ Lookup container abc123
     │                                     │ Forward to container:9501
     │                                     │
     │  HTTP/1.1 200 OK                    │
     │  {"processes": [...]}               │
     │ ◄────────────────────────────────── │
```

## Production Deployment

### Prerequisites

1. **Docker**: Required for the Docker driver (recommended for production)
2. **Network Access**:
   - Outbound: gRPC to Indexify Server (default: 8901)
   - Inbound: HTTP proxy port (default: 8095) from sandbox-proxy

### Docker Deployment

```dockerfile
FROM rust:1.83-slim as builder
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
  -p 8095:8095 \
  indexify-dataplane:latest \
  --config /etc/indexify/dataplane.yaml
```

**Important flags:**

- `--privileged`: Required for iptables network policy enforcement
- `-v /var/run/docker.sock`: Docker socket for container management
- `-v /proc:/host/proc:ro`: Enables accurate host resource detection from within
  container

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
          args: ['--config', '/etc/indexify/dataplane.yaml']
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
            - containerPort: 8095
              hostPort: 8095
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

| Resource | Minimum   | Recommended                |
| -------- | --------- | -------------------------- |
| CPU      | 0.5 cores | 2 cores                    |
| Memory   | 256 MB    | 1 GB                       |
| Disk     | 1 GB      | 10 GB (for state and logs) |

The dataplane itself is lightweight; resources are primarily consumed by managed
containers.

## Metrics

Metrics are exported via OTLP when telemetry is configured. All metrics are
prefixed with `indexify.dataplane.`.

### Counters

| Metric                             | Labels                     | Description                               |
| ---------------------------------- | -------------------------- | ----------------------------------------- |
| `containers.started`               | `container_type`           | Number of containers started              |
| `containers.terminated`            | `container_type`, `reason` | Number of containers terminated           |
| `desired_state.received`           | -                          | Desired state messages from server        |
| `desired_state.function_executors` | -                          | Total function executors in desired state |
| `desired_state.allocations`        | -                          | Total allocations in desired state        |
| `heartbeat.success`                | -                          | Successful heartbeats                     |
| `heartbeat.failures`               | -                          | Failed heartbeats                         |
| `stream.disconnections`            | `reason`                   | Stream disconnections                     |

### Gauges (Observable)

| Metric                         | Unit  | Description                 |
| ------------------------------ | ----- | --------------------------- |
| `containers.running.functions` | count | Running function containers |
| `containers.running.sandboxes` | count | Running sandbox containers  |
| `containers.pending.functions` | count | Pending function containers |
| `containers.pending.sandboxes` | count | Pending sandbox containers  |
| `resources.free_cpu_percent`   | %     | Available CPU percentage    |
| `resources.free_memory_bytes`  | bytes | Available memory            |
| `resources.free_disk_bytes`    | bytes | Available disk space        |

## Logs

### Log Format

- **Local environment** (`env: local`): Human-readable compact format
- **Production** (`env: production`): JSON structured logs with tracing spans

### Log Levels

Control via `RUST_LOG` environment variable:

```bash
# Default: INFO level
RUST_LOG=info cargo run -p indexify-dataplane

# Debug level for troubleshooting
RUST_LOG=debug cargo run -p indexify-dataplane

# Specific module debug
RUST_LOG=indexify_dataplane::http_proxy=debug cargo run -p indexify-dataplane
```

### Key Log Events

| Event                                  | Level | Description                                        |
| -------------------------------------- | ----- | -------------------------------------------------- |
| `Starting HTTP proxy server`           | INFO  | Proxy startup with listen address                  |
| `proxy_request`                        | INFO  | Request span with sandbox_id, method, path, status |
| `Routing request to container`         | DEBUG | Request forwarded to container                     |
| `Request completed`                    | INFO  | Successful request with duration_ms                |
| `Missing Tensorlake-Sandbox-Id header` | WARN  | Request without required header                    |
| `Sandbox not found`                    | WARN  | Container ID not found                             |
| `Sandbox not running`                  | WARN  | Container exists but not in running state          |
| `Failed to connect to container`       | ERROR | Cannot reach container                             |

### Structured Log Fields

HTTP proxy request events include:

- `sandbox_id`: Target container ID
- `port`: Target port
- `method`: HTTP method
- `path`: Request path
- `status_code`: Response status
- `duration_ms`: Request duration
- `container_addr`: Resolved container address

### Example Log Output (JSON)

```json
{
  "timestamp": "2024-01-15T10:30:45.123456Z",
  "level": "INFO",
  "target": "indexify_dataplane::http_proxy",
  "span": {
    "sandbox_id": "sb-abc123",
    "port": 9501,
    "method": "POST",
    "path": "/api/v1/processes"
  },
  "fields": {
    "message": "Request completed",
    "status_code": 200,
    "duration_ms": 45
  }
}
```

## Local Development

### Prerequisites

1. **Docker Desktop** or **OrbStack** (recommended for macOS)
2. **Indexify Server** running locally

### Quick Start

```bash
# Terminal 1: Start Indexify Server
cargo run -p indexify-server

# Terminal 2: Start Dataplane
cargo run -p indexify-dataplane
```

The dataplane will:

- Connect to the server at `http://localhost:8901`
- Start HTTP proxy on port `8095`
- Report its proxy address to the server for routing

### Testing with Docker on macOS

When testing with Docker containers on macOS, you need to cross-compile the
Linux daemon binary:

```bash
# Build with Linux daemon for Docker containers
RUN_DOCKER_TESTS=1 cargo build -p indexify-dataplane

# Run with Docker driver
RUN_DOCKER_TESTS=1 cargo run -p indexify-dataplane -- --config /tmp/dataplane.yaml
```

**Important**: Without `RUN_DOCKER_TESTS=1`, the daemon binary will be compiled
for macOS and won't run inside Linux containers.

### Local Testing with Sandbox Proxy

When testing with sandbox-proxy locally, use `advertise_address` to avoid IPv6
resolution issues:

```yaml
# /tmp/dataplane-docker.yaml
env: local
server_addr: 'http://localhost:8901'
driver:
  type: docker
http_proxy:
  port: 8095
  listen_addr: '0.0.0.0'
  advertise_address: '127.0.0.1:8095' # Required for local testing
```

Without `advertise_address`, the dataplane advertises its hostname (e.g.,
`my-macbook.local:8095`), which may resolve to IPv6 and cause connection
failures from sandbox-proxy.

### Testing the HTTP Proxy

```bash
# Create a sandbox first via Indexify API
curl -X POST "http://localhost:8900/v1/namespaces/default/applications/test-app/sandboxes" \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.14-slim"}'
# Returns: {"sandbox_id":"abc123...","status":"Pending"}

# Wait for Running status
curl "http://localhost:8900/v1/namespaces/default/applications/test-app/sandboxes/abc123"

# Test routing directly to dataplane (bypassing sandbox-proxy)
curl -H "Tensorlake-Sandbox-Id: abc123" http://localhost:8095/api/v1/processes
# Returns: {"processes":[]}

# Test with explicit port (default daemon API port is 9501)
curl -H "Tensorlake-Sandbox-Id: abc123" -H "Tensorlake-Sandbox-Port: 9501" \
     http://localhost:8095/api/v1/processes

# Test port-based routing to a custom service
# First, start a server on port 8080 in the container:
curl -X POST -H "Tensorlake-Sandbox-Id: abc123" \
  -H "Content-Type: application/json" \
  http://localhost:8095/api/v1/processes \
  -d '{"command": "python", "args": ["-m", "http.server", "8080"]}'

# Then access port 8080:
curl -H "Tensorlake-Sandbox-Id: abc123" -H "Tensorlake-Sandbox-Port: 8080" http://localhost:8095/
# Returns: Directory listing HTML from Python HTTP server

# Test error cases
curl -H "Tensorlake-Sandbox-Id: nonexistent" http://localhost:8095/health
# Returns: 404 Sandbox not found

curl http://localhost:8095/health
# Returns: 400 Missing Tensorlake-Sandbox-Id header
```

### Creating a Test Sandbox

```bash
# Create namespace
curl -X POST http://localhost:8900/namespaces \
  -H "Content-Type: application/json" \
  -d '{"name": "default"}'

# Create application
curl -X POST http://localhost:8900/v1/namespaces/default/applications \
  -F 'application={"name":"test-app","functions":{}}'

# Create sandbox
curl -X POST http://localhost:8900/v1/namespaces/default/applications/test-app/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.14-slim"}'
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

2. **List managed containers**:

   ```bash
   docker ps --filter "label=indexify.managed=true"
   ```

3. **Check proxy routing**:

   ```bash
   RUST_LOG=indexify_dataplane::http_proxy=debug cargo run -p indexify-dataplane
   ```

4. **Inspect network rules** (Linux only):
   ```bash
   sudo iptables -L DOCKER-USER -n -v
   sudo iptables -L INDEXIFY-SB-<container_prefix> -n -v
   ```

## Troubleshooting

### Common Issues

| Issue                             | Cause                        | Solution                                          |
| --------------------------------- | ---------------------------- | ------------------------------------------------- |
| Heartbeat failures                | Server unreachable           | Check `server_addr` and network connectivity      |
| Container startup timeout         | Slow image pull              | Pre-pull images or increase timeout               |
| 400 Missing Tensorlake-Sandbox-Id | No header in request         | Ensure requests come through sandbox-proxy        |
| 404 Sandbox not found             | Container doesn't exist      | Verify sandbox was created                        |
| 503 Sandbox not running           | Container stopped/terminated | Check container state, may need recreation        |
| Network rules not applied         | Missing privileges           | Run with `--privileged` or `NET_ADMIN` capability |
| Resource detection wrong          | Running in container         | Mount `/proc:/host/proc:ro`                       |

### Health Checks

```bash
# Check if dataplane is sending heartbeats (server logs)
grep "heartbeat" /var/log/indexify-server.log

# List running containers
docker ps --filter "label=indexify.managed=true"

# Check HTTP proxy is listening
nc -zv localhost 8095

# Test proxy with debug logging
RUST_LOG=debug curl -H "Tensorlake-Sandbox-Id: test" http://localhost:8095/health
```
