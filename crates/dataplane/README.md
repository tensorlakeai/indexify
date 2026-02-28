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
function_driver:
  type: docker
sandbox_driver:
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

# Process driver for function executor containers
# Options: "fork_exec" (default), "docker", or "firecracker"
function_driver:
  type: docker
  # Docker daemon address (optional, uses default socket if not specified)
  # address: "unix:///var/run/docker.sock"

# Process driver for sandbox containers
# Can use a different backend than function_driver (e.g., gVisor for
# functions and Firecracker for sandboxes).
sandbox_driver:
  type: docker

  # Firecracker driver (requires --features firecracker):
  # type: firecracker
  # kernel_image_path: /opt/firecracker/vmlinux
  # base_rootfs_image: /opt/firecracker/rootfs.ext4
  # cni_network_name: indexify-fc
  # guest_gateway: "192.168.30.1"
  # lvm_volume_group: indexify-vg    # required: LVM volume group for thin-provisioned volumes
  # lvm_thin_pool: thinpool          # required: LVM thin pool name

# Snapshot storage URI for container filesystem snapshots (optional).
# Enables snapshot/restore for Docker (docker export) and Firecracker (thin_delta).
# snapshot_storage_uri: "s3://my-bucket/snapshots"

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

## Firecracker on EC2

The Firecracker driver runs microVMs with hardware virtualization and requires
an EC2 instance with KVM support, an LVM thin pool for per-VM thin-provisioned
volumes, and CNI networking. Firecracker supports both **x86_64** and
**aarch64** (Graviton).

See [`crates/dataplane/src/driver/firecracker/FIRECRACKER.md`](src/driver/firecracker/FIRECRACKER.md)
for the full Firecracker driver reference.

### EC2 instance requirements

Firecracker requires `/dev/kvm`. All Nitro-based EC2 instances (both `.metal`
and non-metal) expose KVM. The Firecracker project tests on these platforms:

| Family | Instance types | CPU |
|--------|----------------|-----|
| Intel | `m5n.metal`, `m6i.metal`, `m7i.metal-24xl` | Cascade Lake, Ice Lake, Sapphire Rapids |
| AMD | `m6a.metal`, `m7a.metal-48xl` | Milan, Genoa |
| Graviton | `m6g.metal`, `m7g.metal`, `m8g.metal-24xl` | Graviton2/3/4 |

Non-metal Nitro instances (e.g., `c5.xlarge`, `m6i.2xlarge`) also work for
development. Avoid `m8i.metal` (Granite Rapids) unless running a 6.1 host
kernel.

| Requirement | Details |
|-------------|---------|
| Host OS | Amazon Linux 2023 (linux 6.1) recommended. AL2 (linux 5.10) also supported. Ubuntu 22.04/24.04 works. |
| Storage | Root volume (gp3, 30+ GB) for OS and binaries. A second volume or instance store for the LVM thin pool. NVMe instance storage (e.g., `c5d`, `i3`) gives the best I/O performance for COW devices. |
| Security group | Inbound: 8095 (HTTP proxy from sandbox-proxy). Outbound: 8901 (gRPC to control plane). |

### Step 1: Launch the instance

```bash
# Example: c5d.2xlarge (NVMe instance storage for the thin pool)
aws ec2 run-instances \
  --image-id ami-0abcdef1234567890 \
  --instance-type c5d.2xlarge \
  --key-name my-key \
  --block-device-mappings \
    'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}'
```

Instance types with NVMe instance storage (`c5d`, `m5d`, `i3`, `i4i`, etc.)
attach the local disk automatically as `/dev/nvme1n1`. For instance types
without instance storage, add a second EBS volume:

```bash
aws ec2 run-instances \
  --image-id ami-0abcdef1234567890 \
  --instance-type m6i.2xlarge \
  --key-name my-key \
  --block-device-mappings \
    'DeviceName=/dev/xvda,Ebs={VolumeSize=30,VolumeType=gp3}' \
    'DeviceName=/dev/xvdb,Ebs={VolumeSize=200,VolumeType=gp3}'
```

### Step 2: Install system dependencies

```bash
# Amazon Linux 2023
sudo dnf install -y lvm2 device-mapper thin-provisioning-tools iptables jq golang

# Ubuntu 22.04/24.04
sudo apt-get update
sudo apt-get install -y lvm2 dmsetup thin-provisioning-tools iptables jq golang-go
```

### Step 3: Verify KVM access

```bash
# Check that the KVM kernel module is loaded
lsmod | grep kvm

# Verify /dev/kvm exists
ls -l /dev/kvm

# Grant your user access (Firecracker's recommended approach)
sudo setfacl -m u:${USER}:rw /dev/kvm

# Or: if running the dataplane as root, no extra permissions needed.
```

### Step 4: Set up the LVM thin pool

Use the dedicated block device for the thin pool. Instance store NVMe
(`/dev/nvme1n1`) or the extra EBS volume (`/dev/xvdb`) both work.

```bash
# Identify the block device
lsblk

# Create physical volume, volume group, and thin pool
sudo pvcreate /dev/nvme1n1          # or /dev/xvdb for EBS
sudo vgcreate indexify-vg /dev/nvme1n1
sudo lvcreate -l 100%FREE -T indexify-vg/thinpool

# Verify
sudo vgs indexify-vg
sudo lvs indexify-vg
```

> **Note:** Instance store volumes are ephemeral -- data is lost on
> stop/terminate. This is fine for COW devices (VMs are recreated on restart),
> but make sure snapshot data is stored in S3, not on the instance store.

### Step 5: Install Firecracker and guest artifacts

```bash
ARCH="$(uname -m)"

# Download the latest Firecracker release
release_url="https://github.com/firecracker-microvm/firecracker/releases"
latest=$(basename $(curl -fsSLI -o /dev/null -w %{url_effective} ${release_url}/latest))
curl -L ${release_url}/download/${latest}/firecracker-${latest}-${ARCH}.tgz \
  | tar -xz

# Install the binary
sudo mv release-${latest}-${ARCH}/firecracker-${latest}-${ARCH} /usr/local/bin/firecracker
chmod +x /usr/local/bin/firecracker
rm -rf release-${latest}-${ARCH}

firecracker --version
# Expected: Firecracker v1.14.1 (or later)
```

**Guest kernel and rootfs:** Firecracker provides CI-built kernels and Ubuntu
rootfs images on S3. The easiest approach is to use the getting-started script
from the Firecracker repo:

```bash
# Clone just the getting-started script
git clone --depth 1 https://github.com/firecracker-microvm/firecracker.git /tmp/fc-src

# Run the setup script (downloads kernel + Ubuntu 24.04 rootfs)
/tmp/fc-src/docs/getting-started/setup.sh

# Move artifacts to a permanent location
sudo mkdir -p /opt/firecracker
sudo mv vmlinux* /opt/firecracker/vmlinux
sudo mv ubuntu-24.04.ext4 /opt/firecracker/rootfs.ext4

rm -rf /tmp/fc-src
```

Alternatively, build your own rootfs or use any ext4 image. See the
[Firecracker rootfs docs](https://github.com/firecracker-microvm/firecracker/blob/main/docs/rootfs-and-kernel-setup.md)
for details.

### Step 6: Install CNI plugins

```bash
ARCH="$(uname -m)"

# Create the CNI bin directory
sudo mkdir -p /opt/cni/bin

# Install cnitool
sudo GOBIN=/opt/cni/bin go install github.com/containernetworking/cni/cnitool@latest

# Install standard CNI plugins (bridge, host-local, firewall, etc.)
CNI_VERSION="v1.6.1"
curl -fSL "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz" \
  | sudo tar -xz -C /opt/cni/bin

# Install tc-redirect-tap (required for Firecracker TAP networking)
sudo GOBIN=/opt/cni/bin go install github.com/awslabs/tc-redirect-tap/cmd/tc-redirect-tap@latest

# Create CNI config
sudo mkdir -p /etc/cni/net.d
sudo tee /etc/cni/net.d/indexify-fc.conflist > /dev/null <<'EOF'
{
  "name": "indexify-fc",
  "cniVersion": "0.4.0",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "fc-br0",
      "isGateway": true,
      "ipMasq": true,
      "ipam": {
        "type": "host-local",
        "subnet": "192.168.30.0/24",
        "routes": [{ "dst": "0.0.0.0/0" }]
      }
    },
    { "type": "firewall" },
    { "type": "tc-redirect-tap" }
  ]
}
EOF
```

### Step 7: Configure and start the dataplane

```yaml
# /etc/indexify/dataplane.yaml
env: production
server_addr: "http://control-plane.example.com:8901"
sandbox_driver:
  type: firecracker
  kernel_image_path: /opt/firecracker/vmlinux
  base_rootfs_image: /opt/firecracker/rootfs.ext4
  cni_network_name: indexify-fc
  guest_gateway: "192.168.30.1"
  lvm_volume_group: indexify-vg
  lvm_thin_pool: thinpool
http_proxy:
  port: 8095
  advertise_address: "<private-ip>:8095"
```

```bash
# Build with Firecracker support
cargo build --release -p indexify-dataplane --features firecracker

# Run (requires root for LVM thin provisioning and CNI operations)
sudo ./target/release/indexify-dataplane --config /etc/indexify/dataplane.yaml
```

### Verifying the setup

```bash
# Check that VMs create thin LVs
sudo lvs indexify-vg
# Should show:
#   indexify-base        (base image thin LV)
#   indexify-vm-{vm_id}  (per-VM thin snapshots, one per running VM)
#   thinpool             (the thin pool itself)

# Verify thin provisioning is working (data_percent shows actual usage)
sudo lvs -o lv_name,data_percent indexify-vg

# Check that thin_delta is available (used for efficient snapshots)
which thin_delta
# Should print a path like /usr/sbin/thin_delta
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
function_driver:
  type: docker
sandbox_driver:
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
