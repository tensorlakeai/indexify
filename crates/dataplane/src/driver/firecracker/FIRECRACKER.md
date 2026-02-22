# Firecracker MicroVM Driver

Hardware-virtualized isolation for Indexify workloads using
[Firecracker](https://firecracker-microvm.github.io/) microVMs,
dm-snapshot CoW volumes for per-VM rootfs, and CNI networking.

Gated behind the `firecracker` Cargo feature flag.

## Status

**Phase 1, Phase 2a, and snapshot/restore are complete and tested.**

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Core VM lifecycle: start, stop, kill, health checks, logs, recovery | Done |
| 1.1 | VM log streaming into dataplane tracing | Done |
| 2a | Fast rootfs provisioning via dm-snapshot CoW | Done |
| 2c | Filesystem snapshot/restore (COW file to/from S3) | Done |
| 2b | Firecracker memory snapshots (warm start) | Not started |

### What Phase 1 covers

- Full `ProcessDriver` trait implementation (same interface as Docker and fork/exec drivers)
- Per-VM rootfs from a base ext4 image via dm-snapshot CoW
- CNI-based networking with per-VM network namespaces and TAP devices
- Guest init script that mounts filesystems, configures networking, and execs the container daemon as PID 1
- VM metadata persistence to disk for recovery after dataplane restart
- dm-snapshot devices persist in the kernel across dataplane restarts (running VMs survive restarts)
- 12 integration tests that boot real Firecracker VMs, plus unit tests for each module

### What Phase 1.1 covers (VM log streaming)

- **Structured log streaming**: both Firecracker VMM logs (`fc-{vm_id}.log`) and guest serial console output (`fc-{vm_id}-serial.log`) are streamed into the dataplane's tracing output in real time
- **Label propagation**: the allocation controller now passes `indexify.container_id`, `indexify.namespace`, `indexify.application`, `indexify.function`, `indexify.sandbox_id`, and `indexify.pool_id` labels through `ProcessConfig` into the Firecracker driver
- **Labels persisted for recovery**: labels are stored in `VmMetadata` (with `#[serde(default)]` for backward compatibility with pre-labels metadata files), so recovered VMs also get log streaming with proper attribution
- **Non-blocking I/O**: all file reads run inside `tokio::task::spawn_blocking` to avoid starving the Tokio runtime
- **Truncation/rotation handling**: the log tailer detects when a file has been truncated (file size < read position) and re-opens from the beginning
- **Early streamer spawn**: the log streamer starts right after the Firecracker process is spawned, before the API socket wait, so boot-time output is captured
- **Lifecycle-aware cleanup**: log streamers are cancelled via `CancellationToken` during VM cleanup, and log files are removed alongside socket and metadata files
- **Panic safety**: the streamer task catches panics from the inner task and logs them instead of silently swallowing

### What Phase 2a covers (dm-snapshot CoW rootfs)

- **Origin device**: on startup, the base rootfs image is loop-mounted read-only and exposed as a `linear` dm target (`/dev/mapper/indexify-base`). The origin persists across restarts (idempotent setup).
- **Instant CoW snapshots**: per-VM rootfs volumes are dm-snapshot targets that use the origin as the base and a per-VM COW file for writes. Creation is near-instant (<10ms).
- **Explicit COW files**: each VM's writes go to `{state_dir}/overlays/{vm_id}.cow`. The COW file IS the delta — no `thin_delta` extraction needed.
- **Inject-only rootfs preparation**: after snapshotting, only the daemon binary, init script, and env vars are injected (mount + write ~50ms). No `dd` needed.
- **No `libdevmapper-dev` build dependency**: all device-mapper operations use `dmsetup` and `losetup` commands.

### What Phase 2c covers (filesystem snapshot/restore)

- **`FirecrackerSnapshotter`** implements the `Snapshotter` trait for Firecracker VMs
- **Snapshot**: reads the VM's COW file → zstd compress → streams to blob store (S3, local FS)
- **Restore**: streams from blob store → zstd decompress → writes COW file → dm-snapshot → boot
- **Streaming pipeline**: bounded memory usage (~100MB chunks), same pattern as `DockerSnapshotter`

### What is not yet covered

- **Firecracker memory snapshots (Phase 2b)** -- VMs still cold-boot. Phase 2b adds `PUT /snapshot/create` after first daemon boot to create a golden memory snapshot, then `PUT /snapshot/load` for sub-second warm starts.
- **Metrics** -- no per-VM CPU/memory usage tracking or operation latency counters. The existing dataplane OTLP metrics pipeline does not yet instrument Firecracker operations.

## Architecture

```
dataplane
  │
  ├── FirecrackerDriver (mod.rs)
  │     ProcessDriver trait impl, VM lifecycle orchestration, recovery
  │
  ├── DmSnapshotManager (dm_snapshot.rs)
  │     dm-snapshot via dmsetup/losetup: origin setup, per-VM CoW snapshots
  │
  ├── FirecrackerSnapshotter (snapshotter/firecracker_snapshotter.rs)
  │     Snapshotter trait impl: COW file ↔ zstd ↔ blob store
  │
  ├── CniManager (cni.rs)
  │     cnitool invocation, netns lifecycle, TAP device discovery
  │
  ├── FirecrackerApiClient (api.rs)
  │     HTTP/1.1 over Unix socket to Firecracker API
  │
  ├── rootfs preparation (rootfs.rs)
  │     inject daemon binary + init script + env vars into CoW snapshot
  │
  ├── log streaming (log_stream.rs)
  │     poll-based tailer for VMM + serial logs, emits via tracing
  │
  └── VmState / VmMetadata (vm_state.rs)
        per-VM state tracking, metadata + labels persistence for recovery
```

### Storage architecture

```
Base rootfs file (/opt/firecracker/rootfs.ext4)
  → losetup --read-only → /dev/loopN
  → dmsetup create indexify-base  "0 $SZ linear /dev/loopN 0"

Per-VM:
  COW file: {state_dir}/overlays/{vm_id}.cow  (pre-allocated, same size as rootfs)
  → losetup → /dev/loopM
  → dmsetup create indexify-vm-{vm_id}  "0 $SZ snapshot /dev/mapper/indexify-base /dev/loopM P 8"

  /dev/mapper/indexify-vm-{vm_id}  ← VM sees this as rootfs (full read-write block device)
    - Reads of unmodified blocks → served from base image (zero-copy)
    - Writes → go to COW file only

Snapshot:  read COW file → zstd compress → upload to S3
Restore:   download from S3 → zstd decompress → COW file → losetup → dm-snapshot → boot
```

### VM start sequence

1. Create dm-snapshot (allocate COW file, losetup, dmsetup create) (<10ms)
2. Mount snapshot, inject daemon binary, init script, env vars, unmount (~50ms)
3. Set up CNI networking (create netns, `cnitool add`, find TAP device)
4. Generate deterministic MAC address from VM ID
5. Build kernel boot args with guest IP, gateway, netmask
6. Spawn `firecracker` inside the network namespace via `ip netns exec`
7. Spawn log streamer (tails VMM + serial logs, captures boot output)
8. Wait for API socket (poll every 50ms, 5s timeout)
9. Configure VM via Firecracker API (boot source, rootfs, machine config, network)
10. Send `InstanceStart` action
11. Write VM metadata JSON (including labels) to state directory
12. Return `ProcessHandle` with daemon gRPC and HTTP addresses

On failure at any step after spawning the process: cancel log streamer, kill process, teardown CNI, destroy dm-snapshot + COW file, remove socket, metadata, and log files.

### Recovery

On startup, the driver scans the state directory for `fc-{vm_id}.json` metadata files. For each:

- If the PID is alive and is a firecracker process: the dm-snapshot target still exists in the kernel (no reconnection needed), spawn a log streamer with labels from persisted metadata
- If the PID is dead: schedule cleanup (destroy dm-snapshot + COW file, teardown CNI, remove metadata and log files)

The origin device is set up idempotently — if `/dev/mapper/indexify-base` already exists, it's reused.

## Host prerequisites

| Component | Purpose | Install |
|-----------|---------|---------|
| `firecracker` binary | VM monitor | [firecracker releases](https://github.com/firecracker-microvm/firecracker/releases) |
| Linux kernel image (`vmlinux`) | Guest kernel | Build or download from firecracker docs |
| Base rootfs ext4 image | Guest filesystem | Any ext4 image (Ubuntu, Alpine, etc.) |
| `cnitool` | CNI plugin runner | `go install github.com/containernetworking/cni/cnitool@latest` |
| CNI plugins: `bridge`, `firewall`, `tc-redirect-tap` | Networking | [CNI plugins](https://github.com/containernetworking/plugins/releases) + [tc-redirect-tap](https://github.com/awslabs/tc-redirect-tap) |
| `dmsetup` | Device-mapper management | Usually pre-installed (part of `dmsetup` / `device-mapper` package) |
| KVM access (`/dev/kvm`) | Hardware virtualization | Host must support KVM |

**No dedicated block devices needed.** COW data lives in regular files under the state directory.

### CNI configuration

Create `/etc/cni/net.d/indexify-fc.conflist`:

```json
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
```

## Configuration

```yaml
driver:
  type: firecracker
  kernel_image_path: /opt/firecracker/vmlinux
  base_rootfs_image: /opt/firecracker/rootfs.ext4
  cni_network_name: indexify-fc
  guest_gateway: "192.168.30.1"
```

All fields:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `kernel_image_path` | yes | -- | Path to uncompressed Linux kernel (vmlinux) |
| `base_rootfs_image` | yes | -- | Path to base ext4 rootfs image |
| `cni_network_name` | yes | -- | CNI network name (must match conflist `name` field) |
| `guest_gateway` | yes | -- | Gateway IP for guest networking |
| `firecracker_binary` | no | `firecracker` | Path to firecracker binary (uses PATH if not set) |
| `default_rootfs_size_bytes` | no | `1073741824` (1 GiB) | Per-VM COW file size |
| `cni_bin_path` | no | `/opt/cni/bin` | Directory containing CNI plugin binaries |
| `guest_netmask` | no | `255.255.255.0` | Guest network mask |
| `default_vcpu_count` | no | `2` | vCPUs per VM |
| `default_memory_mib` | no | `512` | Memory per VM in MiB |
| `state_dir` | no | `/var/lib/indexify/firecracker` | VM metadata, sockets, and COW overlays directory |
| `log_dir` | no | `/var/log/indexify/firecracker` | VM log directory |

## Building

```bash
# With firecracker support (no libdevmapper dependency needed)
cargo build -p indexify-dataplane --features firecracker

# Without (firecracker module excluded)
cargo build -p indexify-dataplane
```

## Testing

Unit tests (no infrastructure required):

```bash
cargo test -p indexify-dataplane --features firecracker --lib
```

Integration tests (requires root, firecracker, CNI):

```bash
sudo bash -c "export PATH='$PATH' HOME='$HOME'; \
  FC_KERNEL_IMAGE=/path/to/vmlinux \
  FC_BASE_ROOTFS=/path/to/rootfs.ext4 \
  cargo test -p indexify-dataplane --features firecracker \
    --test firecracker_integration_test -- --test-threads=1"
```

Tests skip gracefully if prerequisites are missing. The `--test-threads=1` is required because all tests share the origin device.

### Environment variables for integration tests

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FC_KERNEL_IMAGE` | yes | -- | Path to vmlinux |
| `FC_BASE_ROOTFS` | yes | -- | Base ext4 image |
| `FC_CNI_NETWORK` | no | `indexify-fc` | CNI network name |
| `FC_GUEST_GATEWAY` | no | `192.168.30.1` | Guest gateway IP |

## Module overview

### `mod.rs` -- FirecrackerDriver

`ProcessDriver` implementation. Orchestrates the full VM lifecycle: dm-snapshot allocation, rootfs preparation, CNI networking, Firecracker API configuration, process management, and cleanup. Handles recovery by scanning metadata files on startup. No `Arc<Mutex<>>` needed for the snapshot manager — functions are stateless.

### `dm_snapshot.rs` -- DmSnapshotManager

Manages the origin linear device and per-VM dm-snapshot targets via `dmsetup` and `losetup` commands. No `libdevmapper-dev` build dependency. Provides:

- Origin setup (idempotent `losetup` + `dmsetup create linear`)
- Per-VM snapshot creation (`fallocate` COW file + `losetup` + `dmsetup create snapshot`)
- Snapshot destruction (remove dm target + detach loop + delete COW file)
- Snapshot-from-COW (restore path: existing COW file → `losetup` → `dmsetup create`)
- Async wrappers via `spawn_blocking`

### `cni.rs` -- CniManager

Invokes `cnitool` to set up per-VM networking. Each VM gets a dedicated network namespace, TAP device, IP, and MAC address. Teardown is idempotent.

### `api.rs` -- FirecrackerApiClient

Raw HTTP/1.1 over `tokio::net::UnixStream`. Sends PUT requests to configure boot source, drives, machine config, network interfaces, and start the instance.

### `rootfs.rs` -- Rootfs preparation

Provides `inject_rootfs()` which mounts a CoW snapshot volume and injects per-VM files (daemon binary, init script, env vars).

### `log_stream.rs` -- VM log streaming

Poll-based log tailer that streams Firecracker VMM logs and guest serial console output into the dataplane's structured tracing.

### `vm_state.rs` -- VM state and metadata

`VmProcess` enum distinguishes owned processes from recovered ones. `VmMetadata` tracks `cow_file`, `loop_device`, and `dm_name` per VM (with `#[serde(default)]` for backward compatibility). `OriginMetadata` tracks the origin's base image path, loop device, and dm name.

## Phase 2b outline (not started)

Phase 2a (dm-snapshot CoW rootfs) and Phase 2c (filesystem snapshot/restore) are complete. Phase 2b adds Firecracker memory snapshots:

- Firecracker memory snapshot (`PUT /snapshot/create`) after first daemon boot -> golden snapshot
- Subsequent VMs restore from golden snapshot (`PUT /snapshot/load`) instead of cold boot
- Expected: VM start from ~1s (Phase 2a cold boot) to <500ms (warm restore)
