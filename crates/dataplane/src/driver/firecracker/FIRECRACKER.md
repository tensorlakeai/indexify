# Firecracker MicroVM Driver

Hardware-virtualized isolation for Indexify workloads using
[Firecracker](https://firecracker-microvm.github.io/) microVMs,
LVM thin provisioning for rootfs, and CNI networking.

Gated behind the `firecracker` Cargo feature flag.

## Status

**Phase 1, Phase 2a, and snapshot/restore are complete and tested.**

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Core VM lifecycle: start, stop, kill, health checks, logs, recovery | Done |
| 1.1 | VM log streaming into dataplane tracing | Done |
| 2a | Fast rootfs provisioning via LVM thin snapshots | Done |
| 2c | Filesystem snapshot/restore (delta to/from S3 via thin_delta) | Done |
| 2b | Firecracker memory snapshots (warm start) | Not started |

### What Phase 1 covers

- Full `ProcessDriver` trait implementation (same interface as Docker and fork/exec drivers)
- Per-VM rootfs from a base ext4 image via LVM thin snapshots
- CNI-based networking with per-VM network namespaces and TAP devices
- Guest init script that mounts filesystems, configures networking, and execs the container daemon as PID 1
- VM metadata persistence to disk for recovery after dataplane restart
- Thin LVs persist in LVM metadata across dataplane restarts (running VMs survive restarts)
- 12 integration tests that boot real Firecracker VMs, plus unit tests for each module

### What Phase 1.1 covers (VM log streaming)

- **Structured log streaming**: both Firecracker VMM logs (`fc-{vm_id}.log`) and guest serial console output (`fc-{vm_id}-serial.log`) are streamed into the dataplane's tracing output in real time
- **Label propagation**: the allocation controller now passes `indexify.container_id`, `indexify.namespace`, `indexify.application`, `indexify.function`, `indexify.sandbox_id`, and `indexify.pool_id` labels through `ProcessConfig` into the Firecracker driver
- **Labels persisted for recovery**: labels are stored in `VmMetadata` so recovered VMs also get log streaming with proper attribution
- **Non-blocking I/O**: all file reads run inside `tokio::task::spawn_blocking` to avoid starving the Tokio runtime
- **Truncation/rotation handling**: the log tailer detects when a file has been truncated (file size < read position) and re-opens from the beginning
- **Early streamer spawn**: the log streamer starts right after the Firecracker process is spawned, before the API socket wait, so boot-time output is captured
- **Lifecycle-aware cleanup**: log streamers are cancelled via `CancellationToken` during VM cleanup, and log files are removed alongside socket and metadata files
- **Panic safety**: the streamer task catches panics from the inner task and logs them instead of silently swallowing

### What Phase 2a covers (LVM thin snapshot rootfs)

- **Base image thin LV**: on startup, the base rootfs image is imported into a thin LV (`indexify-base`) via `dd`. The base LV persists across restarts (idempotent setup).
- **Instant CoW snapshots**: per-VM rootfs volumes are native LVM thin snapshots of the base LV. Creation is near-instant (<10ms). Each snapshot can be independently resized beyond the base image size.
- **COW block sharing**: unchanged blocks are shared between all VMs via thin provisioning's COW mechanism. Only modified blocks consume pool space.
- **Inject-only rootfs preparation**: after snapshotting, only the daemon binary, init script, and env vars are injected (mount + write ~50ms). No `dd` needed.
- **No `libdevmapper-dev` build dependency**: all LVM operations use `lvcreate`/`lvremove`/`lvs` commands. Device-mapper operations use `dmsetup`.
- **Startup validation**: the driver validates that the configured LVM volume group and thin pool exist before accepting VMs.
- **Automatic cleanup**: orphaned `indexify-vm-*` thin LVs from previous runs are scanned via `lvs` and removed on startup.

### What Phase 2c covers (filesystem snapshot/restore)

- **`FirecrackerSnapshotter`** implements the `Snapshotter` trait for Firecracker VMs
- **Snapshot**: `thin_delta` metadata query identifies changed blocks -> read only those blocks from the VM device -> emit as delta records -> zstd compress -> stream to blob store (S3, local FS)
- **Restore**: stream from blob store -> zstd decompress -> write delta file -> apply block records into thin snapshot (COW preserved for unchanged blocks)
- **Streaming pipeline**: bounded memory usage (~100MB chunks), same pattern as `DockerSnapshotter`
- **thin_delta optimization**: reads only changed blocks (~500MB) instead of the entire device (10GB+). Read I/O scales with amount of change, not image size.

### What is not yet covered

- **Firecracker memory snapshots (Phase 2b)** -- VMs still cold-boot. Phase 2b adds `PUT /snapshot/create` after first daemon boot to create a golden memory snapshot, then `PUT /snapshot/load` for sub-second warm starts.
- **Metrics** -- no per-VM CPU/memory usage tracking or operation latency counters. The existing dataplane OTLP metrics pipeline does not yet instrument Firecracker operations.

## Architecture

```
dataplane
  |
  +-- FirecrackerDriver (mod.rs)
  |     ProcessDriver trait impl, VM lifecycle orchestration, recovery
  |
  +-- DmThinManager (dm_thin.rs)
  |     LVM thin provisioning via lvcreate/lvremove/lvs + thin_delta
  |
  +-- FirecrackerSnapshotter (snapshotter/firecracker_snapshotter.rs)
  |     Snapshotter trait impl: thin_delta -> delta records -> zstd <-> blob store
  |
  +-- CniManager (cni.rs)
  |     cnitool invocation, netns lifecycle, TAP device discovery
  |
  +-- FirecrackerApiClient (api.rs)
  |     HTTP/1.1 over Unix socket to Firecracker API
  |
  +-- rootfs preparation (rootfs.rs)
  |     inject daemon binary + init script + env vars into thin snapshot
  |
  +-- log streaming (log_stream.rs)
  |     poll-based tailer for VMM + serial logs, emits via tracing
  |
  +-- VmState / VmMetadata (vm_state.rs)
        per-VM state tracking, metadata + labels persistence for recovery
```

### Storage architecture

```
Base rootfs file (/opt/firecracker/rootfs.ext4)
  -> dd into thin LV "indexify-base" (one-time import)

Per-VM:
  lvcreate --snapshot {vg}/indexify-base --name indexify-vm-{vm_id}
  -> optional lvresize + resize2fs for larger disks
  -> /dev/{vg}/indexify-vm-{vm_id}  <- VM sees this as rootfs

  Reads of unmodified blocks -> served from base LV via COW (zero-copy)
  Writes -> go to VM's thin snapshot only

Snapshot:  thin_delta metadata query -> read changed blocks -> delta records -> zstd -> S3
Restore:   download from S3 -> zstd decompress -> delta file -> apply block records into thin snapshot
```

### VM start sequence

1. Create thin snapshot (`lvcreate --snapshot`) (<10ms)
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

On failure at any step after spawning the process: cancel log streamer, kill process, teardown CNI, destroy thin LV, remove socket, metadata, and log files.

### Recovery

On startup, the driver scans the state directory for `fc-{vm_id}.json` metadata files. For each:

- If the PID is alive and is a firecracker process: the thin LV still exists in LVM (no reconnection needed), spawn a log streamer with labels from persisted metadata
- If the PID is dead: schedule cleanup (destroy thin LV, teardown CNI, remove metadata and log files)

The base image thin LV is set up idempotently -- if `indexify-base` already exists with the correct size, it's reused.

Orphaned `indexify-vm-*` thin LVs (those not associated with any active VM) are scanned via `lvs` and removed on startup.

## Host prerequisites

| Component | Purpose | Install |
|-----------|---------|---------|
| `firecracker` binary | VM monitor | [firecracker releases](https://github.com/firecracker-microvm/firecracker/releases) |
| Linux kernel image (`vmlinux`) | Guest kernel | Build or download from firecracker docs |
| Base rootfs ext4 image | Guest filesystem | Any ext4 image (Ubuntu, Alpine, etc.) |
| `cnitool` | CNI plugin runner | `go install github.com/containernetworking/cni/cnitool@latest` |
| CNI plugins: `bridge`, `firewall`, `tc-redirect-tap` | Networking | [CNI plugins](https://github.com/containernetworking/plugins/releases) + [tc-redirect-tap](https://github.com/awslabs/tc-redirect-tap) |
| `dmsetup` | thin_delta metadata queries, thin LV suspend/resume | Usually pre-installed (part of `device-mapper` package) |
| `thin-provisioning-tools` | `thin_delta` for efficient snapshot diff | `apt install thin-provisioning-tools` |
| LVM2 (`lvcreate`, `lvremove`, `lvs`, `vgs`) | Thin-provisioned volumes | `apt install lvm2` or `yum install lvm2` |
| KVM access (`/dev/kvm`) | Hardware virtualization | Host must support KVM |

### LVM thin pool setup

Each per-VM volume is a thin LV in a shared thin pool. You need to provision a volume group and thin pool before starting the dataplane.

**Production (dedicated block device):**

```bash
# Use a dedicated disk or partition (e.g., NVMe instance storage on EC2)
sudo pvcreate /dev/nvme1n1
sudo vgcreate indexify-vg /dev/nvme1n1
sudo lvcreate -l 100%FREE -T indexify-vg/thinpool
```

**Development (loopback file):**

```bash
# Create a loopback file for testing (e.g., 50 GB)
sudo fallocate -l 50G /var/lib/indexify-lvm.img
sudo losetup --find --show /var/lib/indexify-lvm.img   # note the /dev/loopN
sudo pvcreate /dev/loopN
sudo vgcreate indexify-vg /dev/loopN
sudo lvcreate -l 100%FREE -T indexify-vg/thinpool
```

**Verify:**

```bash
sudo vgs indexify-vg      # should show the volume group
sudo lvs indexify-vg      # should show the thin pool
```

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
sandbox_driver:
  type: firecracker
  kernel_image_path: /opt/firecracker/vmlinux
  base_rootfs_image: /opt/firecracker/rootfs.ext4
  cni_network_name: indexify-fc
  guest_gateway: "192.168.30.1"
  lvm_volume_group: indexify-vg
  lvm_thin_pool: thinpool
```

All fields:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `kernel_image_path` | yes | -- | Path to uncompressed Linux kernel (vmlinux) |
| `base_rootfs_image` | yes | -- | Path to base ext4 rootfs image |
| `cni_network_name` | yes | -- | CNI network name (must match conflist `name` field) |
| `guest_gateway` | yes | -- | Gateway IP for guest networking |
| `lvm_volume_group` | yes | -- | LVM volume group for thin-provisioned volumes |
| `lvm_thin_pool` | yes | -- | LVM thin pool LV name within the volume group |
| `firecracker_binary` | no | `firecracker` | Path to firecracker binary (uses PATH if not set) |
| `default_rootfs_size_bytes` | no | `1073741824` (1 GiB) | Per-VM thin LV size |
| `cni_bin_path` | no | `/opt/cni/bin` | Directory containing CNI plugin binaries |
| `guest_netmask` | no | `255.255.255.0` | Guest network mask |
| `default_vcpu_count` | no | `2` | vCPUs per VM |
| `default_memory_mib` | no | `512` | Memory per VM in MiB |
| `state_dir` | no | `/var/lib/indexify/firecracker` | VM metadata and sockets directory |
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

Integration tests (requires root, firecracker, CNI, LVM thin pool):

```bash
sudo bash -c "export PATH='$PATH' HOME='$HOME'; \
  FC_KERNEL_IMAGE=/path/to/vmlinux \
  FC_BASE_ROOTFS=/path/to/rootfs.ext4 \
  FC_LVM_VG=indexify-vg \
  FC_LVM_POOL=thinpool \
  cargo test -p indexify-dataplane --features firecracker \
    --test firecracker_integration_test -- --test-threads=1"
```

Tests skip gracefully if prerequisites are missing. The `--test-threads=1` is required because all tests share the base image thin LV.

### Environment variables for integration tests

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FC_KERNEL_IMAGE` | yes | -- | Path to vmlinux |
| `FC_BASE_ROOTFS` | yes | -- | Base ext4 image |
| `FC_CNI_NETWORK` | no | `indexify-fc` | CNI network name |
| `FC_GUEST_GATEWAY` | no | `192.168.30.1` | Guest gateway IP |
| `FC_LVM_VG` | no | `indexify-vg` | LVM volume group name |
| `FC_LVM_POOL` | no | `thinpool` | LVM thin pool name |

## Module overview

### `mod.rs` -- FirecrackerDriver

`ProcessDriver` implementation. Orchestrates the full VM lifecycle: LVM thin snapshot creation, rootfs preparation, CNI networking, Firecracker API configuration, process management, and cleanup. Handles recovery by scanning metadata files on startup. Validates LVM config at construction time.

### `dm_thin.rs` -- DmThinManager

Manages the base image thin LV and per-VM thin snapshots via `lvcreate`, `lvremove`, `lvs`, and `dmsetup` commands. No `libdevmapper-dev` build dependency. Provides:

- `LvmConfig` struct with `volume_group` and `thin_pool` fields
- `validate_lvm_config()` -- checks VG and thin pool exist via `vgs`/`lvs`
- Base image setup (idempotent `dd` into thin LV `indexify-base`)
- Per-VM thin snapshot creation (`lvcreate --snapshot`, optional `lvresize` + `resize2fs`)
- Thin snapshot destruction (`lvremove -f`)
- Delta snapshot support: `query_thin_delta()` uses `thin_delta` metadata queries to identify changed blocks
- Delta restore: `create_snapshot_from_delta()` applies block records into thin snapshots (COW preserved)
- Suspend/resume for consistent snapshots (`dmsetup suspend/resume`)
- Stale device cleanup (scans `lvs` for orphaned `indexify-vm-*` thin LVs)
- Async wrappers via `spawn_blocking`

### `cni.rs` -- CniManager

Invokes `cnitool` to set up per-VM networking. Each VM gets a dedicated network namespace, TAP device, IP, and MAC address. Teardown is idempotent.

### `api.rs` -- FirecrackerApiClient

Raw HTTP/1.1 over `tokio::net::UnixStream`. Sends PUT requests to configure boot source, drives, machine config, network interfaces, and start the instance.

### `rootfs.rs` -- Rootfs preparation

Provides `inject_rootfs()` which mounts a thin snapshot volume and injects per-VM files (daemon binary, init script, env vars).

### `log_stream.rs` -- VM log streaming

Poll-based log tailer that streams Firecracker VMM logs and guest serial console output into the dataplane's structured tracing.

### `vm_state.rs` -- VM state and metadata

`VmProcess` enum distinguishes owned processes from recovered ones. `VmMetadata` tracks `lv_name` per VM. `BaseImageMetadata` tracks the base image path and LV name.

## Phase 2b outline (not started)

Phase 2a (LVM thin snapshot rootfs) and Phase 2c (filesystem snapshot/restore via thin_delta) are complete. Phase 2b adds Firecracker memory snapshots:

- Firecracker memory snapshot (`PUT /snapshot/create`) after first daemon boot -> golden snapshot
- Subsequent VMs restore from golden snapshot (`PUT /snapshot/load`) instead of cold boot
- Expected: VM start from ~1s (Phase 2a cold boot) to <500ms (warm restore)
