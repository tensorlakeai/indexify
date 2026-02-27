//! dm-snapshot based volume management for Firecracker rootfs.
//!
//! Uses dmsetup thin provisioning for per-VM COW devices. Each VM gets a thin
//! device in the LVM thin pool as its COW device, allocated via `dmsetup`
//! commands to avoid device ID collisions with containerd's devmapper
//! snapshotter which shares the same thin pool.
//!
//! Architecture:
//! ```text
//! Base rootfs file (/opt/firecracker/rootfs.ext4)
//!   → losetup --read-only → /dev/loopN
//!   → dmsetup create indexify-base  "0 $SZ linear /dev/loopN 0"
//!
//! Per-VM:
//!   dmsetup message /dev/mapper/<pool_dm> 0 "create_thin <device_id>"
//!   dmsetup create indexify-cow-{vm_id}  "0 $SZ thin /dev/mapper/<pool_dm> <device_id>"
//!   dmsetup create indexify-vm-{vm_id}   "0 $SZ snapshot /dev/mapper/indexify-base /dev/mapper/indexify-cow-{vm_id} P 8"
//! ```
//!
//! Firecracker thin device IDs start at 10,000,000 to avoid collisions with
//! containerd (which starts from 1 and increments slowly).
//!
//! All device-mapper operations use `dmsetup` and `losetup` commands —
//! no `libdevmapper-dev` build dependency needed.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};

/// LVM thin pool configuration for per-VM COW devices.
#[derive(Debug, Clone)]
pub struct LvmConfig {
    /// LVM volume group name (e.g., "indexify-vg").
    pub volume_group: String,
    /// LVM thin pool LV name within the volume group (e.g., "thinpool").
    pub thin_pool: String,
    /// Resolved dm device name of the actual thin-pool target. Populated by
    /// `validate_lvm_config()`. This may differ from the LVM-derived name
    /// (e.g. `indexify--vg-thinpool-tpool` vs `indexify--vg-thinpool`) when
    /// LVM layers the pool behind a wrapper device.
    pub pool_dm_device: String,
}

/// Handle to the shared origin (base rootfs) linear device.
pub struct OriginHandle {
    /// dm device name (e.g., "indexify-base").
    pub dm_name: String,
    /// Loop device backing the base rootfs file (e.g., "/dev/loop0").
    pub loop_device: String,
    /// Full device path (e.g., "/dev/mapper/indexify-base").
    pub device_path: PathBuf,
    /// Size of the origin in 512-byte sectors.
    pub size_sectors: u64,
}

/// Handle to a per-VM dm-snapshot device.
#[allow(dead_code)]
pub struct SnapshotHandle {
    /// dm device name (e.g., "indexify-vm-abc123").
    pub dm_name: String,
    /// Full device path (e.g., "/dev/mapper/indexify-vm-abc123").
    pub device_path: PathBuf,
    /// COW dm device name (e.g., "indexify-cow-abc123").
    pub cow_dm_name: String,
    /// COW device path (e.g., "/dev/mapper/indexify-cow-abc123").
    pub cow_device: String,
    /// Thin device ID in the pool for the COW device.
    pub thin_device_id: u32,
}

/// Status of a dm-snapshot target.
#[derive(Debug)]
#[allow(dead_code)]
pub struct SnapshotStatus {
    /// Number of sectors allocated (written) in the COW device.
    pub sectors_allocated: u64,
    /// Total sectors available in the COW device.
    pub sectors_total: u64,
}

// ---------------------------------------------------------------------------
// Thin device ID allocator (dmsetup-based)
// ---------------------------------------------------------------------------

/// Base device ID for Firecracker thin devices. containerd's devmapper
/// snapshotter starts from 1 and increments slowly (tens of IDs per image),
/// so 10,000,000+ will never collide.
const FC_DEVICE_ID_BASE: u32 = 10_000_000;

/// Monotonic counter for thin device IDs, persisted to a file.
pub struct ThinDeviceAllocator {
    next_id: Mutex<u32>,
    path: PathBuf,
}

impl ThinDeviceAllocator {
    /// Create a new allocator. Loads the counter from the state file, or
    /// scans existing devices to recover the highest ID if the file is missing.
    pub fn new(state_dir: &Path, lvm_config: &LvmConfig) -> Self {
        let path = state_dir.join("thin_device_counter");
        let next_id = if path.exists() {
            std::fs::read_to_string(&path)
                .ok()
                .and_then(|s| s.trim().parse::<u32>().ok())
                .unwrap_or_else(|| Self::recover_max_id(lvm_config))
        } else {
            Self::recover_max_id(lvm_config)
        };

        tracing::info!(next_id, path = %path.display(), "Thin device allocator initialized");

        Self {
            next_id: Mutex::new(next_id),
            path,
        }
    }

    /// Allocate the next thin device ID and persist the counter.
    pub fn allocate(&self) -> u32 {
        let mut next = self.next_id.lock().expect("allocator lock poisoned");
        let id = *next;
        *next = id + 1;
        // Persist best-effort — if this fails, recovery scan will fix it.
        let _ = std::fs::write(&self.path, next.to_string());
        id
    }

    /// Scan existing `indexify-cow-*` dm devices to find the max device ID.
    /// Falls back to FC_DEVICE_ID_BASE if no devices found.
    fn recover_max_id(lvm_config: &LvmConfig) -> u32 {
        let mut max_id = FC_DEVICE_ID_BASE;
        let pool_dev = format!("/dev/mapper/{}", lvm_config.pool_dm_device);

        if let Ok(output) = run_cmd("dmsetup", &["ls"]) {
            for line in output.lines() {
                if let Some(name) = line.split_whitespace().next() {
                    if name.starts_with("indexify-cow-") {
                        // Parse the thin table to get device ID
                        if let Ok(table) = run_cmd("dmsetup", &["table", name]) {
                            // Table: "0 <size> thin <pool_dev> <device_id>"
                            if let Some(id) = parse_thin_device_id(&table, &pool_dev) {
                                if id >= max_id {
                                    max_id = id + 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        max_id
    }
}

/// Parse the thin device ID from a `dmsetup table` output line.
/// Table format: "0 <size> thin <pool_dev> <device_id>"
fn parse_thin_device_id(table: &str, _pool_dev: &str) -> Option<u32> {
    let parts: Vec<&str> = table.trim().split_whitespace().collect();
    // parts[2] == "thin", parts[4] == device_id
    if parts.len() >= 5 && parts[2] == "thin" {
        parts[4].parse().ok()
    } else {
        None
    }
}

/// Compute the expected LVM device-mapper name for the thin pool.
/// LVM doubles dashes in names: "indexify-vg" → "indexify--vg", "thinpool" →
/// "thinpool".
fn lvm_dm_name(config: &LvmConfig) -> String {
    format!(
        "{}-{}",
        config.volume_group.replace('-', "--"),
        config.thin_pool.replace('-', "--")
    )
}

/// Resolve the actual thin-pool dm target device name.
///
/// LVM may layer the thin pool: `<vg>-<pool>` can be a wrapper device
/// while `<vg>-<pool>-tpool` is the real thin-pool target that accepts
/// `create_thin` / `delete` messages. This function checks the dm table
/// to find the device with a `thin-pool` target type.
fn pool_dm_name(config: &LvmConfig) -> String {
    let base = lvm_dm_name(config);

    // Check if the base name itself is a thin-pool target.
    if is_thin_pool_target(&base) {
        return base;
    }

    // LVM often creates a `-tpool` suffix for the internal thin-pool target.
    let tpool = format!("{}-tpool", base);
    if is_thin_pool_target(&tpool) {
        tracing::info!(
            base_name = %base,
            resolved = %tpool,
            "Resolved LVM thin-pool target (base device is a wrapper)"
        );
        return tpool;
    }

    // Fall back to the base name and let the caller handle the error.
    tracing::warn!(
        base_name = %base,
        "Could not verify thin-pool target type, using base dm name"
    );
    base
}

/// Check whether a dm device name has a `thin-pool` target type.
fn is_thin_pool_target(dm_name: &str) -> bool {
    if let Ok(table) = run_cmd("dmsetup", &["table", dm_name]) {
        // Table format: "0 <size> thin-pool <data_dev> <meta_dev> ..."
        table
            .trim()
            .split_whitespace()
            .nth(2)
            .is_some_and(|target_type| target_type == "thin-pool")
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// LVM thin pool validation and dmsetup thin device helpers
// ---------------------------------------------------------------------------

/// Validate that the LVM volume group and thin pool exist, and resolve the
/// actual thin-pool dm device name (populates `config.pool_dm_device`).
pub fn validate_lvm_config(config: &mut LvmConfig) -> Result<()> {
    run_cmd("vgs", &[&config.volume_group])
        .with_context(|| format!("LVM volume group '{}' not found", config.volume_group))?;

    let pool_path = format!("{}/{}", config.volume_group, config.thin_pool);
    run_cmd("lvs", &[&pool_path])
        .with_context(|| format!("LVM thin pool '{}' not found", pool_path))?;

    // Resolve the actual thin-pool target dm name (may differ from the
    // LVM-derived name when LVM layers the pool).
    config.pool_dm_device = pool_dm_name(config);

    tracing::info!(
        volume_group = %config.volume_group,
        thin_pool = %config.thin_pool,
        pool_dm_device = %config.pool_dm_device,
        "LVM config validated"
    );
    Ok(())
}

/// Create a thin device in the pool via dmsetup and activate it.
///
/// Returns the full device path (e.g., `/dev/mapper/indexify-cow-abc123`).
fn create_thin_device(
    config: &LvmConfig,
    dm_name: &str,
    device_id: u32,
    size_sectors: u64,
) -> Result<String> {
    let pool_dev = format!("/dev/mapper/{}", config.pool_dm_device);

    // Allocate a thin device ID in the pool.
    run_cmd(
        "dmsetup",
        &[
            "message",
            &pool_dev,
            "0",
            &format!("create_thin {}", device_id),
        ],
    )
    .with_context(|| {
        format!(
            "Failed to create thin device {} in pool {}",
            device_id, pool_dev
        )
    })?;

    // Activate it as a named dm device.
    let table = format!("0 {} thin {} {}", size_sectors, pool_dev, device_id);
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", dm_name], &table) {
        // Clean up the thin device on failure.
        let _ = run_cmd(
            "dmsetup",
            &["message", &pool_dev, "0", &format!("delete {}", device_id)],
        );
        return Err(e.context(format!(
            "Failed to activate thin device {} as {}",
            device_id, dm_name
        )));
    }

    tracing::debug!(dm_name, device_id, size_sectors, "Thin device created");
    Ok(format!("/dev/mapper/{}", dm_name))
}

/// Remove (deactivate) a thin device and delete it from the pool.
fn remove_thin_device(config: &LvmConfig, dm_name: &str, device_id: u32) -> Result<()> {
    dmsetup_remove_with_retry(dm_name)?;

    let pool_dev = format!("/dev/mapper/{}", config.pool_dm_device);
    run_cmd(
        "dmsetup",
        &["message", &pool_dev, "0", &format!("delete {}", device_id)],
    )
    .with_context(|| format!("Failed to delete thin device {} from pool", device_id))?;

    tracing::debug!(dm_name, device_id, "Thin device removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Origin management
// ---------------------------------------------------------------------------

/// Set up the origin device (idempotent — reuses existing if present).
///
/// Creates a read-only loop device from the base rootfs file and a
/// `linear` dm target on top of it.
pub fn setup_origin(base_rootfs: &Path) -> Result<OriginHandle> {
    let dm_name = "indexify-base".to_string();
    let device_path = PathBuf::from("/dev/mapper/indexify-base");

    // If the origin device already exists, query it and return.
    if device_path.exists() {
        // Find the loop device backing it by parsing dmsetup table.
        let table = run_cmd("dmsetup", &["table", &dm_name])
            .context("Failed to read existing origin dm table")?;
        // Table format: "0 <size> linear <loop_dev> 0"
        let parts: Vec<&str> = table.trim().split_whitespace().collect();
        if parts.len() >= 4 {
            let size_sectors: u64 = parts[1].parse().unwrap_or(0);
            let loop_device = parts[3].to_string();
            tracing::info!(
                dm_name = %dm_name,
                loop_device = %loop_device,
                size_sectors,
                "Reconnected to existing origin device"
            );
            return Ok(OriginHandle {
                dm_name,
                loop_device,
                device_path,
                size_sectors,
            });
        }
        // If we can't parse the table, tear down and recreate.
        tracing::warn!("Cannot parse existing origin dm table, recreating");
        let _ = run_cmd("dmsetup", &["remove", &dm_name]);
    }

    // Set up a read-only loop device for the base rootfs.
    let loop_device = run_cmd(
        "losetup",
        &[
            "--find",
            "--show",
            "--read-only",
            &base_rootfs.to_string_lossy(),
        ],
    )
    .with_context(|| format!("Failed to create loop device for {}", base_rootfs.display()))?
    .trim()
    .to_string();

    // Get the size in sectors.
    let size_str = run_cmd("blockdev", &["--getsz", &loop_device])
        .with_context(|| format!("Failed to get size of {}", loop_device))?;
    let size_sectors: u64 = size_str
        .trim()
        .parse()
        .with_context(|| format!("Invalid sector count from blockdev: {}", size_str.trim()))?;

    // Create the linear dm target.
    let table_line = format!("0 {} linear {} 0", size_sectors, loop_device);
    run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line).with_context(|| {
        format!(
            "Failed to create dm device {} with table: {}",
            dm_name, table_line
        )
    })?;

    tracing::info!(
        dm_name = %dm_name,
        loop_device = %loop_device,
        size_sectors,
        base_rootfs = %base_rootfs.display(),
        "Origin device created"
    );

    Ok(OriginHandle {
        dm_name,
        loop_device,
        device_path,
        size_sectors,
    })
}

/// Tear down the origin device and release its loop device.
pub fn teardown_origin(handle: &OriginHandle) -> Result<()> {
    if Path::new(&handle.device_path).exists() {
        run_cmd("dmsetup", &["remove", &handle.dm_name])
            .with_context(|| format!("Failed to remove dm device {}", handle.dm_name))?;
    }
    run_cmd("losetup", &["-d", &handle.loop_device])
        .with_context(|| format!("Failed to detach loop device {}", handle.loop_device))?;
    tracing::info!(dm_name = %handle.dm_name, "Origin device torn down");
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-VM snapshot management
// ---------------------------------------------------------------------------

/// Create a new dm-snapshot for a VM using a dmsetup thin device as the COW
/// device.
pub fn create_snapshot(
    origin: &OriginHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    cow_size_bytes: u64,
    allocator: &ThinDeviceAllocator,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));
    let cow_dm_name = format!("indexify-cow-{}", vm_id);
    let size_sectors = cow_size_bytes / 512;
    let device_id = allocator.allocate();

    // Create a thin device for the COW data.
    let cow_device = match create_thin_device(lvm_config, &cow_dm_name, device_id, size_sectors) {
        Ok(path) => path,
        Err(e) => {
            return Err(e.context(format!("Failed to create COW thin device for VM {}", vm_id)));
        }
    };

    // Create the snapshot dm target.
    // Table: "0 <size> snapshot <origin_dev> <cow_dev> P <chunk_size>"
    // P = persistent snapshot, 8 = chunk size in sectors (4KB)
    let table_line = format!(
        "0 {} snapshot {} {} P 8",
        origin.size_sectors,
        origin.device_path.display(),
        cow_device,
    );
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line) {
        // Clean up thin device on failure.
        let _ = remove_thin_device(lvm_config, &cow_dm_name, device_id);
        return Err(e.context(format!("Failed to create dm-snapshot {}", dm_name)));
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_device = %cow_device,
        thin_device_id = device_id,
        "dm-snapshot created for VM"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_dm_name,
        cow_device,
        thin_device_id: device_id,
    })
}

/// Create a dm-snapshot from a restored COW file (snapshot restore path).
///
/// Creates a thin device, copies the restored COW data into it via `dd`,
/// then creates the dm-snapshot target. The temporary COW file is deleted
/// after the data is copied.
pub fn create_snapshot_from_cow(
    origin: &OriginHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    cow_file: &Path,
    allocator: &ThinDeviceAllocator,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));
    let cow_dm_name = format!("indexify-cow-{}", vm_id);

    // Get the size of the COW file to create an appropriately sized thin device.
    let cow_size = std::fs::metadata(cow_file)
        .with_context(|| format!("Failed to stat COW file {}", cow_file.display()))?
        .len();
    let size_sectors = cow_size / 512;
    let device_id = allocator.allocate();

    // Create the thin device.
    let cow_device = create_thin_device(lvm_config, &cow_dm_name, device_id, size_sectors)?;

    // Copy the COW data into the thin device.
    if let Err(e) = run_cmd(
        "dd",
        &[
            &format!("if={}", cow_file.display()),
            &format!("of={}", cow_device),
            "bs=4M",
            "conv=fdatasync",
        ],
    ) {
        let _ = remove_thin_device(lvm_config, &cow_dm_name, device_id);
        return Err(e.context(format!("Failed to dd COW data into {}", cow_device)));
    }

    // Delete the temp COW file now that data is in the thin device.
    let _ = std::fs::remove_file(cow_file);

    // Create the snapshot dm target.
    let table_line = format!(
        "0 {} snapshot {} {} P 8",
        origin.size_sectors,
        origin.device_path.display(),
        cow_device,
    );
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line) {
        let _ = remove_thin_device(lvm_config, &cow_dm_name, device_id);
        return Err(e.context(format!(
            "Failed to create dm-snapshot {} from restored COW",
            dm_name
        )));
    }

    // Log snapshot status for diagnostics — check if persistent exceptions
    // were loaded from the restored COW data.
    match run_cmd("dmsetup", &["status", &dm_name]) {
        Ok(status) => tracing::info!(
            dm_name = %dm_name,
            status = %status.trim(),
            "Restored dm-snapshot status"
        ),
        Err(e) => tracing::warn!(
            dm_name = %dm_name,
            error = ?e,
            "Failed to query restored dm-snapshot status"
        ),
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_device = %cow_device,
        thin_device_id = device_id,
        cow_size_bytes = cow_size,
        "dm-snapshot created from restored COW file"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_dm_name,
        cow_device,
        thin_device_id: device_id,
    })
}

/// Maximum retries for `dmsetup remove` when the device is momentarily busy
/// (e.g. kernel still releasing file descriptors after process kill).
const DM_REMOVE_RETRIES: u32 = 5;
const DM_REMOVE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(200);

/// Remove a dm device with retries for transient "Device or resource busy".
fn dmsetup_remove_with_retry(dm_name: &str) -> Result<()> {
    let device_path = format!("/dev/mapper/{}", dm_name);
    if !Path::new(&device_path).exists() {
        return Ok(());
    }
    for attempt in 0..DM_REMOVE_RETRIES {
        match run_cmd("dmsetup", &["remove", dm_name]) {
            Ok(_) => return Ok(()),
            Err(_e) if attempt + 1 < DM_REMOVE_RETRIES => {
                tracing::debug!(
                    dm_name,
                    attempt = attempt + 1,
                    "dmsetup remove busy, retrying"
                );
                std::thread::sleep(DM_REMOVE_RETRY_DELAY);
                // Re-check existence in case another cleanup removed it.
                if !Path::new(&device_path).exists() {
                    return Ok(());
                }
                continue;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("Failed to remove dm device {}", dm_name));
            }
        }
    }
    Ok(())
}

/// Destroy a VM's dm-snapshot and remove the COW thin device.
pub fn destroy_snapshot(handle: &SnapshotHandle, lvm_config: &LvmConfig) -> Result<()> {
    // Remove the snapshot dm device first.
    dmsetup_remove_with_retry(&handle.dm_name)?;

    // Remove the COW thin device (deactivate + delete from pool).
    if let Err(e) = remove_thin_device(lvm_config, &handle.cow_dm_name, handle.thin_device_id) {
        tracing::warn!(
            cow_dm_name = %handle.cow_dm_name,
            thin_device_id = handle.thin_device_id,
            error = ?e,
            "Failed to remove COW thin device"
        );
    }

    tracing::info!(
        dm_name = %handle.dm_name,
        thin_device_id = handle.thin_device_id,
        "dm-snapshot destroyed"
    );

    Ok(())
}

/// Destroy a VM's dm-snapshot by its metadata fields (used during recovery
/// cleanup when we don't have a SnapshotHandle).
pub fn destroy_snapshot_by_parts(
    dm_name: &str,
    lvm_config: &LvmConfig,
    thin_device_id: u32,
) -> Result<()> {
    dmsetup_remove_with_retry(dm_name)?;

    let vm_id = dm_name.strip_prefix("indexify-vm-").unwrap_or(dm_name);
    let cow_dm_name = format!("indexify-cow-{}", vm_id);
    if let Err(e) = remove_thin_device(lvm_config, &cow_dm_name, thin_device_id) {
        tracing::warn!(
            cow_dm_name = %cow_dm_name,
            thin_device_id = thin_device_id,
            error = ?e,
            "Failed to remove COW thin device (by parts)"
        );
    }

    tracing::info!(
        dm_name = %dm_name,
        thin_device_id = thin_device_id,
        "dm-snapshot destroyed (by parts)"
    );

    Ok(())
}

/// Suspend a dm-snapshot device, flushing all pending I/O to the COW device.
///
/// After suspension the COW device is consistent and can be read
/// directly. The device must be resumed or removed afterwards.
pub fn suspend_snapshot(dm_name: &str) -> Result<()> {
    // Log status before suspend to see how many sectors are allocated.
    match run_cmd("dmsetup", &["status", dm_name]) {
        Ok(status) => tracing::info!(
            dm_name = %dm_name,
            status = %status.trim(),
            "dm-snapshot status before suspend"
        ),
        Err(e) => tracing::warn!(dm_name, error = ?e, "Failed to query dm-snapshot status"),
    }

    run_cmd("dmsetup", &["suspend", dm_name])
        .with_context(|| format!("Failed to suspend dm device {}", dm_name))?;
    tracing::info!(dm_name = %dm_name, "dm-snapshot suspended (COW flushed)");
    Ok(())
}

/// Async version of suspend_snapshot.
pub async fn suspend_snapshot_async(dm_name: String) -> Result<()> {
    tokio::task::spawn_blocking(move || suspend_snapshot(&dm_name))
        .await
        .context("suspend_snapshot task panicked")?
}

/// Resume a previously suspended dm-snapshot device.
pub fn resume_snapshot(dm_name: &str) -> Result<()> {
    run_cmd("dmsetup", &["resume", dm_name])
        .with_context(|| format!("Failed to resume dm device {}", dm_name))?;
    tracing::info!(dm_name = %dm_name, "dm-snapshot resumed");
    Ok(())
}

/// Async version of resume_snapshot.
pub async fn resume_snapshot_async(dm_name: String) -> Result<()> {
    tokio::task::spawn_blocking(move || resume_snapshot(&dm_name))
        .await
        .context("resume_snapshot task panicked")?
}

/// Check if a dm-snapshot target exists for a VM.
#[allow(dead_code)]
pub fn snapshot_exists(vm_id: &str) -> bool {
    Path::new(&format!("/dev/mapper/indexify-vm-{}", vm_id)).exists()
}

/// Get the status of a dm-snapshot (sectors allocated / total).
#[allow(dead_code)]
pub fn snapshot_status(vm_id: &str) -> Result<SnapshotStatus> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let status = run_cmd("dmsetup", &["status", &dm_name])
        .with_context(|| format!("Failed to get status for {}", dm_name))?;

    // Status format: "0 <size> snapshot <allocated>/<total> <metadata_sectors>"
    let parts: Vec<&str> = status.trim().split_whitespace().collect();
    if parts.len() < 4 {
        bail!("Unexpected dmsetup status format: {}", status.trim());
    }

    // The fraction is at parts[3]: "X/Y"
    let fraction = parts[3];
    let slash_parts: Vec<&str> = fraction.split('/').collect();
    if slash_parts.len() != 2 {
        bail!("Cannot parse snapshot fraction: {}", fraction);
    }

    let sectors_allocated: u64 = slash_parts[0]
        .parse()
        .with_context(|| format!("Invalid allocated sectors: {}", slash_parts[0]))?;
    let sectors_total: u64 = slash_parts[1]
        .parse()
        .with_context(|| format!("Invalid total sectors: {}", slash_parts[1]))?;

    Ok(SnapshotStatus {
        sectors_allocated,
        sectors_total,
    })
}

/// Clean up stale `indexify-vm-*` dm devices and orphaned `indexify-cow-*`
/// thin LVs from a previous run.
///
/// Only removes VM snapshot devices whose VM ID is NOT in `active_vm_ids`.
/// Does NOT touch `indexify-base` — the origin is managed by
/// `setup_origin()` / `teardown_origin()`.
pub fn cleanup_stale_devices(active_vm_ids: &HashSet<String>, lvm_config: &LvmConfig) {
    // Clean up legacy thin-provisioning devices first (order matters:
    // thin volumes before pool, pool before data/meta).
    let legacy_prefixes = ["indexify-thin-", "indexify-tpool"];
    if let Ok(output) = run_cmd("dmsetup", &["ls"]) {
        // Remove thin volumes first, then pool, then data/meta.
        let mut legacy_devs: Vec<String> = output
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|name| legacy_prefixes.iter().any(|p| name.starts_with(p)))
            .map(|s| s.to_string())
            .collect();
        // Sort so "indexify-thin-*" comes before "indexify-tpool" (thin
        // volumes must be removed before the pool they belong to).
        legacy_devs.sort();
        for name in &legacy_devs {
            if run_cmd("dmsetup", &["remove", name]).is_ok() {
                tracing::info!(dm_name = %name, "Removed legacy thin-provisioning device");
            }
        }
    }

    // Clean up stale dm-snapshot devices.
    let mut stale_devs = Vec::new();
    if let Ok(output) = run_cmd("dmsetup", &["ls"]) {
        for line in output.lines() {
            if let Some(name) = line.split_whitespace().next() {
                if let Some(vm_id) = name.strip_prefix("indexify-vm-") {
                    if !active_vm_ids.contains(vm_id) {
                        stale_devs.push(name.to_string());
                    }
                }
            }
        }
    }

    for name in &stale_devs {
        // Remove the dm device (retry up to 3 times for busy devices).
        let mut removed = false;
        for _ in 0..3 {
            match run_cmd("dmsetup", &["remove", name]) {
                Ok(_) => {
                    removed = true;
                    break;
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }

        if removed {
            tracing::info!(
                dm_name = %name,
                "Cleaned up stale dm-snapshot device"
            );
        } else {
            tracing::warn!(
                dm_name = %name,
                "Failed to remove stale dm-snapshot device after retries"
            );
        }
    }

    if !stale_devs.is_empty() {
        tracing::info!(
            count = stale_devs.len(),
            "Stale dm-snapshot cleanup complete"
        );
    }

    // Clean up orphaned indexify-cow-* dm devices not associated with active VMs.
    let pool_dev = format!("/dev/mapper/{}", lvm_config.pool_dm_device);
    if let Ok(output) = run_cmd("dmsetup", &["ls"]) {
        for line in output.lines() {
            if let Some(name) = line.split_whitespace().next() {
                if let Some(vm_id) = name.strip_prefix("indexify-cow-") {
                    if !active_vm_ids.contains(vm_id) {
                        // Parse the thin table to get the device ID for deletion.
                        let device_id = run_cmd("dmsetup", &["table", name])
                            .ok()
                            .and_then(|table| parse_thin_device_id(&table, &pool_dev));

                        // Deactivate the dm device.
                        if let Err(e) = dmsetup_remove_with_retry(name) {
                            tracing::warn!(
                                dm_name = %name,
                                error = ?e,
                                "Failed to deactivate orphaned COW dm device"
                            );
                            continue;
                        }

                        // Delete the thin device from the pool.
                        if let Some(id) = device_id {
                            if let Err(e) = run_cmd(
                                "dmsetup",
                                &["message", &pool_dev, "0", &format!("delete {}", id)],
                            ) {
                                tracing::warn!(
                                    dm_name = %name,
                                    device_id = id,
                                    error = ?e,
                                    "Failed to delete orphaned thin device from pool"
                                );
                            }
                        }

                        tracing::info!(
                            dm_name = %name,
                            device_id = ?device_id,
                            "Removed orphaned COW thin device"
                        );
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Command helpers
// ---------------------------------------------------------------------------

/// Run a command and capture stdout. Returns error if exit code != 0.
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String> {
    let output = std::process::Command::new(cmd)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .with_context(|| format!("Failed to execute: {} {:?}", cmd, args))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} {:?} failed (exit {}): {}",
            cmd,
            args,
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// Run a command with stdin input and capture stdout.
fn run_cmd_stdin(cmd: &str, args: &[&str], stdin_data: &str) -> Result<String> {
    use std::io::Write;
    let mut child = std::process::Command::new(cmd)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to spawn: {} {:?}", cmd, args))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(stdin_data.as_bytes())
            .with_context(|| format!("Failed to write stdin to {} {:?}", cmd, args))?;
    }

    let output = child
        .wait_with_output()
        .with_context(|| format!("Failed to wait for: {} {:?}", cmd, args))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "{} {:?} failed (exit {}): {}",
            cmd,
            args,
            output.status.code().unwrap_or(-1),
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

// ---------------------------------------------------------------------------
// Async variants for use from tokio context
// ---------------------------------------------------------------------------

/// Async version of create_snapshot (runs blocking IO on spawn_blocking).
pub async fn create_snapshot_async(
    origin_dm_name: String,
    origin_device_path: PathBuf,
    origin_size_sectors: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    cow_size_bytes: u64,
    allocator: Arc<ThinDeviceAllocator>,
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(), // not needed for snapshot creation
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot(&origin, &vm_id, &lvm_config, cow_size_bytes, &allocator)
    })
    .await
    .context("create_snapshot task panicked")?
}

/// Async version of create_snapshot_from_cow.
pub async fn create_snapshot_from_cow_async(
    origin_dm_name: String,
    origin_device_path: PathBuf,
    origin_size_sectors: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    cow_file: PathBuf,
    allocator: Arc<ThinDeviceAllocator>,
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(),
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot_from_cow(&origin, &vm_id, &lvm_config, &cow_file, &allocator)
    })
    .await
    .context("create_snapshot_from_cow task panicked")?
}

/// Async version of destroy_snapshot.
pub async fn destroy_snapshot_async(handle: SnapshotHandle, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || destroy_snapshot(&handle, &lvm_config))
        .await
        .context("destroy_snapshot task panicked")?
}

/// Async version of destroy_snapshot_by_parts.
pub async fn destroy_snapshot_by_parts_async(
    dm_name: String,
    lvm_config: LvmConfig,
    thin_device_id: u32,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        destroy_snapshot_by_parts(&dm_name, &lvm_config, thin_device_id)
    })
    .await
    .context("destroy_snapshot_by_parts task panicked")?
}
