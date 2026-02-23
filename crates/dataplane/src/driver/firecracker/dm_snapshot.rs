//! dm-snapshot based volume management for Firecracker rootfs.
//!
//! Uses LVM thin provisioning for per-VM COW devices. Each VM gets a thin LV
//! as its COW device, eliminating file+loop overhead and loop device leaks.
//!
//! Architecture:
//! ```text
//! Base rootfs file (/opt/firecracker/rootfs.ext4)
//!   → losetup --read-only → /dev/loopN
//!   → dmsetup create indexify-base  "0 $SZ linear /dev/loopN 0"
//!
//! Per-VM:
//!   lvcreate -V {size} -T {vg}/{pool} -n indexify-cow-{vm_id}
//!   → dmsetup create indexify-vm-{vm_id}  "0 $SZ snapshot /dev/mapper/indexify-base /dev/{vg}/indexify-cow-{vm_id} P 8"
//! ```
//!
//! All device-mapper operations use `dmsetup` and `losetup` commands —
//! no `libdevmapper-dev` build dependency needed.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

/// LVM thin pool configuration for per-VM COW devices.
#[derive(Debug, Clone)]
pub struct LvmConfig {
    /// LVM volume group name (e.g., "indexify-vg").
    pub volume_group: String,
    /// LVM thin pool LV name within the volume group (e.g., "thinpool").
    pub thin_pool: String,
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
    /// COW device path (e.g., "/dev/indexify-vg/indexify-cow-abc123").
    pub cow_device: String,
    /// LV name for the COW thin LV (e.g., "indexify-cow-abc123").
    pub lv_name: String,
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
// LVM thin provisioning helpers
// ---------------------------------------------------------------------------

/// Validate that the LVM volume group and thin pool exist.
pub fn validate_lvm_config(config: &LvmConfig) -> Result<()> {
    run_cmd("vgs", &[&config.volume_group])
        .with_context(|| format!("LVM volume group '{}' not found", config.volume_group))?;

    let pool_path = format!("{}/{}", config.volume_group, config.thin_pool);
    run_cmd("lvs", &[&pool_path])
        .with_context(|| format!("LVM thin pool '{}' not found", pool_path))?;

    tracing::info!(
        volume_group = %config.volume_group,
        thin_pool = %config.thin_pool,
        "LVM config validated"
    );
    Ok(())
}

/// Create a thin LV in the configured thin pool.
fn create_thin_lv(config: &LvmConfig, lv_name: &str, size_bytes: u64) -> Result<()> {
    let pool_path = format!("{}/{}", config.volume_group, config.thin_pool);
    let size_arg = format!("{}B", size_bytes);
    run_cmd(
        "lvcreate",
        &["-V", &size_arg, "-T", &pool_path, "-n", lv_name],
    )
    .with_context(|| {
        format!(
            "Failed to create thin LV {}/{}",
            config.volume_group, lv_name
        )
    })?;
    tracing::debug!(lv_name = %lv_name, size_bytes, "Thin LV created");
    Ok(())
}

/// Remove a thin LV.
fn remove_thin_lv(config: &LvmConfig, lv_name: &str) -> Result<()> {
    let lv_path = format!("{}/{}", config.volume_group, lv_name);
    run_cmd("lvremove", &["-f", &lv_path])
        .with_context(|| format!("Failed to remove thin LV {}", lv_path))?;
    tracing::debug!(lv_name = %lv_name, "Thin LV removed");
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

/// Create a new dm-snapshot for a VM using a thin LV as the COW device.
pub fn create_snapshot(
    origin: &OriginHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    cow_size_bytes: u64,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));
    let lv_name = format!("indexify-cow-{}", vm_id);
    let cow_device = format!("/dev/{}/{}", lvm_config.volume_group, lv_name);

    // Create a thin LV for the COW device.
    create_thin_lv(lvm_config, &lv_name, cow_size_bytes)?;

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
        // Clean up thin LV on failure.
        let _ = remove_thin_lv(lvm_config, &lv_name);
        return Err(e.context(format!("Failed to create dm-snapshot {}", dm_name)));
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_device = %cow_device,
        lv_name = %lv_name,
        "dm-snapshot created for VM"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_device,
        lv_name,
    })
}

/// Create a dm-snapshot from a restored COW file (snapshot restore path).
///
/// Creates a thin LV, copies the restored COW data into it via `dd`,
/// then creates the dm-snapshot target. The temporary COW file is deleted
/// after the data is copied.
pub fn create_snapshot_from_cow(
    origin: &OriginHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    cow_file: &Path,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));
    let lv_name = format!("indexify-cow-{}", vm_id);
    let cow_device = format!("/dev/{}/{}", lvm_config.volume_group, lv_name);

    // Get the size of the COW file to create an appropriately sized thin LV.
    let cow_size = std::fs::metadata(cow_file)
        .with_context(|| format!("Failed to stat COW file {}", cow_file.display()))?
        .len();

    // Create the thin LV.
    create_thin_lv(lvm_config, &lv_name, cow_size)?;

    // Copy the COW data into the thin LV.
    if let Err(e) = run_cmd(
        "dd",
        &[
            &format!("if={}", cow_file.display()),
            &format!("of={}", cow_device),
            "bs=4M",
            "conv=fdatasync",
        ],
    ) {
        let _ = remove_thin_lv(lvm_config, &lv_name);
        return Err(e.context(format!("Failed to dd COW data into {}", cow_device)));
    }

    // Delete the temp COW file now that data is in the LV.
    let _ = std::fs::remove_file(cow_file);

    // Create the snapshot dm target.
    let table_line = format!(
        "0 {} snapshot {} {} P 8",
        origin.size_sectors,
        origin.device_path.display(),
        cow_device,
    );
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line) {
        let _ = remove_thin_lv(lvm_config, &lv_name);
        return Err(e.context(format!(
            "Failed to create dm-snapshot {} from restored COW",
            dm_name
        )));
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_device = %cow_device,
        lv_name = %lv_name,
        "dm-snapshot created from restored COW file"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_device,
        lv_name,
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

/// Destroy a VM's dm-snapshot and remove the COW thin LV.
pub fn destroy_snapshot(handle: &SnapshotHandle, lvm_config: &LvmConfig) -> Result<()> {
    dmsetup_remove_with_retry(&handle.dm_name)?;

    // Remove the COW thin LV.
    if let Err(e) = remove_thin_lv(lvm_config, &handle.lv_name) {
        tracing::warn!(
            lv_name = %handle.lv_name,
            error = ?e,
            "Failed to remove COW thin LV"
        );
    }

    tracing::info!(
        dm_name = %handle.dm_name,
        lv_name = %handle.lv_name,
        "dm-snapshot destroyed"
    );

    Ok(())
}

/// Destroy a VM's dm-snapshot by its metadata fields (used during recovery
/// cleanup when we don't have a SnapshotHandle).
pub fn destroy_snapshot_by_parts(
    dm_name: &str,
    lv_name: &str,
    lvm_config: &LvmConfig,
) -> Result<()> {
    dmsetup_remove_with_retry(dm_name)?;

    if !lv_name.is_empty() {
        if let Err(e) = remove_thin_lv(lvm_config, lv_name) {
            tracing::warn!(
                lv_name = %lv_name,
                error = ?e,
                "Failed to remove COW thin LV (by parts)"
            );
        }
    }

    tracing::info!(
        dm_name = %dm_name,
        lv_name = %lv_name,
        "dm-snapshot destroyed (by parts)"
    );

    Ok(())
}

/// Suspend a dm-snapshot device, flushing all pending I/O to the COW device.
///
/// After suspension the COW device is consistent and can be read
/// directly. The device must be resumed or removed afterwards.
pub fn suspend_snapshot(dm_name: &str) -> Result<()> {
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

    // Clean up orphaned indexify-cow-* thin LVs not associated with active VMs.
    let vg = &lvm_config.volume_group;
    if let Ok(output) = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]) {
        for line in output.lines() {
            let lv_name = line.trim();
            if let Some(vm_id) = lv_name.strip_prefix("indexify-cow-") {
                if !active_vm_ids.contains(vm_id) {
                    if let Err(e) = remove_thin_lv(lvm_config, lv_name) {
                        tracing::warn!(
                            lv_name = %lv_name,
                            error = ?e,
                            "Failed to remove orphaned COW thin LV"
                        );
                    } else {
                        tracing::info!(
                            lv_name = %lv_name,
                            "Removed orphaned COW thin LV"
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
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(), // not needed for snapshot creation
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot(&origin, &vm_id, &lvm_config, cow_size_bytes)
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
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(),
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot_from_cow(&origin, &vm_id, &lvm_config, &cow_file)
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
    lv_name: String,
    lvm_config: LvmConfig,
) -> Result<()> {
    tokio::task::spawn_blocking(move || destroy_snapshot_by_parts(&dm_name, &lv_name, &lvm_config))
        .await
        .context("destroy_snapshot_by_parts task panicked")?
}
