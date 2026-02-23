//! dm-snapshot based volume management for Firecracker rootfs.
//!
//! Replaces dm-thin provisioning with dm-snapshot, which stores CoW data in
//! an explicit COW file per VM. This makes snapshot/restore trivial — the COW
//! file IS the delta.
//!
//! Architecture:
//! ```text
//! Base rootfs file (/opt/firecracker/rootfs.ext4)
//!   → losetup --read-only → /dev/loopN
//!   → dmsetup create indexify-base  "0 $SZ linear /dev/loopN 0"
//!
//! Per-VM:
//!   COW file: {overlay_dir}/{vm_id}.cow  (pre-allocated)
//!   → losetup → /dev/loopM
//!   → dmsetup create indexify-vm-{vm_id}  "0 $SZ snapshot /dev/mapper/indexify-base /dev/loopM P 8"
//! ```
//!
//! All device-mapper operations use `dmsetup` and `losetup` commands —
//! no `libdevmapper-dev` build dependency needed.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

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
pub struct SnapshotHandle {
    /// dm device name (e.g., "indexify-vm-abc123").
    pub dm_name: String,
    /// Full device path (e.g., "/dev/mapper/indexify-vm-abc123").
    pub device_path: PathBuf,
    /// Path to the COW file on the host filesystem.
    pub cow_file: PathBuf,
    /// Loop device backing the COW file.
    pub loop_device: String,
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

/// Create a new dm-snapshot for a VM.
///
/// Allocates a COW file, sets up a loop device for it, and creates a
/// `snapshot` dm target that uses the origin as the base.
pub fn create_snapshot(
    origin: &OriginHandle,
    vm_id: &str,
    overlay_dir: &Path,
    cow_size_bytes: u64,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));
    let cow_file = overlay_dir.join(format!("{}.cow", vm_id));

    // Pre-allocate the COW file.
    run_cmd(
        "fallocate",
        &[
            "-l",
            &cow_size_bytes.to_string(),
            &cow_file.to_string_lossy(),
        ],
    )
    .with_context(|| format!("Failed to allocate COW file {}", cow_file.display()))?;

    // Create a loop device for the COW file.
    let loop_device = run_cmd(
        "losetup",
        &["--find", "--show", &cow_file.to_string_lossy()],
    )
    .with_context(|| format!("Failed to create loop device for {}", cow_file.display()))?
    .trim()
    .to_string();

    // Create the snapshot dm target.
    // Table: "0 <size> snapshot <origin_dev> <cow_dev> P <chunk_size>"
    // P = persistent snapshot, 8 = chunk size in sectors (4KB)
    let table_line = format!(
        "0 {} snapshot {} {} P 8",
        origin.size_sectors,
        origin.device_path.display(),
        loop_device,
    );
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line) {
        // Clean up loop device and COW file on failure.
        let _ = run_cmd("losetup", &["-d", &loop_device]);
        let _ = std::fs::remove_file(&cow_file);
        return Err(e.context(format!("Failed to create dm-snapshot {}", dm_name)));
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_file = %cow_file.display(),
        loop_device = %loop_device,
        "dm-snapshot created for VM"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_file,
        loop_device,
    })
}

/// Create a dm-snapshot from a restored COW file (snapshot restore path).
///
/// The COW file already exists (downloaded from blob store). We just set up
/// the loop device and dm target.
pub fn create_snapshot_from_cow(
    origin: &OriginHandle,
    vm_id: &str,
    cow_file: &Path,
) -> Result<SnapshotHandle> {
    let dm_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/mapper/{}", dm_name));

    // Create a loop device for the existing COW file.
    let loop_device = run_cmd(
        "losetup",
        &["--find", "--show", &cow_file.to_string_lossy()],
    )
    .with_context(|| format!("Failed to create loop device for {}", cow_file.display()))?
    .trim()
    .to_string();

    // Create the snapshot dm target.
    let table_line = format!(
        "0 {} snapshot {} {} P 8",
        origin.size_sectors,
        origin.device_path.display(),
        loop_device,
    );
    if let Err(e) = run_cmd_stdin("dmsetup", &["create", &dm_name], &table_line) {
        let _ = run_cmd("losetup", &["-d", &loop_device]);
        return Err(e.context(format!(
            "Failed to create dm-snapshot {} from restored COW",
            dm_name
        )));
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_file = %cow_file.display(),
        loop_device = %loop_device,
        "dm-snapshot created from restored COW file"
    );

    Ok(SnapshotHandle {
        dm_name,
        device_path,
        cow_file: cow_file.to_path_buf(),
        loop_device,
    })
}

/// Destroy a VM's dm-snapshot and clean up loop device and COW file.
pub fn destroy_snapshot(handle: &SnapshotHandle) -> Result<()> {
    // Remove the dm device.
    if Path::new(&handle.device_path).exists() {
        run_cmd("dmsetup", &["remove", &handle.dm_name])
            .with_context(|| format!("Failed to remove dm device {}", handle.dm_name))?;
    }

    // Detach the loop device.
    if !handle.loop_device.is_empty() {
        let _ = run_cmd("losetup", &["-d", &handle.loop_device]);
    }

    // Remove the COW file.
    if handle.cow_file.exists() {
        let _ = std::fs::remove_file(&handle.cow_file);
    }

    tracing::info!(
        dm_name = %handle.dm_name,
        cow_file = %handle.cow_file.display(),
        "dm-snapshot destroyed"
    );

    Ok(())
}

/// Destroy a VM's dm-snapshot by its metadata fields (used during recovery
/// cleanup when we don't have a SnapshotHandle).
pub fn destroy_snapshot_by_parts(dm_name: &str, loop_device: &str, cow_file: &str) -> Result<()> {
    let device_path = format!("/dev/mapper/{}", dm_name);
    if Path::new(&device_path).exists() {
        run_cmd("dmsetup", &["remove", dm_name])
            .with_context(|| format!("Failed to remove dm device {}", dm_name))?;
    }

    if !loop_device.is_empty() {
        let _ = run_cmd("losetup", &["-d", loop_device]);
    }

    if !cow_file.is_empty() {
        let _ = std::fs::remove_file(cow_file);
    }

    tracing::info!(
        dm_name = %dm_name,
        cow_file = %cow_file,
        "dm-snapshot destroyed (by parts)"
    );

    Ok(())
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

/// Clean up stale `indexify-vm-*` dm devices from a previous run.
///
/// Only removes VM snapshot devices whose VM ID is NOT in `active_vm_ids`.
/// For each removed device, also detaches the COW loop device to prevent
/// loop device leaks. Does NOT touch `indexify-base` — the origin is
/// managed by `setup_origin()` / `teardown_origin()`.
pub fn cleanup_stale_devices(active_vm_ids: &HashSet<String>) {
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
        // Parse dmsetup table to find the COW loop device before removing.
        // Table format: "0 <size> snapshot <origin_dev> <cow_loop_dev> P <chunk>"
        let cow_loop = run_cmd("dmsetup", &["table", name]).ok().and_then(|table| {
            let parts: Vec<&str> = table.trim().split_whitespace().collect();
            // For a snapshot target, the COW device is at index 4.
            if parts.len() >= 5 && parts[2] == "snapshot" {
                Some(parts[4].to_string())
            } else {
                None
            }
        });

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
            // Detach the COW loop device to prevent leaks.
            if let Some(ref loop_dev) = cow_loop {
                let _ = run_cmd("losetup", &["-d", loop_dev]);
            }
            tracing::info!(
                dm_name = %name,
                cow_loop = ?cow_loop,
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
    overlay_dir: PathBuf,
    cow_size_bytes: u64,
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(), // not needed for snapshot creation
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot(&origin, &vm_id, &overlay_dir, cow_size_bytes)
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
    cow_file: PathBuf,
) -> Result<SnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let origin = OriginHandle {
            dm_name: origin_dm_name,
            loop_device: String::new(),
            device_path: origin_device_path,
            size_sectors: origin_size_sectors,
        };
        create_snapshot_from_cow(&origin, &vm_id, &cow_file)
    })
    .await
    .context("create_snapshot_from_cow task panicked")?
}

/// Async version of destroy_snapshot.
pub async fn destroy_snapshot_async(handle: SnapshotHandle) -> Result<()> {
    tokio::task::spawn_blocking(move || destroy_snapshot(&handle))
        .await
        .context("destroy_snapshot task panicked")?
}

/// Async version of destroy_snapshot_by_parts.
pub async fn destroy_snapshot_by_parts_async(
    dm_name: String,
    loop_device: String,
    cow_file: String,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        destroy_snapshot_by_parts(&dm_name, &loop_device, &cow_file)
    })
    .await
    .context("destroy_snapshot_by_parts task panicked")?
}
