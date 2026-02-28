//! dm-thin native snapshot volume management for Firecracker rootfs.
//!
//! Uses LVM thin provisioning's native snapshot capability. The base rootfs
//! image is imported into a thin LV once, and each VM gets a thin snapshot
//! of that LV. Thin snapshots can be resized beyond the base image size,
//! giving per-VM disk sizing with COW block sharing.
//!
//! Architecture:
//! ```text
//! Base rootfs file (/opt/firecracker/rootfs.ext4)
//!   → dd into thin LV "indexify-base" (one-time import)
//!
//! Per-VM:
//!   lvcreate --snapshot {vg}/indexify-base --name indexify-vm-{vm_id}
//!   → optional lvresize + resize2fs for larger disks
//!   Device: /dev/{vg}/indexify-vm-{vm_id}
//! ```
//!
//! No loop devices, no `dmsetup create`, no separate snapshot devices.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, bail};

/// LVM thin pool configuration for per-VM thin snapshots.
#[derive(Debug, Clone)]
pub struct LvmConfig {
    /// LVM volume group name (e.g., "indexify-vg").
    pub volume_group: String,
    /// LVM thin pool LV name within the volume group (e.g., "thinpool").
    pub thin_pool: String,
}

/// Handle to the shared base image thin LV.
pub struct BaseImageHandle {
    /// LV name (e.g., "indexify-base").
    pub lv_name: String,
    /// Full device path (e.g., "/dev/{vg}/indexify-base").
    pub device_path: PathBuf,
    /// Size of the base image in bytes.
    pub size_bytes: u64,
}

/// Handle to a per-VM thin snapshot LV.
pub struct ThinSnapshotHandle {
    /// LV name (e.g., "indexify-vm-abc123").
    pub lv_name: String,
    /// Full device path (e.g., "/dev/{vg}/indexify-vm-abc123").
    pub device_path: PathBuf,
    /// Size of the thin LV in bytes.
    pub size_bytes: u64,
}

// ---------------------------------------------------------------------------
// LVM validation
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

// ---------------------------------------------------------------------------
// Base image management
// ---------------------------------------------------------------------------

/// Set up the base image thin LV (idempotent — reuses existing if size
/// matches).
///
/// Imports the base rootfs file into a thin LV named "indexify-base".
/// If the LV already exists and matches the base image size, it is reused.
/// If the LV exists but has a different size, it is recreated.
pub fn setup_base_image(base_rootfs: &Path, lvm_config: &LvmConfig) -> Result<BaseImageHandle> {
    let lv_name = "indexify-base".to_string();
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));

    let base_size = std::fs::metadata(base_rootfs)
        .with_context(|| format!("Failed to stat base rootfs {}", base_rootfs.display()))?
        .len();

    // Check if the LV already exists.
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    if let Ok(output) = run_cmd(
        "lvs",
        &[
            "--noheadings",
            "--nosuffix",
            "--units",
            "b",
            "-o",
            "lv_size",
            &lv_path,
        ],
    ) {
        let existing_size: u64 = output.trim().parse().unwrap_or(0);
        // LVM rounds LV sizes up to extent boundaries (typically 4MB).
        // Accept any existing LV that is at least as large as the base image.
        if existing_size >= base_size {
            tracing::info!(
                lv_name = %lv_name,
                size_bytes = existing_size,
                "Reusing existing base image thin LV"
            );
            return Ok(BaseImageHandle {
                lv_name,
                device_path,
                size_bytes: existing_size,
            });
        }
        // Existing LV is too small — remove and recreate.
        tracing::info!(
            lv_name = %lv_name,
            existing_size,
            new_size = base_size,
            "Base image size changed, recreating thin LV"
        );
        let _ = run_cmd("lvremove", &["-f", &lv_path]);
    }

    // Create the thin LV.
    let pool_path = format!("{}/{}", lvm_config.volume_group, lvm_config.thin_pool);
    let size_arg = format!("{}B", base_size);
    run_cmd(
        "lvcreate",
        &["-V", &size_arg, "-T", &pool_path, "-n", &lv_name],
    )
    .with_context(|| format!("Failed to create base image thin LV {}", lv_path))?;

    // Import the base rootfs into the thin LV.
    run_cmd(
        "dd",
        &[
            &format!("if={}", base_rootfs.display()),
            &format!("of={}", device_path.display()),
            "bs=4M",
        ],
    )
    .with_context(|| format!("Failed to dd base rootfs into {}", device_path.display()))?;

    tracing::info!(
        lv_name = %lv_name,
        size_bytes = base_size,
        base_rootfs = %base_rootfs.display(),
        "Base image thin LV created and imported"
    );

    Ok(BaseImageHandle {
        lv_name,
        device_path,
        size_bytes: base_size,
    })
}

/// Tear down the base image thin LV.
pub fn teardown_base_image(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    run_cmd("lvremove", &["-f", &lv_path])
        .with_context(|| format!("Failed to remove base image thin LV {}", lv_path))?;
    tracing::info!(lv_name = %lv_name, "Base image thin LV removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Per-VM snapshot management
// ---------------------------------------------------------------------------

/// Create a thin snapshot for a VM from the base image.
///
/// Creates a native LVM thin snapshot, then optionally resizes if the
/// requested size exceeds the base image size.
pub fn create_snapshot(
    base: &BaseImageHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    let t0 = Instant::now();
    let lv_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));
    let base_lv_path = format!("{}/{}", lvm_config.volume_group, base.lv_name);

    // Create thin snapshot of the base image.
    run_cmd(
        "lvcreate",
        &["--snapshot", &base_lv_path, "--name", &lv_name],
    )
    .with_context(|| format!("Failed to create thin snapshot {}", lv_name))?;
    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "lvcreate --snapshot complete"
    );

    // Resize if requested size exceeds the base image.
    if size_bytes > base.size_bytes {
        let t_resize = Instant::now();
        let size_arg = format!("{}B", size_bytes);
        let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
        if let Err(e) = run_cmd("lvresize", &["-L", &size_arg, &lv_path]) {
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!("Failed to resize thin snapshot {}", lv_name)));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            size_bytes,
            elapsed_ms = t_resize.elapsed().as_millis() as u64,
            "lvresize complete"
        );

        // Check and resize the filesystem.
        let t_fsck = Instant::now();
        let dev_str = device_path.to_string_lossy().to_string();
        if let Err(e) = run_cmd("e2fsck", &["-f", "-y", &dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "e2fsck reported issues (continuing with resize)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_fsck.elapsed().as_millis() as u64,
            "e2fsck complete"
        );

        let t_resize2fs = Instant::now();
        if let Err(e) = run_cmd("resize2fs", &[&dev_str]) {
            let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!("Failed to resize2fs on {}", lv_name)));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_resize2fs.elapsed().as_millis() as u64,
            "resize2fs complete"
        );
    }

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        size_bytes,
        base_lv = %base.lv_name,
        total_ms = t0.elapsed().as_millis() as u64,
        "Thin snapshot created for VM"
    );

    Ok(ThinSnapshotHandle {
        lv_name,
        device_path,
        size_bytes,
    })
}

/// Create a thin snapshot from a restored image file (snapshot restore path).
///
/// Creates a thin snapshot of the base image, resizes if needed, then
/// overwrites the snapshot content with the restored image via `dd`.
/// The temporary image file is deleted after import.
pub fn create_snapshot_from_image(
    base: &BaseImageHandle,
    vm_id: &str,
    lvm_config: &LvmConfig,
    image_file: &Path,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    let t0 = Instant::now();
    let lv_name = format!("indexify-vm-{}", vm_id);
    let device_path = PathBuf::from(format!("/dev/{}/{}", lvm_config.volume_group, lv_name));
    let base_lv_path = format!("{}/{}", lvm_config.volume_group, base.lv_name);

    let image_size = std::fs::metadata(image_file)
        .with_context(|| format!("Failed to stat image file {}", image_file.display()))?
        .len();

    let lv_size = size_bytes.max(image_size);

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        image_size,
        lv_size,
        image_file = %image_file.display(),
        "Starting snapshot restore from image"
    );

    // Create thin snapshot of the base image.
    if let Err(e) = run_cmd(
        "lvcreate",
        &["--snapshot", &base_lv_path, "--name", &lv_name],
    ) {
        return Err(e.context(format!(
            "Failed to create thin snapshot for restored VM {}",
            lv_name
        )));
    }
    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "lvcreate --snapshot complete (restore)"
    );

    // Resize if the image or requested size exceeds the base.
    if lv_size > base.size_bytes {
        let t_resize = Instant::now();
        let size_arg = format!("{}B", lv_size);
        let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
        if let Err(e) = run_cmd("lvresize", &["-L", &size_arg, &lv_path]) {
            let _ = run_cmd("lvremove", &["-f", &lv_path]);
            return Err(e.context(format!(
                "Failed to resize snapshot for restored VM {}",
                lv_name
            )));
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            lv_size,
            elapsed_ms = t_resize.elapsed().as_millis() as u64,
            "lvresize complete (restore)"
        );
    }

    // Copy the restored image data into the snapshot. Deliberately omit
    // conv=fdatasync — flushing hundreds of MB to physical storage adds
    // latency. The data stays in the page cache where Firecracker reads
    // it immediately.
    let t_dd = Instant::now();
    if let Err(e) = run_cmd(
        "dd",
        &[
            &format!("if={}", image_file.display()),
            &format!("of={}", device_path.display()),
            "bs=4M",
        ],
    ) {
        let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
        let _ = run_cmd("lvremove", &["-f", &lv_path]);
        return Err(e.context(format!("Failed to dd image into {}", lv_name)));
    }
    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        image_size,
        elapsed_ms = t_dd.elapsed().as_millis() as u64,
        "dd image into snapshot complete (restore)"
    );

    // Delete the temp image file now that data is in the LV.
    let _ = std::fs::remove_file(image_file);

    // Resize filesystem if the LV is larger than the image.
    if size_bytes > image_size {
        let t_fsck = Instant::now();
        let dev_str = device_path.to_string_lossy().to_string();
        if let Err(e) = run_cmd("e2fsck", &["-f", "-y", &dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "e2fsck reported issues on restored image (continuing)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_fsck.elapsed().as_millis() as u64,
            "e2fsck complete (restore)"
        );

        let t_resize2fs = Instant::now();
        if let Err(e) = run_cmd("resize2fs", &[&dev_str]) {
            tracing::warn!(
                vm_id = %vm_id,
                lv_name = %lv_name,
                error = ?e,
                "Failed to resize2fs on restored image (continuing)"
            );
        }
        tracing::info!(
            vm_id = %vm_id,
            lv_name = %lv_name,
            elapsed_ms = t_resize2fs.elapsed().as_millis() as u64,
            "resize2fs complete (restore)"
        );
    }

    tracing::info!(
        vm_id = %vm_id,
        lv_name = %lv_name,
        image_size,
        lv_size,
        total_ms = t0.elapsed().as_millis() as u64,
        "Thin snapshot created from restored image"
    );

    Ok(ThinSnapshotHandle {
        lv_name,
        device_path,
        size_bytes: lv_size,
    })
}

/// Destroy a VM's thin snapshot LV.
pub fn destroy_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let lv_path = format!("{}/{}", lvm_config.volume_group, lv_name);
    run_cmd("lvremove", &["-f", &lv_path])
        .with_context(|| format!("Failed to remove thin LV {}", lv_path))?;
    tracing::info!(lv_name = %lv_name, "Thin snapshot LV removed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Suspend/resume for snapshotter
// ---------------------------------------------------------------------------

/// Convert an LV name to its device-mapper name.
///
/// LVM's device-mapper names double all hyphens in the VG and LV names,
/// then join them with a single hyphen. For example:
/// VG "indexify-vg", LV "indexify-vm-abc" → "indexify--vg-indexify--vm--abc"
fn dm_name_for_lv(config: &LvmConfig, lv_name: &str) -> String {
    let vg_escaped = config.volume_group.replace('-', "--");
    let lv_escaped = lv_name.replace('-', "--");
    format!("{}-{}", vg_escaped, lv_escaped)
}

/// Suspend a thin LV device, flushing all pending I/O.
///
/// After suspension the device is consistent and can be read directly.
/// The device must be resumed or removed afterwards.
pub fn suspend_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let dm_name = dm_name_for_lv(lvm_config, lv_name);

    run_cmd("dmsetup", &["suspend", &dm_name])
        .with_context(|| format!("Failed to suspend dm device {}", dm_name))?;
    tracing::info!(lv_name = %lv_name, dm_name = %dm_name, "Thin LV suspended");
    Ok(())
}

/// Async version of suspend_snapshot.
pub async fn suspend_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || suspend_snapshot(&lv_name, &lvm_config))
        .await
        .context("suspend_snapshot task panicked")?
}

/// Resume a previously suspended thin LV device.
pub fn resume_snapshot(lv_name: &str, lvm_config: &LvmConfig) -> Result<()> {
    let dm_name = dm_name_for_lv(lvm_config, lv_name);

    run_cmd("dmsetup", &["resume", &dm_name])
        .with_context(|| format!("Failed to resume dm device {}", dm_name))?;
    tracing::info!(lv_name = %lv_name, dm_name = %dm_name, "Thin LV resumed");
    Ok(())
}

/// Async version of resume_snapshot.
pub async fn resume_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || resume_snapshot(&lv_name, &lvm_config))
        .await
        .context("resume_snapshot task panicked")?
}

// ---------------------------------------------------------------------------
// Stale device cleanup
// ---------------------------------------------------------------------------

/// Clean up stale `indexify-vm-*` thin LVs and old-format artifacts from
/// previous runs.
///
/// Only removes VM LVs whose VM ID is NOT in `active_vm_ids`.
/// Also removes old-format artifacts: `indexify-cow-*` LVs and the
/// `indexify-base` dm device at `/dev/mapper/indexify-base`.
pub fn cleanup_stale_devices(active_vm_ids: &HashSet<String>, lvm_config: &LvmConfig) {
    // Clean up old-format dm devices (dm-snapshot era).
    cleanup_old_format_dm_devices();

    // Clean up stale and old-format LVs.
    let vg = &lvm_config.volume_group;
    if let Ok(output) = run_cmd("lvs", &["--noheadings", "-o", "lv_name", vg]) {
        for line in output.lines() {
            let lv_name = line.trim();

            // Clean up old-format indexify-cow-* LVs.
            if lv_name.starts_with("indexify-cow-") {
                let lv_path = format!("{}/{}", vg, lv_name);
                if let Err(e) = run_cmd("lvremove", &["-f", &lv_path]) {
                    tracing::warn!(
                        lv_name = %lv_name,
                        error = ?e,
                        "Failed to remove old-format COW thin LV"
                    );
                } else {
                    tracing::info!(
                        lv_name = %lv_name,
                        "Removed old-format COW thin LV"
                    );
                }
                continue;
            }

            // Clean up stale indexify-vm-* LVs not associated with active VMs.
            if let Some(vm_id) = lv_name.strip_prefix("indexify-vm-") {
                if !active_vm_ids.contains(vm_id) {
                    let lv_path = format!("{}/{}", vg, lv_name);
                    if let Err(e) = run_cmd("lvremove", &["-f", &lv_path]) {
                        tracing::warn!(
                            lv_name = %lv_name,
                            error = ?e,
                            "Failed to remove stale VM thin LV"
                        );
                    } else {
                        tracing::info!(
                            lv_name = %lv_name,
                            "Removed stale VM thin LV"
                        );
                    }
                }
            }
        }
    }
}

/// Remove old-format dm devices left over from the dm-snapshot era.
fn cleanup_old_format_dm_devices() {
    // Remove old indexify-base dm device at /dev/mapper/indexify-base.
    if Path::new("/dev/mapper/indexify-base").exists() {
        match run_cmd("dmsetup", &["remove", "indexify-base"]) {
            Ok(_) => tracing::info!("Removed old-format indexify-base dm device"),
            Err(e) => {
                tracing::warn!(error = ?e, "Failed to remove old-format indexify-base dm device")
            }
        }
    }

    // Remove old indexify-vm-* dm devices and legacy thin-provisioning devices.
    let legacy_prefixes = ["indexify-vm-", "indexify-thin-", "indexify-tpool"];
    if let Ok(output) = run_cmd("dmsetup", &["ls"]) {
        let mut devs: Vec<String> = output
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .filter(|name| legacy_prefixes.iter().any(|p| name.starts_with(p)))
            .map(|s| s.to_string())
            .collect();
        // Sort so thin volumes come before pools.
        devs.sort();
        for name in &devs {
            if run_cmd("dmsetup", &["remove", name]).is_ok() {
                tracing::info!(dm_name = %name, "Removed old-format dm device");
            }
        }
    }

    // Detach any orphaned loop devices for indexify base rootfs.
    if let Ok(output) = run_cmd("losetup", &["-l", "--noheadings", "-O", "NAME,BACK-FILE"]) {
        for line in output.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1].contains("rootfs.ext4") {
                let loop_dev = parts[0];
                if run_cmd("losetup", &["-d", loop_dev]).is_ok() {
                    tracing::info!(loop_device = %loop_dev, "Detached orphaned loop device");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Async variants
// ---------------------------------------------------------------------------

/// Async version of create_snapshot (runs blocking IO on spawn_blocking).
pub async fn create_snapshot_async(
    base_lv_name: String,
    base_device_path: PathBuf,
    base_size_bytes: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let base = BaseImageHandle {
            lv_name: base_lv_name,
            device_path: base_device_path,
            size_bytes: base_size_bytes,
        };
        create_snapshot(&base, &vm_id, &lvm_config, size_bytes)
    })
    .await
    .context("create_snapshot task panicked")?
}

/// Async version of create_snapshot_from_image.
pub async fn create_snapshot_from_image_async(
    base_lv_name: String,
    base_device_path: PathBuf,
    base_size_bytes: u64,
    vm_id: String,
    lvm_config: LvmConfig,
    image_file: PathBuf,
    size_bytes: u64,
) -> Result<ThinSnapshotHandle> {
    tokio::task::spawn_blocking(move || {
        let base = BaseImageHandle {
            lv_name: base_lv_name,
            device_path: base_device_path,
            size_bytes: base_size_bytes,
        };
        create_snapshot_from_image(&base, &vm_id, &lvm_config, &image_file, size_bytes)
    })
    .await
    .context("create_snapshot_from_image task panicked")?
}

/// Async version of destroy_snapshot.
pub async fn destroy_snapshot_async(lv_name: String, lvm_config: LvmConfig) -> Result<()> {
    tokio::task::spawn_blocking(move || destroy_snapshot(&lv_name, &lvm_config))
        .await
        .context("destroy_snapshot task panicked")?
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
