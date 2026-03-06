//! Device-mapper rootfs swapping for warm VM pool.
//!
//! Creates a dm-linear device backed by an empty LV at boot time, then
//! swaps the backing store to a user's thin LV snapshot at claim time
//! using `dmsetup suspend/load/resume`. Sub-millisecond since no I/O
//! is in-flight on the idle vdb device.

use anyhow::{Context, Result};
use tracing::warn;

use super::dm_thin::run_cmd;

/// Create a dm-linear device backed by a given LV.
///
/// Returns the device path `/dev/mapper/dm-vdb-{vm_id}`.
pub fn create_dm_device(vm_id: &str, backing_device: &str, size_sectors: u64) -> Result<String> {
    let dm_name = dm_device_name(vm_id);
    let table = format!("0 {} linear {} 0", size_sectors, backing_device);

    run_cmd("dmsetup", &["create", &dm_name, "--table", &table])?;

    Ok(dm_device_path(&dm_name))
}

/// Swap the backing store of a dm-linear device.
///
/// Suspends the device, loads a new table pointing at `new_backing_device`,
/// then resumes. Sub-millisecond when no I/O is in-flight.
pub fn swap_backing(dm_name: &str, new_backing_device: &str, size_sectors: u64) -> Result<()> {
    let table = format!("0 {} linear {} 0", size_sectors, new_backing_device);

    run_cmd("dmsetup", &["suspend", dm_name])?;

    // If load fails, try to resume anyway to avoid leaving the device suspended.
    let load_result = run_cmd("dmsetup", &["load", dm_name, "--table", &table]);

    let resume_result = run_cmd("dmsetup", &["resume", dm_name]);

    // Report both errors if both failed (load is the root cause).
    if let Err(load_err) = load_result {
        if let Err(resume_err) = resume_result {
            warn!(
                load_error = ?load_err,
                resume_error = ?resume_err,
                dm_name,
                "Both dmsetup load and resume failed — device may be stuck suspended"
            );
        }
        return Err(load_err);
    }
    resume_result?;

    // Flush the block device buffer cache so consumers (e.g. Firecracker VMM)
    // see fresh data from the new backing device instead of stale page cache
    // entries from the old backing.
    let dm_path = dm_device_path(dm_name);
    if let Err(e) = run_cmd("blockdev", &["--flushbufs", &dm_path]) {
        warn!(
            error = ?e,
            dm_name,
            "blockdev --flushbufs failed — Firecracker may see stale page cache data"
        );
    }

    Ok(())
}

/// Swap the backing store asynchronously (runs in spawn_blocking).
pub async fn swap_backing_async(
    dm_name: String,
    new_backing_device: String,
    size_sectors: u64,
) -> Result<()> {
    tokio::task::spawn_blocking(move || swap_backing(&dm_name, &new_backing_device, size_sectors))
        .await
        .context("dm swap task panicked")?
}

/// Remove a dm device.
pub fn remove_dm_device(dm_name: &str) -> Result<()> {
    run_cmd("dmsetup", &["remove", dm_name])?;
    Ok(())
}

/// Remove a dm device, falling back to `--force` on failure.
pub fn remove_dm_device_force(dm_name: &str) {
    if let Err(e) = remove_dm_device(dm_name) {
        warn!(error = ?e, dm_name, "dmsetup remove failed, retrying with --force");
        if let Err(e2) = run_cmd("dmsetup", &["remove", "--force", dm_name]) {
            warn!(error = ?e2, dm_name, "dmsetup remove --force also failed, device leaked");
        }
    }
}

/// Remove a dm device asynchronously.
pub async fn remove_dm_device_async(dm_name: String) {
    tokio::task::spawn_blocking(move || remove_dm_device_force(&dm_name))
        .await
        .ok();
}

/// Canonical dm device name for a warm VM's vdb.
pub fn dm_device_name(vm_id: &str) -> String {
    format!("dm-vdb-{}", vm_id)
}

/// Device path for a dm device name.
pub fn dm_device_path(dm_name: &str) -> String {
    format!("/dev/mapper/{}", dm_name)
}

/// Convert device size in bytes to 512-byte sectors.
///
/// Panics in debug mode if `bytes` is not aligned to 512.
pub fn bytes_to_sectors(bytes: u64) -> u64 {
    debug_assert!(
        bytes.is_multiple_of(512),
        "bytes_to_sectors: {} is not aligned to 512-byte sectors",
        bytes
    );
    bytes / 512
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dm_device_name() {
        assert_eq!(dm_device_name("abc123"), "dm-vdb-abc123");
    }

    #[test]
    fn test_dm_device_path() {
        assert_eq!(dm_device_path("dm-vdb-abc123"), "/dev/mapper/dm-vdb-abc123");
    }

    #[test]
    fn test_bytes_to_sectors() {
        assert_eq!(bytes_to_sectors(512), 1);
        assert_eq!(bytes_to_sectors(1024), 2);
        assert_eq!(bytes_to_sectors(1048576), 2048); // 1 MiB
    }
}
