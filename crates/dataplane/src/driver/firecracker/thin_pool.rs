//! Device-mapper thin pool management for Firecracker rootfs volumes.
//!
//! Uses the `devicemapper` crate to create and manage a thin-provisioned pool
//! backed by two raw block devices (one for metadata, one for data). Each VM
//! gets a thin volume carved from this pool.
//!
//! Phase 1: Each `create_volume()` populates the thin volume with `dd` from
//! the base image. Phase 2 will replace this with instant thin snapshots.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use devicemapper::{
    DM, DmDevice, DmName, DmOptions, DataBlocks, Device, LinearDev, LinearDevTargetParams,
    LinearTargetParams, Sectors, TargetLine, ThinDev, ThinDevId, ThinPoolDev,
    ThinPoolStatusSummary,
};

/// Manages a device-mapper thin pool and per-VM thin volumes.
pub struct ThinPoolManager {
    dm: DM,
    thin_pool: ThinPoolDev,
    /// Track active thin devices by their thin ID.
    thin_devs: HashMap<u32, ThinDev>,
}

/// Status information for the thin pool.
#[derive(Debug)]
pub struct PoolStatus {
    pub healthy: bool,
}

impl ThinPoolManager {
    /// Initialize or reconnect to a thin pool from raw block devices.
    ///
    /// If the thin pool already exists (e.g., after a dataplane restart with
    /// VMs still running), reconnects to it. Otherwise, cleans up stale
    /// devices, zeroes metadata, and creates a fresh pool.
    ///
    /// - `meta_dev_path`: Block device for thin pool metadata (e.g., "/dev/sdb1")
    /// - `data_dev_path`: Block device for thin pool data (e.g., "/dev/sdb2")
    /// - `block_size`: Data block size in sectors (default: 128 = 64KB)
    /// - `low_water_mark`: Low water mark in data blocks (default: 1024)
    pub fn new(
        meta_dev_path: &str,
        data_dev_path: &str,
        block_size: u64,
        low_water_mark: u64,
    ) -> Result<Self> {
        let dm = DM::new().context("Failed to initialize device-mapper")?;

        // Check if the thin pool already exists (recovery scenario — VMs may
        // still be running with thin devices open).
        let pool_exists = std::path::Path::new("/dev/mapper/indexify-tpool").exists();

        if !pool_exists {
            // Fresh start: clean up any stale fragments and zero metadata.
            Self::cleanup_stale_devices(&dm);
            Self::zero_metadata_device(meta_dev_path)?;
        }

        // Get Device (major:minor) from the block device paths
        let meta_device = device_from_path(meta_dev_path)
            .with_context(|| format!("Failed to stat metadata device {}", meta_dev_path))?;
        let data_device = device_from_path(data_dev_path)
            .with_context(|| format!("Failed to stat data device {}", data_dev_path))?;

        // Get sizes of the block devices
        let meta_size = device_size_sectors(meta_dev_path)
            .with_context(|| format!("Failed to get size of {}", meta_dev_path))?;
        let data_size = device_size_sectors(data_dev_path)
            .with_context(|| format!("Failed to get size of {}", data_dev_path))?;

        // Create (or reconnect to) LinearDev wrappers — setup() is idempotent
        let meta_table = vec![TargetLine::new(
            Sectors(0),
            meta_size,
            LinearDevTargetParams::Linear(LinearTargetParams::new(meta_device, Sectors(0))),
        )];
        let meta_linear = LinearDev::setup(
            &dm,
            DmName::new("indexify-tpool-meta")?,
            None,
            meta_table,
        )
        .context("Failed to create linear device for thin pool metadata")?;

        let data_table = vec![TargetLine::new(
            Sectors(0),
            data_size,
            LinearDevTargetParams::Linear(LinearTargetParams::new(data_device, Sectors(0))),
        )];
        let data_linear = LinearDev::setup(
            &dm,
            DmName::new("indexify-tpool-data")?,
            None,
            data_table,
        )
        .context("Failed to create linear device for thin pool data")?;

        // Create or reconnect to the thin pool.
        // - ThinPoolDev::new(): for fresh pools (requires empty metadata)
        // - ThinPoolDev::setup(): reconnects if pool exists, creates if not
        let thin_pool = if pool_exists {
            ThinPoolDev::setup(
                &dm,
                DmName::new("indexify-tpool")?,
                None,
                meta_linear,
                data_linear,
                Sectors(block_size),
                DataBlocks(low_water_mark),
                vec![],
            )
            .context("Failed to reconnect to existing thin pool")?
        } else {
            ThinPoolDev::new(
                &dm,
                DmName::new("indexify-tpool")?,
                None,
                meta_linear,
                data_linear,
                Sectors(block_size),
                DataBlocks(low_water_mark),
                vec![],
            )
            .context("Failed to create thin pool device")?
        };

        Ok(Self {
            dm,
            thin_pool,
            thin_devs: HashMap::new(),
        })
    }

    /// Remove any stale device-mapper devices left from a previous run.
    ///
    /// Uses `dmsetup ls` to find stale `indexify-*` devices and removes them
    /// in dependency order:
    /// 1. `indexify-thin-*` (thin volumes depend on the pool)
    /// 2. `indexify-tpool` (thin pool depends on the linear devs)
    /// 3. `indexify-tpool-data` and `indexify-tpool-meta` (linear devs)
    fn cleanup_stale_devices(_dm: &DM) {
        // Discover all stale indexify-* devices via dmsetup ls (no target filter)
        let mut thin_devs = Vec::new();
        if let Ok(output) = std::process::Command::new("dmsetup")
            .args(["ls"])
            .output()
        {
            let stdout = String::from_utf8_lossy(&output.stdout);
            for line in stdout.lines() {
                if let Some(name) = line.split_whitespace().next() {
                    if name.starts_with("indexify-thin-") {
                        thin_devs.push(name.to_string());
                    }
                }
            }
        }

        // Remove in dependency order with retries.
        // Do NOT use --force (it defers removal; the device may still be
        // present when we later zero the metadata device).
        let ordered_names: Vec<String> = thin_devs
            .into_iter()
            .chain(
                ["indexify-tpool", "indexify-tpool-data", "indexify-tpool-meta"]
                    .iter()
                    .map(|s| s.to_string()),
            )
            .collect();

        for name in &ordered_names {
            for _ in 0..5 {
                let result = std::process::Command::new("dmsetup")
                    .args(["remove", name])
                    .stderr(std::process::Stdio::piped())
                    .output();
                match result {
                    Ok(output) if output.status.success() => break,
                    _ => std::thread::sleep(std::time::Duration::from_millis(100)),
                }
            }
        }
    }

    /// Zero out the thin pool metadata device to clear stale state.
    ///
    /// The metadata device holds the thin provisioning superblock and mapping
    /// tables. If it contains data from a previous pool, `ThinDev::new()` will
    /// fail with EEXIST for thin device IDs that already exist in the old metadata.
    fn zero_metadata_device(meta_dev_path: &str) -> Result<()> {
        let output = std::process::Command::new("dd")
            .args([
                "if=/dev/zero",
                &format!("of={}", meta_dev_path),
                "bs=4096",
                "count=1024",
                "conv=notrunc",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .context("Failed to zero metadata device")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!("Failed to zero metadata device {}: {}", meta_dev_path, stderr);
        }
        Ok(())
    }

    /// Create a new thin volume for a VM.
    ///
    /// Returns the path to the dm device (e.g., `/dev/mapper/indexify-thin-42`).
    pub fn create_volume(&mut self, thin_id: u32, size_sectors: u64) -> Result<PathBuf> {
        let name = format!("indexify-thin-{}", thin_id);
        let dm_name = DmName::new(&name)?;
        let thin_dev_id = ThinDevId::new_u64(thin_id as u64)?;

        let thin_dev = ThinDev::new(
            &self.dm,
            dm_name,
            None,
            Sectors(size_sectors),
            &self.thin_pool,
            thin_dev_id,
        )
        .with_context(|| format!("Failed to create thin device with id {}", thin_id))?;

        self.thin_devs.insert(thin_id, thin_dev);

        Ok(PathBuf::from(format!("/dev/mapper/{}", name)))
    }

    /// Reconnect to an existing thin volume (used during recovery).
    ///
    /// After a dataplane restart, thin volumes may still be active in the kernel.
    /// This registers them in the in-memory map so `destroy_volume()` can find them.
    pub fn reconnect_volume(&mut self, thin_id: u32, size_sectors: u64) -> Result<()> {
        let name = format!("indexify-thin-{}", thin_id);
        let dm_name = DmName::new(&name)?;
        let thin_dev_id = ThinDevId::new_u64(thin_id as u64)?;

        let thin_dev = ThinDev::setup(
            &self.dm,
            dm_name,
            None,
            Sectors(size_sectors),
            &self.thin_pool,
            thin_dev_id,
        )
        .with_context(|| format!("Failed to reconnect thin device with id {}", thin_id))?;

        self.thin_devs.insert(thin_id, thin_dev);
        Ok(())
    }

    /// Create a CoW snapshot of an existing thin volume.
    ///
    /// The snapshot shares all blocks with the source and only stores
    /// divergent blocks on write. This is instant (<1ms).
    pub fn create_snapshot(
        &mut self,
        source_thin_id: u32,
        snapshot_thin_id: u32,
        _size_sectors: u64,
    ) -> Result<PathBuf> {
        let source = self
            .thin_devs
            .get(&source_thin_id)
            .context("Source thin device not found")?;
        let snap_name = format!("indexify-thin-{}", snapshot_thin_id);
        let dm_name = DmName::new(&snap_name)?;
        let snap_dev_id = ThinDevId::new_u64(snapshot_thin_id as u64)?;

        let snap = source
            .snapshot(
                &self.dm,
                dm_name,
                None,
                &self.thin_pool,
                snap_dev_id,
            )
            .with_context(|| {
                format!(
                    "Failed to snapshot thin {} -> {}",
                    source_thin_id, snapshot_thin_id
                )
            })?;

        self.thin_devs.insert(snapshot_thin_id, snap);
        Ok(PathBuf::from(format!("/dev/mapper/{}", snap_name)))
    }

    /// Destroy a thin volume, freeing its blocks back to the pool.
    pub fn destroy_volume(&mut self, thin_id: u32) -> Result<()> {
        if let Some(mut thin_dev) = self.thin_devs.remove(&thin_id) {
            thin_dev
                .destroy(&self.dm, &self.thin_pool)
                .with_context(|| format!("Failed to destroy thin device {}", thin_id))?;
        }
        Ok(())
    }

    /// Get the thin pool status.
    pub fn pool_status(&self) -> Result<PoolStatus> {
        let status = self
            .thin_pool
            .status(&self.dm, DmOptions::default())
            .context("Failed to get thin pool status")?;

        let healthy = match status {
            devicemapper::ThinPoolStatus::Working(ref st) => {
                st.summary == ThinPoolStatusSummary::Good
            }
            devicemapper::ThinPoolStatus::Error => false,
            devicemapper::ThinPoolStatus::Fail => false,
        };

        Ok(PoolStatus { healthy })
    }

    /// Tear down the entire thin pool (used during shutdown).
    pub fn teardown(mut self) -> Result<()> {
        // Destroy all active thin devices first
        let thin_ids: Vec<u32> = self.thin_devs.keys().cloned().collect();
        for thin_id in thin_ids {
            if let Err(e) = self.destroy_volume(thin_id) {
                tracing::warn!(thin_id, error = ?e, "Failed to destroy thin volume during teardown");
            }
        }

        self.thin_pool
            .teardown(&self.dm)
            .context("Failed to tear down thin pool")?;

        Ok(())
    }
}

/// Get device major:minor numbers from a block device path.
fn device_from_path(path: &str) -> Result<Device> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("Cannot stat device path: {}", path))?;
    let rdev = metadata.rdev();
    // Linux: major = rdev >> 8, minor = rdev & 0xff (for old-style)
    // Use nix::sys::stat for proper extraction
    let major = nix::sys::stat::major(rdev) as u32;
    let minor = nix::sys::stat::minor(rdev) as u32;
    Ok(Device { major, minor })
}

/// Get the size of a block device in sectors (512 bytes each).
///
/// Reads the size from `/sys/dev/block/{major}:{minor}/size` which reports
/// the number of 512-byte sectors, avoiding any need for raw ioctls.
fn device_size_sectors(path: &str) -> Result<Sectors> {
    let device = device_from_path(path)?;
    let sysfs_path = format!("/sys/dev/block/{}:{}/size", device.major, device.minor);
    let size_str = std::fs::read_to_string(&sysfs_path)
        .with_context(|| format!("Cannot read block device size from {}", sysfs_path))?;
    let sectors: u64 = size_str
        .trim()
        .parse()
        .with_context(|| format!("Invalid sector count in {}", sysfs_path))?;
    Ok(Sectors(sectors))
}
