//! Device mapper snapshot driver for copy-on-write rootfs management.
//!
//! Implements the snapshot-based storage approach described at
//! <https://parandrus.dev/devicemapper/>. Uses Linux device mapper to create
//! copy-on-write overlays from a shared base rootfs image, enabling fast VM
//! startup with minimal disk overhead.
//!
//! Architecture (two layers):
//!
//! ```text
//! Layer 1 (base device):
//!   [0 .. BASE_SZ)  → linear target → base loop device
//!   [BASE_SZ .. OVERLAY_SZ) → zero target (virtual expansion)
//!
//! Layer 2 (snapshot overlay):
//!   [0 .. OVERLAY_SZ) → snapshot target → CoW on overlay loop device
//! ```
//!
//! Reads fall through the snapshot to the base; writes go to the CoW device.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::process::Command;
use tracing::{debug, info, warn};

/// Sector size in bytes used by device mapper.
const SECTOR_SIZE: u64 = 512;

/// Default chunk size for device mapper snapshot target (in sectors).
/// 8 sectors = 4 KiB, matching typical page size.
const DEFAULT_CHUNK_SIZE: u64 = 8;

// ---------------------------------------------------------------------------
// Loop device management
// ---------------------------------------------------------------------------

/// A Linux loop device backed by a file.
#[derive(Debug, Clone)]
pub struct LoopDevice {
    /// Path to the loop device (e.g., `/dev/loop0`).
    pub device_path: PathBuf,
    /// Path to the backing file.
    pub backing_file: PathBuf,
}

impl LoopDevice {
    /// Attach a file to a new loop device using `losetup`.
    ///
    /// The file must already exist. Use [`create_sparse_file`] to create a
    /// sparse backing file for overlays.
    pub async fn attach(file_path: &Path) -> Result<Self> {
        let output = Command::new("losetup")
            .args(["--find", "--show"])
            .arg(file_path)
            .output()
            .await
            .context("Failed to execute losetup")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("losetup failed: {}", stderr.trim());
        }

        let device_path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        info!(
            device = %device_path,
            file = %file_path.display(),
            "Loop device attached"
        );

        Ok(Self {
            device_path: PathBuf::from(device_path),
            backing_file: file_path.to_path_buf(),
        })
    }

    /// Detach this loop device.
    pub async fn detach(&self) -> Result<()> {
        let output = Command::new("losetup")
            .args(["--detach"])
            .arg(&self.device_path)
            .output()
            .await
            .context("Failed to execute losetup --detach")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "losetup --detach {} failed: {}",
                self.device_path.display(),
                stderr.trim()
            );
        }

        info!(
            device = %self.device_path.display(),
            "Loop device detached"
        );
        Ok(())
    }

    /// Get the size of the backing file in bytes.
    pub async fn size_bytes(&self) -> Result<u64> {
        let metadata = tokio::fs::metadata(&self.backing_file)
            .await
            .with_context(|| {
                format!(
                    "Failed to stat backing file: {}",
                    self.backing_file.display()
                )
            })?;
        Ok(metadata.len())
    }

    /// Get the size of the backing file in 512-byte sectors.
    pub async fn size_sectors(&self) -> Result<u64> {
        Ok(self.size_bytes().await? / SECTOR_SIZE)
    }
}

// ---------------------------------------------------------------------------
// Sparse file creation
// ---------------------------------------------------------------------------

/// Create a sparse file of the given logical size using `truncate`.
///
/// Sparse files consume no physical disk space until written to, making them
/// ideal for CoW overlay backing stores.
pub async fn create_sparse_file(path: &Path, size_bytes: u64) -> Result<()> {
    let output = Command::new("truncate")
        .args(["--size"])
        .arg(size_bytes.to_string())
        .arg(path)
        .output()
        .await
        .context("Failed to execute truncate")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "truncate failed for {}: {}",
            path.display(),
            stderr.trim()
        );
    }

    debug!(
        path = %path.display(),
        size_bytes = size_bytes,
        "Created sparse file"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Device mapper operations
// ---------------------------------------------------------------------------

/// Manages device mapper devices via `dmsetup`.
#[derive(Debug)]
pub struct DeviceMapper;

impl DeviceMapper {
    /// Create a new device mapper device with the given table.
    ///
    /// The table follows dmsetup format: one line per target with
    /// `start_sector num_sectors target_type target_args`.
    pub async fn create(name: &str, table: &str) -> Result<PathBuf> {
        // dmsetup create reads from stdin, so we pipe the table via shell.
        let output = Command::new("sh")
            .arg("-c")
            .arg(format!("echo '{}' | dmsetup create {}", table, name))
            .output()
            .await
            .context("Failed to execute dmsetup create")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("dmsetup create {} failed: {}", name, stderr.trim());
        }

        let device_path = PathBuf::from(format!("/dev/mapper/{}", name));
        info!(
            name = name,
            device = %device_path.display(),
            "Device mapper device created"
        );
        Ok(device_path)
    }

    /// Remove a device mapper device.
    pub async fn remove(name: &str) -> Result<()> {
        let output = Command::new("dmsetup")
            .args(["remove", name])
            .output()
            .await
            .context("Failed to execute dmsetup remove")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Don't fail if device doesn't exist (already cleaned up)
            if stderr.contains("No such device") || stderr.contains("not found") {
                warn!(name = name, "Device mapper device already removed");
                return Ok(());
            }
            anyhow::bail!("dmsetup remove {} failed: {}", name, stderr.trim());
        }

        info!(name = name, "Device mapper device removed");
        Ok(())
    }

    /// Check if a device mapper device exists.
    pub async fn exists(name: &str) -> Result<bool> {
        let output = Command::new("dmsetup")
            .args(["info", name])
            .output()
            .await
            .context("Failed to execute dmsetup info")?;

        Ok(output.status.success())
    }

    /// Get status of a device mapper device.
    pub async fn status(name: &str) -> Result<String> {
        let output = Command::new("dmsetup")
            .args(["status", name])
            .output()
            .await
            .context("Failed to execute dmsetup status")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("dmsetup status {} failed: {}", name, stderr.trim());
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

// ---------------------------------------------------------------------------
// Snapshot configuration
// ---------------------------------------------------------------------------

/// Configuration for creating a device mapper snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Path to the base rootfs image file.
    pub base_image_path: PathBuf,
    /// Directory where overlay sparse files will be created.
    pub overlay_dir: PathBuf,
    /// Total overlay size in bytes (must be >= base image size).
    /// The additional space beyond the base image is filled with a zero target.
    pub overlay_size_bytes: u64,
    /// Chunk size for the snapshot target in sectors. Defaults to 8 (4 KiB).
    pub chunk_size: Option<u64>,
}

// ---------------------------------------------------------------------------
// Snapshot handle
// ---------------------------------------------------------------------------

/// Handle to an active device mapper snapshot.
///
/// Represents the two-layer device mapper setup:
/// - `base_dm_name`: Layer 1 — linear + zero composite device
/// - `overlay_dm_name`: Layer 2 — snapshot (CoW) device
///
/// Drop or call [`SnapshotHandle::cleanup`] to release all resources.
#[derive(Debug)]
pub struct SnapshotHandle {
    /// Unique identifier for this snapshot.
    pub id: String,
    /// Device mapper name for the base device (layer 1).
    pub base_dm_name: String,
    /// Device mapper name for the overlay device (layer 2).
    pub overlay_dm_name: String,
    /// Path to the overlay device (e.g., `/dev/mapper/overlay-<id>`).
    pub overlay_device_path: PathBuf,
    /// Loop device for the base image.
    pub base_loop: LoopDevice,
    /// Loop device for the overlay sparse file.
    pub overlay_loop: LoopDevice,
    /// Path to the overlay sparse file (for cleanup).
    pub overlay_file_path: PathBuf,
}

impl SnapshotHandle {
    /// Clean up all resources: remove DM devices, detach loop devices, remove
    /// overlay file.
    pub async fn cleanup(&self) -> Result<()> {
        // Order matters: remove overlay (layer 2) before base (layer 1)
        info!(id = %self.id, "Cleaning up snapshot");

        if let Err(e) = DeviceMapper::remove(&self.overlay_dm_name).await {
            warn!(
                id = %self.id,
                error = ?e,
                "Failed to remove overlay DM device"
            );
        }

        if let Err(e) = DeviceMapper::remove(&self.base_dm_name).await {
            warn!(
                id = %self.id,
                error = ?e,
                "Failed to remove base DM device"
            );
        }

        if let Err(e) = self.overlay_loop.detach().await {
            warn!(
                id = %self.id,
                error = ?e,
                "Failed to detach overlay loop device"
            );
        }

        if let Err(e) = self.base_loop.detach().await {
            warn!(
                id = %self.id,
                error = ?e,
                "Failed to detach base loop device"
            );
        }

        // Remove the overlay sparse file
        if let Err(e) = tokio::fs::remove_file(&self.overlay_file_path).await {
            warn!(
                id = %self.id,
                path = %self.overlay_file_path.display(),
                error = ?e,
                "Failed to remove overlay sparse file"
            );
        }

        info!(id = %self.id, "Snapshot cleanup complete");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Snapshotter
// ---------------------------------------------------------------------------

/// Creates and manages device mapper snapshots for copy-on-write rootfs.
///
/// Each snapshot shares a read-only base image and gets its own CoW overlay,
/// enabling fast VM startup with minimal disk overhead.
pub struct Snapshotter {
    config: SnapshotConfig,
}

impl Snapshotter {
    /// Create a new Snapshotter with the given configuration.
    pub fn new(config: SnapshotConfig) -> Self {
        Self { config }
    }

    /// Create a new snapshot with the given unique ID.
    ///
    /// This sets up the two-layer device mapper structure:
    /// 1. Base device: `linear` target mapping to the base image + `zero` target
    ///    for expansion
    /// 2. Overlay device: `snapshot` target providing copy-on-write semantics
    ///
    /// Returns a [`SnapshotHandle`] that can be used to access the overlay device
    /// and clean up resources when done.
    pub async fn create_snapshot(&self, id: &str) -> Result<SnapshotHandle> {
        info!(id = id, "Creating device mapper snapshot");

        let chunk_size = self.config.chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);

        // Ensure overlay directory exists
        tokio::fs::create_dir_all(&self.config.overlay_dir)
            .await
            .with_context(|| {
                format!(
                    "Failed to create overlay directory: {}",
                    self.config.overlay_dir.display()
                )
            })?;

        // Step 1: Attach base image as a loop device
        let base_loop = LoopDevice::attach(&self.config.base_image_path)
            .await
            .context("Failed to attach base image loop device")?;

        let base_size_sectors = base_loop
            .size_sectors()
            .await
            .context("Failed to get base image size")?;

        let overlay_size_sectors = self.config.overlay_size_bytes / SECTOR_SIZE;

        if overlay_size_sectors < base_size_sectors {
            base_loop.detach().await.ok();
            anyhow::bail!(
                "Overlay size ({} sectors) must be >= base image size ({} sectors)",
                overlay_size_sectors,
                base_size_sectors
            );
        }

        let expansion_sectors = overlay_size_sectors - base_size_sectors;

        // Step 2: Create overlay sparse file and attach as loop device
        let overlay_file_path = self.config.overlay_dir.join(format!("overlay-{}.img", id));
        if let Err(e) = create_sparse_file(&overlay_file_path, self.config.overlay_size_bytes).await
        {
            base_loop.detach().await.ok();
            return Err(e).context("Failed to create overlay sparse file");
        }

        let overlay_loop = match LoopDevice::attach(&overlay_file_path).await {
            Ok(loop_dev) => loop_dev,
            Err(e) => {
                base_loop.detach().await.ok();
                tokio::fs::remove_file(&overlay_file_path).await.ok();
                return Err(e).context("Failed to attach overlay loop device");
            }
        };

        // Step 3: Create base DM device (layer 1: linear + zero)
        let base_dm_name = format!("base-{}", id);
        let base_table = if expansion_sectors > 0 {
            format!(
                "0 {} linear {} 0\n{} {} zero",
                base_size_sectors,
                base_loop.device_path.display(),
                base_size_sectors,
                expansion_sectors
            )
        } else {
            format!(
                "0 {} linear {} 0",
                base_size_sectors,
                base_loop.device_path.display()
            )
        };

        if let Err(e) = DeviceMapper::create(&base_dm_name, &base_table).await {
            overlay_loop.detach().await.ok();
            base_loop.detach().await.ok();
            tokio::fs::remove_file(&overlay_file_path).await.ok();
            return Err(e).context("Failed to create base DM device");
        }

        // Step 4: Create overlay DM device (layer 2: snapshot)
        let overlay_dm_name = format!("overlay-{}", id);
        let snapshot_table = format!(
            "0 {} snapshot /dev/mapper/{} {} P {}",
            overlay_size_sectors,
            base_dm_name,
            overlay_loop.device_path.display(),
            chunk_size
        );

        let overlay_device_path =
            match DeviceMapper::create(&overlay_dm_name, &snapshot_table).await {
                Ok(path) => path,
                Err(e) => {
                    DeviceMapper::remove(&base_dm_name).await.ok();
                    overlay_loop.detach().await.ok();
                    base_loop.detach().await.ok();
                    tokio::fs::remove_file(&overlay_file_path).await.ok();
                    return Err(e).context("Failed to create overlay DM device");
                }
            };

        info!(
            id = id,
            overlay_device = %overlay_device_path.display(),
            base_size_sectors = base_size_sectors,
            overlay_size_sectors = overlay_size_sectors,
            "Snapshot created successfully"
        );

        Ok(SnapshotHandle {
            id: id.to_string(),
            base_dm_name,
            overlay_dm_name,
            overlay_device_path,
            base_loop,
            overlay_loop,
            overlay_file_path,
        })
    }

    /// Get the configured overlay directory.
    pub fn overlay_dir(&self) -> &Path {
        &self.config.overlay_dir
    }

    /// Get the configured base image path.
    pub fn base_image_path(&self) -> &Path {
        &self.config.base_image_path
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_config_defaults() {
        let config = SnapshotConfig {
            base_image_path: PathBuf::from("/tmp/base.img"),
            overlay_dir: PathBuf::from("/tmp/overlays"),
            overlay_size_bytes: 1024 * 1024 * 1024, // 1 GiB
            chunk_size: None,
        };
        assert_eq!(config.base_image_path, PathBuf::from("/tmp/base.img"));
        assert_eq!(config.overlay_dir, PathBuf::from("/tmp/overlays"));
        assert_eq!(config.overlay_size_bytes, 1073741824);
        assert!(config.chunk_size.is_none());
    }

    #[test]
    fn test_snapshot_config_custom_chunk_size() {
        let config = SnapshotConfig {
            base_image_path: PathBuf::from("/tmp/base.img"),
            overlay_dir: PathBuf::from("/tmp/overlays"),
            overlay_size_bytes: 2 * 1024 * 1024 * 1024,
            chunk_size: Some(16),
        };
        assert_eq!(config.chunk_size, Some(16));
    }

    #[test]
    fn test_dm_device_naming() {
        let id = "abc123";
        assert_eq!(format!("base-{}", id), "base-abc123");
        assert_eq!(format!("overlay-{}", id), "overlay-abc123");
    }

    #[test]
    fn test_dm_table_linear_plus_zero() {
        let base_size_sectors: u64 = 2048;
        let overlay_size_sectors: u64 = 4096;
        let expansion_sectors = overlay_size_sectors - base_size_sectors;
        let base_loop_path = "/dev/loop0";

        let table = format!(
            "0 {} linear {} 0\n{} {} zero",
            base_size_sectors, base_loop_path, base_size_sectors, expansion_sectors
        );

        assert_eq!(
            table,
            "0 2048 linear /dev/loop0 0\n2048 2048 zero"
        );
    }

    #[test]
    fn test_dm_table_linear_only_no_expansion() {
        let base_size_sectors: u64 = 4096;
        let overlay_size_sectors: u64 = 4096;
        let expansion_sectors = overlay_size_sectors - base_size_sectors;

        let table = if expansion_sectors > 0 {
            format!(
                "0 {} linear /dev/loop0 0\n{} {} zero",
                base_size_sectors, base_size_sectors, expansion_sectors
            )
        } else {
            format!("0 {} linear /dev/loop0 0", base_size_sectors)
        };

        assert_eq!(table, "0 4096 linear /dev/loop0 0");
    }

    #[test]
    fn test_dm_table_snapshot() {
        let overlay_size_sectors: u64 = 4096;
        let base_dm_name = "base-test1";
        let overlay_loop_path = "/dev/loop1";
        let chunk_size: u64 = 8;

        let table = format!(
            "0 {} snapshot /dev/mapper/{} {} P {}",
            overlay_size_sectors, base_dm_name, overlay_loop_path, chunk_size
        );

        assert_eq!(
            table,
            "0 4096 snapshot /dev/mapper/base-test1 /dev/loop1 P 8"
        );
    }

    #[test]
    fn test_sector_size_constant() {
        assert_eq!(SECTOR_SIZE, 512);
    }

    #[test]
    fn test_bytes_to_sectors_conversion() {
        let bytes: u64 = 1024 * 1024; // 1 MiB
        let sectors = bytes / SECTOR_SIZE;
        assert_eq!(sectors, 2048);
    }

    #[test]
    fn test_default_chunk_size() {
        // 8 sectors * 512 bytes = 4096 bytes = 4 KiB
        assert_eq!(DEFAULT_CHUNK_SIZE * SECTOR_SIZE, 4096);
    }

    #[test]
    fn test_snapshotter_accessors() {
        let config = SnapshotConfig {
            base_image_path: PathBuf::from("/images/base.ext4"),
            overlay_dir: PathBuf::from("/var/lib/overlays"),
            overlay_size_bytes: 2 * 1024 * 1024 * 1024,
            chunk_size: Some(16),
        };
        let snapshotter = Snapshotter::new(config);
        assert_eq!(
            snapshotter.base_image_path(),
            Path::new("/images/base.ext4")
        );
        assert_eq!(
            snapshotter.overlay_dir(),
            Path::new("/var/lib/overlays")
        );
    }

    #[test]
    fn test_snapshot_handle_names() {
        let handle = SnapshotHandle {
            id: "vm-42".to_string(),
            base_dm_name: "base-vm-42".to_string(),
            overlay_dm_name: "overlay-vm-42".to_string(),
            overlay_device_path: PathBuf::from("/dev/mapper/overlay-vm-42"),
            base_loop: LoopDevice {
                device_path: PathBuf::from("/dev/loop0"),
                backing_file: PathBuf::from("/tmp/base.img"),
            },
            overlay_loop: LoopDevice {
                device_path: PathBuf::from("/dev/loop1"),
                backing_file: PathBuf::from("/tmp/overlay-vm-42.img"),
            },
            overlay_file_path: PathBuf::from("/tmp/overlay-vm-42.img"),
        };

        assert_eq!(handle.id, "vm-42");
        assert_eq!(handle.base_dm_name, "base-vm-42");
        assert_eq!(handle.overlay_dm_name, "overlay-vm-42");
        assert_eq!(
            handle.overlay_device_path,
            PathBuf::from("/dev/mapper/overlay-vm-42")
        );
    }

    #[test]
    fn test_overlay_file_path_construction() {
        let overlay_dir = PathBuf::from("/var/lib/snapshots");
        let id = "test-123";
        let overlay_file = overlay_dir.join(format!("overlay-{}.img", id));
        assert_eq!(
            overlay_file,
            PathBuf::from("/var/lib/snapshots/overlay-test-123.img")
        );
    }

    #[tokio::test]
    async fn test_create_sparse_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_sparse.img");
        let size: u64 = 10 * 1024 * 1024; // 10 MiB

        create_sparse_file(&file_path, size).await.unwrap();

        let metadata = tokio::fs::metadata(&file_path).await.unwrap();
        // Logical size should match requested size
        assert_eq!(metadata.len(), size);

        // On filesystems that support sparse files, physical size should be
        // much smaller. We only assert the logical size is correct since not
        // all filesystems/environments support sparse file semantics.
    }

    #[tokio::test]
    async fn test_loop_device_size_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_size.img");
        let size: u64 = 1024 * 1024; // 1 MiB

        create_sparse_file(&file_path, size).await.unwrap();

        let loop_dev = LoopDevice {
            device_path: PathBuf::from("/dev/loop99"),
            backing_file: file_path,
        };

        assert_eq!(loop_dev.size_bytes().await.unwrap(), size);
        assert_eq!(loop_dev.size_sectors().await.unwrap(), size / SECTOR_SIZE);
    }
}
