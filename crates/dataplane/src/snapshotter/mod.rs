pub mod docker_snapshotter;

use anyhow::Result;
use async_trait::async_trait;

/// Result of a successful snapshot creation.
pub struct SnapshotResult {
    /// URI where the snapshot was stored (e.g. "s3://bucket/snap.tar.zst").
    pub snapshot_uri: String,
    /// Size of the snapshot in bytes.
    pub size_bytes: u64,
}

/// Result of a successful snapshot restore. Contains whatever the
/// implementation needs for creating a container from the snapshot.
pub struct RestoreResult {
    /// Image name/tag that can be passed to the container runtime.
    /// For Docker: the imported image tag.
    /// For Firecracker: path to the rootfs file.
    pub image: String,
}

/// Trait for snapshot backends. Implementations handle the specifics of
/// how container filesystems are captured, stored, and restored.
///
/// Analogous to `ProcessDriver` which abstracts container runtimes.
#[async_trait]
pub trait Snapshotter: Send + Sync {
    /// Capture the filesystem of a stopped container and upload to storage.
    ///
    /// The container should already be stopped (graceful SIGTERM sent)
    /// before calling this. The implementation handles:
    /// - Exporting the filesystem (docker export, rootfs copy, etc.)
    /// - Compressing the data
    /// - Uploading to the snapshot store
    async fn create_snapshot(
        &self,
        container_id: &str,
        snapshot_id: &str,
        upload_uri: &str,
    ) -> Result<SnapshotResult>;

    /// Download a snapshot and prepare it for container creation.
    ///
    /// The implementation handles:
    /// - Downloading from the snapshot store
    /// - Decompressing
    /// - Making it available to the container runtime (docker import, write
    ///   rootfs, etc.)
    ///
    /// Returns a `RestoreResult` with the image/path to use when creating the
    /// container.
    async fn restore_snapshot(&self, snapshot_uri: &str) -> Result<RestoreResult>;

    /// Delete a snapshot's local artifacts (cached images, temp files).
    /// The blob store cleanup (S3 deletion) is handled separately by the
    /// server.
    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let _ = snapshot_uri;
        Ok(())
    }
}
