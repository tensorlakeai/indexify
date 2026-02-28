//! Firecracker-based snapshotter using dm-thin LVs + zstd + blob store.
//!
//! **Snapshot**: read thin LV device → streaming zstd compress → blob store put
//! **Restore**: blob store streaming download → zstd decompress → write temp
//! image file (driver's create_snapshot_from_image copies into thin LV)
//!
//! Thin LVs are read in full, but unprovisioned zero blocks compress to nearly
//! nothing via zstd, so compressed size is comparable to delta-only COW.

use std::{io::Write, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use indexify_blob_store::PutOptions;
use sha2::{Digest, Sha256};
use tracing::info;

use super::{RestoreResult, SnapshotResult, Snapshotter};
use crate::{
    blob_ops::BlobStore,
    driver::firecracker::{api::FirecrackerApiClient, dm_thin, vm_state::VmMetadata},
    metrics::DataplaneMetrics,
};

/// Size of compressed chunks yielded to `blob_store.put()`.
const COMPRESSED_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/// Firecracker snapshotter using dm-thin LVs.
///
/// Snapshot: read thin LV device → zstd compress → blob store put (streaming)
/// Restore:  blob store streaming download → zstd decompress → image file
///           (driver's create_snapshot_from_image copies into thin LV)
pub struct FirecrackerSnapshotter {
    state_dir: PathBuf,
    blob_store: BlobStore,
    _metrics: Arc<DataplaneMetrics>,
    lvm_config: dm_thin::LvmConfig,
}

impl FirecrackerSnapshotter {
    pub fn new(
        state_dir: PathBuf,
        blob_store: BlobStore,
        metrics: Arc<DataplaneMetrics>,
        lvm_config: dm_thin::LvmConfig,
    ) -> Self {
        Self {
            state_dir,
            blob_store,
            _metrics: metrics,
            lvm_config,
        }
    }
}

#[async_trait]
impl Snapshotter for FirecrackerSnapshotter {
    fn requires_running_container(&self) -> bool {
        // The VM must be running so we can pause it via the Firecracker API.
        // Pausing quiesces the guest kernel, flushing dirty filesystem pages
        // to the virtual block device (thin LV). Without this, files written
        // inside the VM but not yet fsynced would be lost.
        true
    }

    async fn create_snapshot(
        &self,
        container_id: &str,
        snapshot_id: &str,
        upload_uri: &str,
    ) -> Result<SnapshotResult> {
        info!(
            container_id = %container_id,
            snapshot_id = %snapshot_id,
            upload_uri = %upload_uri,
            "Starting Firecracker snapshot creation"
        );

        // Parse vm_id from container_id (strip "fc-" prefix).
        let vm_id = container_id.strip_prefix("fc-").unwrap_or(container_id);

        // Load VM metadata to find the thin LV path.
        let metadata_path = self.state_dir.join(format!("fc-{}.json", vm_id));
        let metadata = VmMetadata::load(&metadata_path)
            .with_context(|| format!("Failed to load VM metadata for {}", vm_id))?;

        let lv_path = PathBuf::from(format!(
            "/dev/{}/{}",
            self.lvm_config.volume_group, metadata.lv_name
        ));
        if !lv_path.exists() {
            anyhow::bail!(
                "Thin LV device not found for VM {}: {}",
                vm_id,
                lv_path.display()
            );
        }

        // Pause the VM to quiesce the guest kernel. This ensures all dirty
        // filesystem pages are flushed to the virtual block device before we
        // read the thin LV.
        let api_client = FirecrackerApiClient::new(&metadata.socket_path);
        if let Err(e) = api_client.pause_vm().await {
            tracing::warn!(
                container_id = %container_id,
                vm_id = %vm_id,
                error = %e,
                "Failed to pause VM before snapshot (VM may already be stopped)"
            );
        } else {
            info!(container_id = %container_id, vm_id = %vm_id, "VM paused for snapshot");
        }

        // Suspend the thin LV device to flush all pending host-side I/O,
        // ensuring the device is consistent for reading.
        if let Err(e) =
            dm_thin::suspend_snapshot_async(metadata.lv_name.clone(), self.lvm_config.clone()).await
        {
            tracing::warn!(
                container_id = %container_id,
                vm_id = %vm_id,
                lv_name = %metadata.lv_name,
                error = %e,
                "Failed to suspend thin LV (continuing with snapshot)"
            );
        }

        info!(
            container_id = %container_id,
            lv_device = %lv_path.display(),
            "Reading thin LV device for snapshot"
        );

        // Build a compressed stream from the thin LV.
        let compressed_stream = build_compressed_stream(lv_path);

        let result = self
            .blob_store
            .put(upload_uri, compressed_stream, PutOptions::default())
            .await
            .context("Snapshot upload failed")?;

        // Resume the thin LV device so it can be cleanly removed during
        // the subsequent cleanup. `lvremove` fails on suspended devices.
        if let Err(e) =
            dm_thin::resume_snapshot_async(metadata.lv_name.clone(), self.lvm_config.clone()).await
        {
            tracing::warn!(
                container_id = %container_id,
                vm_id = %vm_id,
                lv_name = %metadata.lv_name,
                error = %e,
                "Failed to resume thin LV after snapshot (cleanup may fail)"
            );
        }

        info!(
            container_id = %container_id,
            snapshot_id = %snapshot_id,
            upload_uri = %upload_uri,
            size_bytes = result.size_bytes,
            "Firecracker snapshot upload completed"
        );

        Ok(SnapshotResult {
            snapshot_uri: upload_uri.to_string(),
            size_bytes: result.size_bytes,
        })
    }

    async fn restore_snapshot(&self, snapshot_uri: &str) -> Result<RestoreResult> {
        let hash = hash_uri(snapshot_uri);
        let img_path = format!("/tmp/indexify-snapshot-{}.img", hash);

        info!(
            snapshot_uri = %snapshot_uri,
            img_path = %img_path,
            "Starting Firecracker snapshot restore"
        );

        // Stream download from blob store.
        let compressed_stream = self
            .blob_store
            .get_stream(snapshot_uri)
            .await
            .context("Failed to open snapshot stream")?;

        // Collect compressed data.
        use futures_util::StreamExt;
        let mut compressed = Vec::new();
        futures_util::pin_mut!(compressed_stream);
        while let Some(chunk) = compressed_stream.next().await {
            let chunk = chunk.context("Failed to read snapshot stream")?;
            compressed.extend_from_slice(&chunk);
        }

        info!(
            snapshot_uri = %snapshot_uri,
            compressed_size = compressed.len(),
            "Downloaded snapshot, decompressing"
        );

        // Decompress and write to image file.
        let img_path_clone = img_path.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let decompressed =
                zstd::decode_all(compressed.as_slice()).context("Failed to decompress snapshot")?;
            std::fs::write(&img_path_clone, &decompressed)
                .with_context(|| format!("Failed to write image file {}", img_path_clone))?;
            info!(
                img_path = %img_path_clone,
                decompressed_size = decompressed.len(),
                "Image file restored"
            );
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("Snapshot restore task panicked")??;

        Ok(RestoreResult {
            image: img_path,
            rootfs_overlay: None,
        })
    }

    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let hash = hash_uri(snapshot_uri);
        let img_path = format!("/tmp/indexify-snapshot-{}.img", hash);
        if std::path::Path::new(&img_path).exists() {
            let _ = std::fs::remove_file(&img_path);
        }
        Ok(())
    }
}

/// Build an async stream that yields zstd-compressed chunks from a block
/// device.
fn build_compressed_stream(
    device_path: PathBuf,
) -> impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);

    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<()> {
            use std::{io::Read, os::unix::io::AsRawFd};
            let file = std::fs::File::open(&device_path)
                .with_context(|| format!("Failed to open device {}", device_path.display()))?;

            // Drop page cache for this device before reading. LVM thin
            // provisioning writes through the kernel bio layer, which may
            // bypass the page cache. Without this, buffered reads could
            // return stale data.
            unsafe {
                libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
            }

            let mut reader = std::io::BufReader::new(file);
            let mut encoder = zstd::stream::Encoder::new(
                Vec::with_capacity(COMPRESSED_CHUNK_SIZE + 4 * 1024 * 1024),
                3,
            )
            .context("Failed to create zstd encoder")?;

            let mut buf = vec![0u8; 4 * 1024 * 1024];
            loop {
                let n = reader.read(&mut buf).context("Failed to read device")?;
                if n == 0 {
                    break;
                }
                encoder
                    .write_all(&buf[..n])
                    .context("zstd compression write failed")?;

                if encoder.get_ref().len() >= COMPRESSED_CHUNK_SIZE {
                    let data = std::mem::take(encoder.get_mut());
                    if tx.blocking_send(Ok(Bytes::from(data))).is_err() {
                        return Ok(()); // Receiver dropped
                    }
                }
            }

            let remaining = encoder.finish().context("Failed to finish zstd encoder")?;
            if !remaining.is_empty() {
                let _ = tx.blocking_send(Ok(Bytes::from(remaining)));
            }

            Ok(())
        })();

        if let Err(e) = result {
            let _ = tx.blocking_send(Err(e));
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Derive a short hash from a snapshot URI for use in temp file names.
fn hash_uri(uri: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(uri.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}
