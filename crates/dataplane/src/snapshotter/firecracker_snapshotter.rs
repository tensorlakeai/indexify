//! Firecracker-based snapshotter using dm-snapshot COW files + zstd + blob store.
//!
//! **Snapshot**: read COW file → streaming zstd compress → blob store put
//! **Restore**: blob store streaming download → zstd decompress → write COW file
//!
//! The COW file IS the delta — it contains only the blocks that differ from the
//! base rootfs image. This makes snapshot/restore extremely efficient.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use indexify_blob_store::PutOptions;
use sha2::{Digest, Sha256};
use tracing::info;

use super::{RestoreResult, SnapshotResult, Snapshotter};
use crate::blob_ops::BlobStore;
use crate::driver::firecracker::vm_state::VmMetadata;
use crate::metrics::DataplaneMetrics;

/// Size of compressed chunks yielded to `blob_store.put()`.
const COMPRESSED_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/// Firecracker snapshotter using dm-snapshot COW files.
///
/// Snapshot: read COW file → zstd compress → blob store put (streaming)
/// Restore:  blob store streaming download → zstd decompress → COW file
pub struct FirecrackerSnapshotter {
    state_dir: PathBuf,
    blob_store: BlobStore,
    _metrics: Arc<DataplaneMetrics>,
}

impl FirecrackerSnapshotter {
    pub fn new(
        state_dir: PathBuf,
        blob_store: BlobStore,
        metrics: Arc<DataplaneMetrics>,
    ) -> Self {
        Self {
            state_dir,
            blob_store,
            _metrics: metrics,
        }
    }
}

#[async_trait]
impl Snapshotter for FirecrackerSnapshotter {
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
        let vm_id = container_id
            .strip_prefix("fc-")
            .unwrap_or(container_id);

        // Load VM metadata to find the COW file path.
        let metadata_path = self.state_dir.join(format!("fc-{}.json", vm_id));
        let metadata = VmMetadata::load(&metadata_path)
            .with_context(|| format!("Failed to load VM metadata for {}", vm_id))?;

        let cow_path = PathBuf::from(&metadata.cow_file);
        if !cow_path.exists() {
            anyhow::bail!(
                "COW file not found for VM {}: {}",
                vm_id,
                cow_path.display()
            );
        }

        info!(
            cow_file = %cow_path.display(),
            "Reading COW file for snapshot"
        );

        // Build a compressed stream from the COW file.
        let compressed_stream = build_compressed_cow_stream(cow_path);

        let result = self
            .blob_store
            .put(upload_uri, compressed_stream, PutOptions::default())
            .await
            .context("Snapshot upload failed")?;

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
        let cow_path = format!("/tmp/indexify-snapshot-{}.cow", hash);

        info!(
            snapshot_uri = %snapshot_uri,
            cow_path = %cow_path,
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

        // Decompress and write to COW file.
        let cow_path_clone = cow_path.clone();
        tokio::task::spawn_blocking(move || {
            let decompressed = zstd::decode_all(compressed.as_slice())
                .context("Failed to decompress snapshot")?;
            std::fs::write(&cow_path_clone, &decompressed)
                .with_context(|| {
                    format!("Failed to write COW file {}", cow_path_clone)
                })?;
            info!(
                cow_path = %cow_path_clone,
                decompressed_size = decompressed.len(),
                "COW file restored"
            );
            Ok::<(), anyhow::Error>(())
        })
        .await
        .context("Snapshot restore task panicked")??;

        Ok(RestoreResult { image: cow_path, rootfs_overlay: None })
    }

    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let hash = hash_uri(snapshot_uri);
        let cow_path = format!("/tmp/indexify-snapshot-{}.cow", hash);
        if std::path::Path::new(&cow_path).exists() {
            let _ = std::fs::remove_file(&cow_path);
        }
        Ok(())
    }
}

/// Build an async stream that yields zstd-compressed chunks from a COW file.
fn build_compressed_cow_stream(
    cow_path: PathBuf,
) -> impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);

    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<()> {
            use std::io::Read;
            let file = std::fs::File::open(&cow_path)
                .with_context(|| format!("Failed to open COW file {}", cow_path.display()))?;
            let mut reader = std::io::BufReader::new(file);
            let mut encoder = zstd::stream::Encoder::new(
                Vec::with_capacity(COMPRESSED_CHUNK_SIZE + 4 * 1024 * 1024),
                3,
            )
            .context("Failed to create zstd encoder")?;

            let mut buf = [0u8; 4 * 1024 * 1024]; // 4MB read chunks
            loop {
                let n = reader
                    .read(&mut buf)
                    .context("Failed to read COW file")?;
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
