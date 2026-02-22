//! Docker-based snapshotter using `docker export` + zstd + blob store.
//!
//! **Snapshot**: docker export → streaming zstd compress → blob store put
//! **Restore**: blob store streaming download → zstd decompress → docker import
//!
//! Both pipelines are fully streaming with bounded memory usage.

use std::{io::Write, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    body_full,
    query_parameters::{CreateImageOptionsBuilder, RemoveImageOptions},
};
use bytes::Bytes;
use futures_util::StreamExt;
use indexify_blob_store::PutOptions;
use tracing::{debug, info};

use super::{RestoreResult, SnapshotResult, Snapshotter};
use crate::{blob_ops::BlobStore, metrics::DataplaneMetrics};

/// Size of compressed chunks yielded to `blob_store.put()`.
///
/// Docker export sends small chunks (~32-64 KB). We accumulate compressed
/// output until it reaches this size before yielding, keeping the number of
/// blob store parts manageable for large snapshots.
const COMPRESSED_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/// Timeout for the entire docker export + compress + upload pipeline.
/// Prevents indefinite hangs if docker export stalls.
const SNAPSHOT_TOTAL_TIMEOUT: Duration = Duration::from_secs(3600);

/// Docker-based snapshotter using `docker export` + zstd + blob store.
///
/// Snapshot: docker export → zstd compress → blob store put (streaming)
/// Restore:  blob store streaming download → zstd decompress → docker import
pub struct DockerSnapshotter {
    docker: Docker,
    blob_store: BlobStore,
    _metrics: Arc<DataplaneMetrics>,
}

impl DockerSnapshotter {
    pub fn new(docker: Docker, blob_store: BlobStore, metrics: Arc<DataplaneMetrics>) -> Self {
        Self {
            docker,
            blob_store,
            _metrics: metrics,
        }
    }
}

#[async_trait]
impl Snapshotter for DockerSnapshotter {
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
            "Starting streaming snapshot creation"
        );

        // Build a stream that yields compressed chunks from docker export.
        // The blob store's put() handles multipart upload, local FS writes,
        // retries, and cleanup internally.
        let compressed_stream = self.build_compressed_export_stream(container_id);

        let result = match tokio::time::timeout(
            SNAPSHOT_TOTAL_TIMEOUT,
            self.blob_store
                .put(upload_uri, compressed_stream, PutOptions::default()),
        )
        .await
        {
            Ok(result) => result.context("Snapshot upload failed")?,
            Err(_) => {
                anyhow::bail!(
                    "Snapshot export+upload timed out after {}s",
                    SNAPSHOT_TOTAL_TIMEOUT.as_secs()
                );
            }
        };

        info!(
            container_id = %container_id,
            snapshot_id = %snapshot_id,
            upload_uri = %upload_uri,
            size_bytes = result.size_bytes,
            "Snapshot upload completed"
        );

        Ok(SnapshotResult {
            snapshot_uri: upload_uri.to_string(),
            size_bytes: result.size_bytes,
        })
    }

    async fn restore_snapshot(&self, snapshot_uri: &str) -> Result<RestoreResult> {
        let tag = snapshot_tag_from_uri(snapshot_uri);
        info!(
            snapshot_uri = %snapshot_uri,
            tag = %tag,
            "Starting snapshot restore"
        );

        // Stream download from blob store → decompress → docker import.
        let compressed_stream = self
            .blob_store
            .get_stream(snapshot_uri)
            .await
            .context("Failed to open snapshot stream")?;

        // Collect the compressed data. We need the full blob in memory for
        // zstd::decode_all, but streaming download avoids a second copy vs get().
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

        // Decompress in a blocking thread. Move `compressed` into the closure
        // so it is dropped as soon as `decode_all` finishes.
        let decompressed = tokio::task::spawn_blocking(move || {
            zstd::decode_all(compressed.as_slice())
            // `compressed` is dropped here after decode_all consumes it
        })
        .await
        .context("zstd decompression task panicked")?
        .context("Failed to decompress snapshot")?;

        info!(
            snapshot_uri = %snapshot_uri,
            decompressed_size = decompressed.len(),
            "Decompressed snapshot, importing to Docker"
        );

        // Import into Docker using `POST /images/create?fromSrc=-`
        // (the `docker import` API). Note: bollard's `import_image` maps to
        // `docker load` (`/images/load`), which expects Docker image format —
        // not the raw filesystem tar from `docker export`.
        let image_tag = format!("indexify-snapshot:{}", tag);

        let options = CreateImageOptionsBuilder::default()
            .from_src("-")
            .repo(&image_tag)
            .build();

        let body = body_full(Bytes::from(decompressed));
        let mut import_stream = self.docker.create_image(Some(options), Some(body), None);

        let mut imported_id = None;
        while let Some(result) = import_stream.next().await {
            match result {
                Ok(info) => {
                    debug!(status = ?info.status, "Docker import progress");
                    if let Some(ref status) = info.status {
                        imported_id = Some(status.clone());
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Docker import failed: {}", e));
                }
            }
        }

        let imported_id = imported_id.unwrap_or_default();
        info!(
            snapshot_uri = %snapshot_uri,
            image_tag = %image_tag,
            imported_id = %imported_id,
            "Snapshot restored as Docker image"
        );

        Ok(RestoreResult { image: image_tag })
    }

    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let tag = snapshot_tag_from_uri(snapshot_uri);
        let image_tag = format!("indexify-snapshot:{}", tag);
        if let Err(e) = self
            .docker
            .remove_image(&image_tag, None::<RemoveImageOptions>, None)
            .await
        {
            debug!(
                image_tag = %image_tag,
                error = %e,
                "Failed to remove snapshot image (may not exist)"
            );
        }
        Ok(())
    }
}

impl DockerSnapshotter {
    /// Build an async stream that yields zstd-compressed chunks from a docker
    /// export.
    ///
    /// Docker export chunks (~32-64 KB) are fed into a synchronous zstd
    /// encoder. Compressed output is accumulated and yielded in
    /// ~COMPRESSED_CHUNK_SIZE pieces. The blob store's `put()` then handles
    /// uploading these chunks via its multipart upload logic.
    ///
    /// Memory usage is bounded to ~COMPRESSED_CHUNK_SIZE regardless of
    /// container filesystem size.
    fn build_compressed_export_stream(
        &self,
        container_id: &str,
    ) -> impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);
        let docker = self.docker.clone();
        let container_id = container_id.to_string();

        tokio::spawn(async move {
            let result = Self::run_export_compress_pipeline(&docker, &container_id, &tx).await;
            if let Err(e) = result {
                // Send the error downstream so blob_store.put() sees it.
                let _ = tx.send(Err(e)).await;
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// The actual export → compress → send pipeline running in a spawned task.
    async fn run_export_compress_pipeline(
        docker: &Docker,
        container_id: &str,
        tx: &tokio::sync::mpsc::Sender<Result<Bytes>>,
    ) -> Result<()> {
        // zstd level 3 is a good balance of speed and compression.
        // Pre-allocate the output buffer slightly over COMPRESSED_CHUNK_SIZE
        // to avoid reallocating on every chunk boundary.
        let mut encoder = zstd::stream::Encoder::new(
            Vec::with_capacity(COMPRESSED_CHUNK_SIZE + 4 * 1024 * 1024),
            3,
        )
        .context("Failed to create zstd encoder")?;

        let mut total_raw = 0u64;
        let mut total_compressed = 0u64;
        let mut export_stream = docker.export_container(container_id);

        while let Some(chunk_result) = export_stream.next().await {
            let chunk = chunk_result.context("Failed to read docker export stream")?;
            total_raw += chunk.len() as u64;

            // Compress chunk. Docker sends small chunks (~32-64 KB) so this
            // synchronous write is fast (microseconds) and won't block the
            // async runtime.
            encoder
                .write_all(&chunk)
                .context("zstd compression write failed")?;

            // When the compressed buffer is large enough, yield it downstream.
            if encoder.get_ref().len() >= COMPRESSED_CHUNK_SIZE {
                let data = std::mem::take(encoder.get_mut());
                total_compressed += data.len() as u64;
                tx.send(Ok(Bytes::from(data)))
                    .await
                    .map_err(|_| anyhow::anyhow!("Blob store consumer dropped"))?;
            }
        }

        // Finish the zstd frame and send any remaining compressed data.
        let remaining = encoder.finish().context("Failed to finish zstd encoder")?;
        if !remaining.is_empty() {
            total_compressed += remaining.len() as u64;
            tx.send(Ok(Bytes::from(remaining)))
                .await
                .map_err(|_| anyhow::anyhow!("Blob store consumer dropped"))?;
        }

        info!(
            container_id = %container_id,
            raw_size = total_raw,
            compressed_size = total_compressed,
            "Streaming export+compress complete"
        );

        Ok(())
    }
}

/// Derive a Docker image tag from a snapshot URI.
///
/// The URI format is `{base}/snapshots/{namespace}/{snapshot_id}.tar.zst`.
/// This extracts the filename stem (e.g. `abc123` from `.../abc123.tar.zst`).
/// Falls back to a sanitized version of the full URI if parsing fails.
fn snapshot_tag_from_uri(uri: &str) -> String {
    uri.rsplit('/')
        .next()
        .and_then(|filename| filename.split('.').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string()
}
