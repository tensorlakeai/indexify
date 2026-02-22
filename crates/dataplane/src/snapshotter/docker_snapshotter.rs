//! Docker-based snapshotter using `docker export` + zstd + blob store.
//!
//! **Snapshot**: docker export → streaming zstd compress → blob store multipart
//! upload
//! **Restore**: blob store download → zstd decompress → docker import → local
//! image
//!
//! The snapshot pipeline is fully streaming: `docker export` chunks are fed
//! into a zstd encoder, and compressed output is uploaded in 100 MB parts as
//! soon as each part fills up. Peak memory usage is bounded to
//! ~UPLOAD_CHUNK_SIZE regardless of container filesystem size.

use std::{io::Write, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    body_full,
    query_parameters::{ImportImageOptions, RemoveImageOptions, TagImageOptions},
};
use bytes::Bytes;
use futures_util::StreamExt;
use tracing::{debug, info, warn};

use super::{RestoreResult, SnapshotResult, Snapshotter};
use crate::{blob_ops::BlobStore, metrics::DataplaneMetrics};

/// Chunk size for multipart upload (100 MB).
const UPLOAD_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/// Timeout for individual part uploads.
const UPLOAD_PART_TIMEOUT: Duration = Duration::from_secs(300);

/// Timeout for the entire docker export + compress + upload pipeline.
/// Prevents indefinite hangs if docker export stalls.
const SNAPSHOT_TOTAL_TIMEOUT: Duration = Duration::from_secs(3600);

/// Docker-based snapshotter using `docker export` + zstd + S3.
///
/// Snapshot: docker export → zstd compress → S3 multipart upload (streaming)
/// Restore:  S3 download → zstd decompress → docker import → local image
pub struct DockerSnapshotter {
    docker: Docker,
    blob_store: BlobStore,
    http_client: reqwest::Client,
    _metrics: Arc<DataplaneMetrics>,
}

impl DockerSnapshotter {
    pub fn new(docker: Docker, blob_store: BlobStore, metrics: Arc<DataplaneMetrics>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(UPLOAD_PART_TIMEOUT)
            .build()
            .expect("Failed to build HTTP client for snapshot uploads");
        Self {
            docker,
            blob_store,
            http_client,
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

        // Create the multipart upload first so we have an upload_id.
        let handle = self
            .blob_store
            .create_multipart_upload(upload_uri)
            .await
            .context("Failed to create multipart upload")?;

        // Stream docker export → zstd compress → multipart upload.
        // On failure, abort the multipart upload to clean up S3 resources.
        // Wrap the entire pipeline in a timeout to prevent indefinite hangs.
        let stream_result = match tokio::time::timeout(
            SNAPSHOT_TOTAL_TIMEOUT,
            self.stream_export_compress_upload(container_id, upload_uri, &handle.upload_id),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "Snapshot export+upload timed out after {}s",
                SNAPSHOT_TOTAL_TIMEOUT.as_secs()
            )),
        };

        match stream_result {
            Ok((compressed_size, etags)) => {
                self.blob_store
                    .complete_multipart_upload(upload_uri, &handle.upload_id, &etags)
                    .await
                    .context("Failed to complete multipart upload")?;

                info!(
                    container_id = %container_id,
                    snapshot_id = %snapshot_id,
                    upload_uri = %upload_uri,
                    size_bytes = compressed_size,
                    parts = etags.len(),
                    "Snapshot upload completed"
                );

                Ok(SnapshotResult {
                    snapshot_uri: upload_uri.to_string(),
                    size_bytes: compressed_size,
                })
            }
            Err(e) => {
                warn!(
                    upload_uri = %upload_uri,
                    error = %e,
                    "Snapshot stream failed, aborting multipart upload"
                );
                if let Err(abort_err) = self
                    .blob_store
                    .abort_multipart_upload(upload_uri, &handle.upload_id)
                    .await
                {
                    warn!(
                        upload_uri = %upload_uri,
                        error = %abort_err,
                        "Failed to abort multipart upload after failure"
                    );
                }
                Err(e).context("Snapshot creation failed")
            }
        }
    }

    async fn restore_snapshot(
        &self,
        snapshot_uri: &str,
        snapshot_id: &str,
    ) -> Result<RestoreResult> {
        info!(
            snapshot_uri = %snapshot_uri,
            snapshot_id = %snapshot_id,
            "Starting snapshot restore"
        );

        // Step 1: Download compressed snapshot
        let compressed = self
            .blob_store
            .get(snapshot_uri)
            .await
            .context("Failed to download snapshot")?;

        info!(
            snapshot_id = %snapshot_id,
            compressed_size = compressed.len(),
            "Downloaded snapshot, decompressing"
        );

        // Step 2: Decompress in a blocking thread. Move `compressed` into the
        // closure so it is dropped as soon as `decode_all` finishes, avoiding
        // holding both compressed and decompressed data simultaneously.
        let decompressed = tokio::task::spawn_blocking(move || {
            zstd::decode_all(compressed.as_ref())
            // `compressed` is dropped here after decode_all consumes it
        })
        .await
        .context("zstd decompression task panicked")?
        .context("Failed to decompress snapshot")?;

        info!(
            snapshot_id = %snapshot_id,
            decompressed_size = decompressed.len(),
            "Decompressed snapshot, importing to Docker"
        );

        // Step 3: Import into Docker
        let image_tag = format!("indexify-snapshot:{}", snapshot_id);

        let options = ImportImageOptions {
            quiet: true,
            ..Default::default()
        };

        let body = body_full(Bytes::from(decompressed));
        let mut import_stream = self.docker.import_image(options, body, None);

        let mut imported_id = None;
        while let Some(result) = import_stream.next().await {
            match result {
                Ok(info) => {
                    debug!(status = ?info.status, "Docker import progress");
                    if let Some(status) = &info.status &&
                        status.starts_with("sha256:")
                    {
                        imported_id = Some(status.clone());
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Docker import failed: {}", e));
                }
            }
        }

        // Ensure we got a valid image ID from the import
        let imported_id = imported_id.ok_or_else(|| {
            anyhow::anyhow!("Docker import completed but no image ID (sha256:...) was returned")
        })?;

        // Tag the imported image so we can reference it by name
        let tag_options = TagImageOptions {
            repo: Some("indexify-snapshot".to_string()),
            tag: Some(snapshot_id.to_string()),
        };
        self.docker
            .tag_image(&imported_id, Some(tag_options))
            .await
            .context("Failed to tag imported snapshot image")?;

        info!(
            snapshot_id = %snapshot_id,
            image_tag = %image_tag,
            "Snapshot restored as Docker image"
        );

        Ok(RestoreResult { image: image_tag })
    }

    async fn cleanup_local(&self, snapshot_id: &str) -> Result<()> {
        let image_tag = format!("indexify-snapshot:{}", snapshot_id);
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
    /// Stream docker export → zstd compress → multipart upload.
    ///
    /// Memory usage is bounded to ~UPLOAD_CHUNK_SIZE: each docker export chunk
    /// is fed into a zstd streaming encoder, and once the compressed output
    /// buffer exceeds UPLOAD_CHUNK_SIZE it is uploaded as a multipart part and
    /// the buffer is reused.
    ///
    /// Returns `(total_compressed_bytes, etags)`.
    async fn stream_export_compress_upload(
        &self,
        container_id: &str,
        upload_uri: &str,
        upload_id: &str,
    ) -> Result<(u64, Vec<String>)> {
        // zstd level 3 is a good balance of speed and compression.
        // Pre-allocate the output buffer slightly over UPLOAD_CHUNK_SIZE so
        // we don't reallocate on every part boundary.
        let mut encoder =
            zstd::stream::Encoder::new(Vec::with_capacity(UPLOAD_CHUNK_SIZE + 4 * 1024 * 1024), 3)
                .context("Failed to create zstd encoder")?;

        let mut etags = Vec::new();
        let mut part_number = 1i32;
        let mut total_compressed = 0u64;
        let mut total_raw = 0u64;

        // Stream docker export chunks directly into the zstd encoder.
        let mut export_stream = self.docker.export_container(container_id);

        while let Some(chunk_result) = export_stream.next().await {
            let chunk = chunk_result.context("Failed to read docker export stream")?;
            total_raw += chunk.len() as u64;

            // Compress chunk. Docker sends small chunks (~32-64 KB) so this
            // synchronous write is fast (microseconds) and won't block the
            // async runtime.
            encoder
                .write_all(&chunk)
                .context("zstd compression write failed")?;

            // When the compressed output buffer exceeds the upload chunk size,
            // drain it and upload as a multipart part.
            if encoder.get_ref().len() >= UPLOAD_CHUNK_SIZE {
                let data = std::mem::take(encoder.get_mut());
                total_compressed += data.len() as u64;

                let etag = self
                    .upload_single_part(upload_uri, upload_id, part_number, Bytes::from(data))
                    .await?;
                etags.push(etag);
                part_number += 1;
            }
        }

        // Finish the zstd frame and upload the remaining compressed data.
        let remaining = encoder.finish().context("Failed to finish zstd encoder")?;
        if !remaining.is_empty() || etags.is_empty() {
            total_compressed += remaining.len() as u64;
            let etag = self
                .upload_single_part(upload_uri, upload_id, part_number, Bytes::from(remaining))
                .await?;
            etags.push(etag);
        }

        info!(
            container_id = %container_id,
            raw_size = total_raw,
            compressed_size = total_compressed,
            parts = etags.len(),
            "Streaming export+compress+upload complete"
        );

        Ok((total_compressed, etags))
    }

    /// Upload a single part of a multipart upload with retry.
    ///
    /// Retries up to 3 times with exponential backoff (1s, 2s, 4s) on
    /// transient failures (network errors, 5xx responses).
    async fn upload_single_part(
        &self,
        upload_uri: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<String> {
        const MAX_RETRIES: u32 = 3;

        let presigned_url = self
            .blob_store
            .presign_upload_part_uri(upload_uri, part_number, upload_id)
            .await
            .context("Failed to presign upload part")?;

        let chunk_size = data.len();
        let mut last_err = None;

        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let delay = Duration::from_secs(1 << (attempt - 1));
                warn!(
                    part_number,
                    attempt,
                    delay_secs = delay.as_secs(),
                    "Retrying upload part after failure"
                );
                tokio::time::sleep(delay).await;
            }

            match self
                .http_client
                .put(&presigned_url)
                .body(data.clone())
                .send()
                .await
            {
                Ok(resp) => {
                    if resp.status().is_server_error() {
                        let status = resp.status();
                        last_err = Some(anyhow::anyhow!(
                            "Upload part {} returned server error: {}",
                            part_number,
                            status
                        ));
                        continue;
                    }

                    let etag = resp
                        .headers()
                        .get("etag")
                        .and_then(|v| v.to_str().ok())
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Missing or empty ETag header in upload part {} response",
                                part_number
                            )
                        })?
                        .to_string();

                    debug!(
                        part_number,
                        chunk_size,
                        etag = %etag,
                        "Uploaded part"
                    );

                    return Ok(etag);
                }
                Err(e) => {
                    last_err = Some(
                        anyhow::Error::from(e)
                            .context(format!("Failed to upload part {}", part_number)),
                    );
                }
            }
        }

        Err(last_err.unwrap())
    }
}
