//! Docker-based snapshotter using `docker export` + zstd + blob store.
//!
//! **Snapshot**: docker export → streaming zstd compress → blob store put
//! **Restore**: blob store streaming download → zstd decompress → docker import
//!
//! When the runtime is `runsc` (gVisor), snapshot creation uses
//! `runsc tar rootfs-upper` instead of `docker export` to capture only the
//! filesystem delta. Restore writes the tar to a local file and returns a
//! `rootfs_overlay` path instead of importing a Docker image.
//!
//! Both pipelines are fully streaming with bounded memory usage.

use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    body_full,
    query_parameters::{CreateImageOptionsBuilder, InspectContainerOptions, RemoveImageOptions},
};
use bytes::Bytes;
use futures_util::StreamExt;
use indexify_blob_store::PutOptions;
use nix::unistd::geteuid;
use tracing::{debug, info, warn};

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

/// Default directory for storing restored gVisor snapshot overlay tars.
const DEFAULT_SNAPSHOT_LOCAL_DIR: &str = "/tmp/indexify-snapshots";

/// Default runsc state root used by Docker's runtime integration.
/// Docker stores container state under this path, which is root-only
/// (`drwx------`), so we invoke `runsc` via `sudo`.
const DEFAULT_RUNSC_ROOT: &str = "/var/run/docker/runtime-runc/moby";

/// Docker-based snapshotter using `docker export` + zstd + blob store.
///
/// Snapshot: docker export → zstd compress → blob store put (streaming)
/// Restore:  blob store streaming download → zstd decompress → docker import
///
/// When `runtime` is `"runsc"`, uses gVisor-native commands instead.
pub struct DockerSnapshotter {
    docker: Docker,
    blob_store: BlobStore,
    _metrics: Arc<DataplaneMetrics>,
    /// OCI runtime name (e.g., `"runsc"` for gVisor). `None` means default
    /// Docker runtime (runc).
    runtime: Option<String>,
    /// Root directory for runsc container state (`--root` flag).
    runsc_root: String,
    /// Local directory for storing restored gVisor overlay tars.
    snapshot_local_dir: String,
}

impl DockerSnapshotter {
    pub fn new(
        docker: Docker,
        blob_store: BlobStore,
        metrics: Arc<DataplaneMetrics>,
        runtime: Option<String>,
        runsc_root: Option<String>,
        snapshot_local_dir: Option<String>,
    ) -> Self {
        Self {
            docker,
            blob_store,
            _metrics: metrics,
            runtime,
            runsc_root: runsc_root.unwrap_or_else(|| DEFAULT_RUNSC_ROOT.to_string()),
            snapshot_local_dir: snapshot_local_dir
                .unwrap_or_else(|| DEFAULT_SNAPSHOT_LOCAL_DIR.to_string()),
        }
    }

    /// Whether this snapshotter is configured for gVisor (runsc).
    fn is_gvisor(&self) -> bool {
        self.runtime.as_deref() == Some("runsc")
    }

    /// Remove stale overlay tars from `snapshot_local_dir`.
    ///
    /// Call this at startup to clean up any orphaned files from a previous
    /// crash or unclean shutdown. Only affects `.tar` and `.tar.tmp` files
    /// in the snapshot directory — other files are left untouched.
    pub async fn cleanup_stale_overlays(&self) {
        let dir = PathBuf::from(&self.snapshot_local_dir);
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(_) => return, // Directory doesn't exist yet — nothing to clean.
        };

        let mut removed = 0u32;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();

            if name.ends_with(".tar") || name.ends_with(".tar.tmp") {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to remove stale overlay tar"
                    );
                } else {
                    removed += 1;
                }
            }
        }

        if removed > 0 {
            info!(
                dir = %dir.display(),
                removed = removed,
                "Cleaned up stale overlay tars from previous run"
            );
        }
    }
}

#[async_trait]
impl Snapshotter for DockerSnapshotter {
    fn requires_running_container(&self) -> bool {
        self.is_gvisor()
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
            gvisor = self.is_gvisor(),
            "Starting streaming snapshot creation"
        );

        if self.is_gvisor() {
            return self
                .create_snapshot_gvisor(container_id, snapshot_id, upload_uri)
                .await;
        }

        // runc path: docker export → zstd compress → blob store put
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
            gvisor = self.is_gvisor(),
            "Starting snapshot restore"
        );

        // Download and decompress (shared between runc and gVisor paths).
        let decompressed = self.download_and_decompress(snapshot_uri).await?;

        if self.is_gvisor() {
            return self
                .restore_snapshot_gvisor(&tag, snapshot_uri, decompressed)
                .await;
        }

        // runc path: docker import
        info!(
            snapshot_uri = %snapshot_uri,
            decompressed_size = decompressed.len(),
            "Decompressed snapshot, importing to Docker"
        );

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

        Ok(RestoreResult {
            image: image_tag,
            rootfs_overlay: None,
        })
    }

    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let tag = snapshot_tag_from_uri(snapshot_uri);

        if self.is_gvisor() {
            // gVisor path: delete the local overlay tar file.
            let tar_path = PathBuf::from(&self.snapshot_local_dir).join(format!("{}.tar", tag));
            if let Err(e) = tokio::fs::remove_file(&tar_path).await {
                debug!(
                    path = %tar_path.display(),
                    error = %e,
                    "Failed to remove gVisor overlay tar (may not exist)"
                );
            }
            return Ok(());
        }

        // runc path: remove the imported Docker image.
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

// ── gVisor-specific methods ──────────────────────────────────────────────

impl DockerSnapshotter {
    /// gVisor snapshot: use `runsc tar rootfs-upper` to capture only the
    /// filesystem delta (copy-on-write upper layer).
    async fn create_snapshot_gvisor(
        &self,
        container_id: &str,
        snapshot_id: &str,
        upload_uri: &str,
    ) -> Result<SnapshotResult> {
        // Resolve the Docker container name to the full hex container ID.
        // `runsc` requires the full ID, not the friendly name.
        let inspect = self
            .docker
            .inspect_container(container_id, None::<InspectContainerOptions>)
            .await
            .context("Failed to inspect container for gVisor snapshot")?;

        let full_id = inspect
            .id
            .ok_or_else(|| anyhow::anyhow!("Container {} has no ID", container_id))?;

        // Write the rootfs-upper tar to a temp file inside snapshot_local_dir,
        // then compress + upload. Using the same directory avoids cross-device
        // move issues and keeps temp files in one place.
        tokio::fs::create_dir_all(&self.snapshot_local_dir)
            .await
            .context("Failed to create snapshot temp directory")?;

        let tmp_tar = PathBuf::from(&self.snapshot_local_dir)
            .join(format!("gvisor-snap-{}.tar.tmp", snapshot_id));
        let tmp_tar_str = tmp_tar.to_string_lossy().to_string();

        info!(
            container_id = %container_id,
            full_id = %full_id,
            tmp_tar = %tmp_tar_str,
            "Running runsc tar rootfs-upper"
        );

        // The Docker runtime state directory is root-only, so `runsc` must
        // run via `sudo` when the dataplane is not running as root.
        let mut cmd = tokio::process::Command::new(if geteuid().is_root() {
            "runsc"
        } else {
            "sudo"
        });
        if !geteuid().is_root() {
            cmd.arg("runsc");
        }
        cmd.arg("--root")
            .arg(&self.runsc_root)
            .arg("tar")
            .arg("rootfs-upper")
            .arg("--file")
            .arg(&tmp_tar_str)
            .arg(&full_id);

        let output = match tokio::time::timeout(SNAPSHOT_TOTAL_TIMEOUT, cmd.output()).await {
            Ok(result) => result.context("Failed to execute runsc tar rootfs-upper")?,
            Err(_) => {
                // Clean up temp file on timeout
                let _ = tokio::fs::remove_file(&tmp_tar).await;
                anyhow::bail!(
                    "runsc tar rootfs-upper timed out after {}s",
                    SNAPSHOT_TOTAL_TIMEOUT.as_secs()
                );
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let _ = tokio::fs::remove_file(&tmp_tar).await;
            anyhow::bail!(
                "runsc tar rootfs-upper failed (exit {}): {}",
                output.status.code().unwrap_or(-1),
                stderr
            );
        }

        // Read the tar, compress with zstd, upload to blob store.
        let raw_tar = tokio::fs::read(&tmp_tar)
            .await
            .context("Failed to read gVisor rootfs-upper tar")?;

        // Clean up temp file immediately — we have the data in memory.
        let _ = tokio::fs::remove_file(&tmp_tar).await;

        let raw_size = raw_tar.len() as u64;

        info!(
            container_id = %container_id,
            raw_size = raw_size,
            "Compressing gVisor rootfs-upper tar"
        );

        let compressed = tokio::task::spawn_blocking(move || {
            zstd::encode_all(raw_tar.as_slice(), 3)
        })
        .await
        .context("zstd compression task panicked")?
        .context("Failed to compress gVisor snapshot")?;

        let compressed_size = compressed.len() as u64;

        // Upload as a single-chunk stream. Box::pin required because
        // blob_store.put() requires Unpin.
        let stream = Box::pin(futures_util::stream::once(async {
            Ok(Bytes::from(compressed))
        }));

        let result = match tokio::time::timeout(
            SNAPSHOT_TOTAL_TIMEOUT,
            self.blob_store
                .put(upload_uri, stream, PutOptions::default()),
        )
        .await
        {
            Ok(result) => result.context("gVisor snapshot upload failed")?,
            Err(_) => {
                anyhow::bail!(
                    "gVisor snapshot upload timed out after {}s",
                    SNAPSHOT_TOTAL_TIMEOUT.as_secs()
                );
            }
        };

        info!(
            container_id = %container_id,
            snapshot_id = %snapshot_id,
            upload_uri = %upload_uri,
            raw_size = raw_size,
            compressed_size = compressed_size,
            blob_size = result.size_bytes,
            "gVisor snapshot upload completed"
        );

        Ok(SnapshotResult {
            snapshot_uri: upload_uri.to_string(),
            size_bytes: result.size_bytes,
        })
    }

    /// gVisor restore: write the decompressed tar to a local file and return
    /// its path as `rootfs_overlay`. The Docker driver will pass it as a
    /// `dev.gvisor.tar.rootfs.upper` annotation.
    async fn restore_snapshot_gvisor(
        &self,
        tag: &str,
        snapshot_uri: &str,
        decompressed: Vec<u8>,
    ) -> Result<RestoreResult> {
        let tar_path = PathBuf::from(&self.snapshot_local_dir).join(format!("{}.tar", tag));

        // Ensure the snapshot directory exists.
        tokio::fs::create_dir_all(&self.snapshot_local_dir)
            .await
            .context("Failed to create gVisor snapshot directory")?;

        tokio::fs::write(&tar_path, &decompressed)
            .await
            .context("Failed to write gVisor overlay tar")?;

        info!(
            snapshot_uri = %snapshot_uri,
            tar_path = %tar_path.display(),
            size = decompressed.len(),
            "gVisor snapshot restored as overlay tar"
        );

        Ok(RestoreResult {
            image: String::new(),
            rootfs_overlay: Some(tar_path.to_string_lossy().to_string()),
        })
    }
}

// ── Shared helpers (runc path) ───────────────────────────────────────────

impl DockerSnapshotter {
    /// Download from blob store and decompress with zstd. Used by both runc
    /// and gVisor restore paths.
    async fn download_and_decompress(&self, snapshot_uri: &str) -> Result<Vec<u8>> {
        let compressed_stream = self
            .blob_store
            .get_stream(snapshot_uri)
            .await
            .context("Failed to open snapshot stream")?;

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

        let decompressed = tokio::task::spawn_blocking(move || {
            zstd::decode_all(compressed.as_slice())
        })
        .await
        .context("zstd decompression task panicked")?
        .context("Failed to decompress snapshot")?;

        Ok(decompressed)
    }

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

