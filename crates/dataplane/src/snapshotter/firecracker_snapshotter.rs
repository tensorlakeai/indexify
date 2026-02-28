//! Firecracker-based snapshotter using dm-thin LVs + zstd + blob store.
//!
//! **Snapshot**: `thin_delta` metadata query identifies changed blocks → read
//! only those blocks from the VM device → emit as delta records → streaming
//! zstd compress → blob store put
//! **Restore**: blob store streaming download → zstd decompress → write delta
//! file (driver's create_snapshot_from_delta applies block records into thin
//! LV)
//!
//! Only blocks that differ from the base image are uploaded, so a 10GB rootfs
//! with 500MB of changes produces a ~500MB compressed blob instead of ~10GB.

use std::{io::Write, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
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
/// Snapshot: thin_delta metadata query → read changed blocks → delta records
///           → zstd → blob store put
/// Restore:  blob store streaming download → zstd decompress → delta file
///           (driver's create_snapshot_from_delta applies records into thin LV)
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

        // Suspend the thin LV device to flush all pending host-side I/O.
        // After flushing we immediately resume so that
        // `build_delta_compressed_stream` can read blocks from the device.
        // The VM is still paused, so no new writes will arrive.
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
        // Resume immediately — suspend was only needed to flush I/O.
        // A suspended device blocks ALL reads, so we must resume before
        // reading delta blocks.
        if let Err(e) =
            dm_thin::resume_snapshot_async(metadata.lv_name.clone(), self.lvm_config.clone()).await
        {
            tracing::warn!(
                container_id = %container_id,
                vm_id = %vm_id,
                lv_name = %metadata.lv_name,
                error = %e,
                "Failed to resume thin LV after flush (snapshot may hang)"
            );
        }

        info!(
            container_id = %container_id,
            lv_device = %lv_path.display(),
            "Building delta snapshot via thin_delta metadata query"
        );

        // Build a delta-compressed stream: thin_delta identifies changed
        // blocks, then only those blocks are read from the VM device.
        let compressed_stream = build_delta_compressed_stream(
            lv_path,
            self.lvm_config.clone(),
            metadata.lv_name.clone(),
        );

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
        let delta_path = format!("/tmp/indexify-snapshot-{}.delta", hash);

        info!(
            snapshot_uri = %snapshot_uri,
            delta_path = %delta_path,
            "Starting Firecracker snapshot restore (delta)"
        );

        // Channel bridges async download → sync decompression.
        // 16 items gives enough slack so the downloader and decompressor rarely
        // block on each other, without buffering the entire blob in memory.
        let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);

        // Spawn blocking decompression task.
        let delta_clone = delta_path.clone();
        let span = tracing::Span::current();
        let decompress_handle = tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            let reader = ChannelReader::new(rx);
            let mut decoder =
                zstd::stream::Decoder::new(reader).context("Failed to create zstd decoder")?;
            let file = std::fs::File::create(&delta_clone)
                .with_context(|| format!("Failed to create delta file {}", delta_clone))?;
            let mut output = std::io::BufWriter::new(file);
            std::io::copy(&mut decoder, &mut output).context("Failed to decompress snapshot")?;
            output.flush()?;
            info!(delta_path = %delta_clone, "Delta file restored (streaming)");
            Ok::<(), anyhow::Error>(())
        });

        // Async: stream download, feed chunks into channel.
        let stream = self
            .blob_store
            .get_stream(snapshot_uri)
            .await
            .context("Failed to open snapshot stream")?;
        futures_util::pin_mut!(stream);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("Failed to read snapshot stream")?;
            if tx.send(chunk).await.is_err() {
                break; // Decompress task errored/dropped
            }
        }
        drop(tx); // Signal EOF to decompressor

        decompress_handle
            .await
            .context("Snapshot restore task panicked")??;

        Ok(RestoreResult {
            image: delta_path,
            rootfs_overlay: None,
        })
    }

    async fn cleanup_local(&self, snapshot_uri: &str) -> Result<()> {
        let hash = hash_uri(snapshot_uri);
        let delta_path = format!("/tmp/indexify-snapshot-{}.delta", hash);
        if std::path::Path::new(&delta_path).exists() {
            let _ = std::fs::remove_file(&delta_path);
        }
        Ok(())
    }
}

/// Adapter that bridges a `tokio::sync::mpsc::Receiver<Bytes>` into a
/// synchronous `std::io::Read`.
///
/// Must only be used inside `spawn_blocking` — calls `blocking_recv()`.
struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    /// Current chunk being consumed.
    current: Option<Bytes>,
    /// Read offset into `current`.
    offset: usize,
}

impl ChannelReader {
    fn new(rx: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            current: None,
            offset: 0,
        }
    }
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if let Some(ref chunk) = self.current {
                let remaining = &chunk[self.offset..];
                if !remaining.is_empty() {
                    let n = remaining.len().min(buf.len());
                    buf[..n].copy_from_slice(&remaining[..n]);
                    self.offset += n;
                    return Ok(n);
                }
            }
            // Current chunk exhausted — get the next one.
            match self.rx.blocking_recv() {
                Some(chunk) => {
                    self.current = Some(chunk);
                    self.offset = 0;
                }
                None => return Ok(0), // Channel closed = EOF
            }
        }
    }
}

/// Build an async stream that yields zstd-compressed delta chunks.
///
/// Uses `thin_delta` metadata query to discover exactly which blocks changed,
/// then reads only those blocks from the VM device. The base device is never
/// opened.
fn build_delta_compressed_stream(
    device_path: PathBuf,
    lvm_config: dm_thin::LvmConfig,
    vm_lv_name: String,
) -> impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);

    tokio::task::spawn_blocking(move || {
        let result = (|| -> Result<()> {
            let ranges =
                dm_thin::query_thin_delta(&lvm_config, dm_thin::BASE_LV_NAME, &vm_lv_name)?;

            tracing::info!(
                num_ranges = ranges.len(),
                total_changed_bytes = ranges.iter().map(|r| r.byte_length).sum::<u64>(),
                "Using thin_delta metadata query for delta snapshot"
            );

            build_delta_from_ranges(&device_path, &ranges, &tx)
        })();

        if let Err(e) = result {
            let _ = tx.blocking_send(Err(e));
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Build delta stream by reading only the changed ranges from the VM device.
///
/// Uses `pread` (`read_at`) to seek directly to each changed range, avoiding
/// any reads of unchanged blocks. The base device is never opened.
fn build_delta_from_ranges(
    device_path: &std::path::Path,
    ranges: &[dm_thin::ThinDeltaRange],
    tx: &tokio::sync::mpsc::Sender<Result<Bytes>>,
) -> Result<()> {
    use std::os::unix::fs::FileExt;

    const BLOCK_SIZE: usize = 4 * 1024 * 1024;

    let vm_file = std::fs::File::open(device_path)
        .with_context(|| format!("Failed to open VM device {}", device_path.display()))?;

    #[cfg(target_os = "linux")]
    unsafe {
        use std::os::unix::io::AsRawFd;
        libc::posix_fadvise(vm_file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
    }

    let image_size = block_device_size(&vm_file).context("Failed to get VM device size")?;

    let mut encoder =
        zstd::stream::Encoder::new(Vec::with_capacity(COMPRESSED_CHUNK_SIZE + BLOCK_SIZE), 3)
            .context("Failed to create zstd encoder")?;

    dm_thin::write_delta_header(&mut encoder, BLOCK_SIZE as u32, image_size)?;

    let mut buf = vec![0u8; BLOCK_SIZE];
    let mut blocks_written: u64 = 0;

    for range in ranges {
        // Clamp to device size: thin_delta ranges are in data_block_size
        // units which should align with the device size, but clamp defensively
        // in case of any metadata/kernel discrepancy.
        if range.byte_offset >= image_size {
            continue;
        }
        let mut range_offset = range.byte_offset;
        let range_end = (range.byte_offset + range.byte_length).min(image_size);

        while range_offset < range_end {
            let chunk_len = ((range_end - range_offset) as usize).min(BLOCK_SIZE);

            vm_file
                .read_exact_at(&mut buf[..chunk_len], range_offset)
                .with_context(|| format!("Failed to read VM device at offset {}", range_offset))?;

            // Write block record: offset (u64 LE) + length (u32 LE) + data
            encoder.write_all(&range_offset.to_le_bytes())?;
            encoder.write_all(&(chunk_len as u32).to_le_bytes())?;
            encoder.write_all(&buf[..chunk_len])?;
            blocks_written += 1;

            range_offset += chunk_len as u64;

            if encoder.get_ref().len() >= COMPRESSED_CHUNK_SIZE {
                let data = std::mem::take(encoder.get_mut());
                if tx.blocking_send(Ok(Bytes::from(data))).is_err() {
                    return Ok(());
                }
            }
        }
    }

    let remaining = encoder.finish().context("Failed to finish zstd encoder")?;
    if !remaining.is_empty() {
        let _ = tx.blocking_send(Ok(Bytes::from(remaining)));
    }

    tracing::info!(
        blocks_written,
        image_size,
        "Delta snapshot stream complete (thin_delta path)"
    );

    Ok(())
}

/// Get the size of a block device in bytes.
///
/// On Linux, block devices report size 0 via `stat(2)` /
/// `File::metadata().len()`. The actual size must be queried via `ioctl(fd,
/// BLKGETSIZE64, &size)`. Falls back to `File::metadata().len()` on non-Linux
/// (for tests on macOS with regular files).
fn block_device_size(file: &std::fs::File) -> anyhow::Result<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        // Try stat first — works for regular files (e.g., in tests).
        let stat_size = file.metadata()?.len();
        if stat_size > 0 {
            return Ok(stat_size);
        }

        // Block device: use ioctl(BLKGETSIZE64).
        nix::ioctl_read!(blkgetsize64, 0x12, 114, u64);
        let mut size: u64 = 0;
        // SAFETY: BLKGETSIZE64 writes a u64 to the provided pointer.
        unsafe {
            blkgetsize64(file.as_raw_fd(), &mut size)
                .context("ioctl BLKGETSIZE64 failed on block device")?;
        }
        anyhow::ensure!(size > 0, "Block device reported size 0 via BLKGETSIZE64");
        Ok(size)
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(file.metadata()?.len())
    }
}

/// Derive a short hash from a snapshot URI for use in temp file names.
fn hash_uri(uri: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(uri.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_string()
}
