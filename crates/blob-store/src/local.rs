//! Local filesystem backend implementation for blob store operations.

use std::{ops::Range, path::Path};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::{BlobMetadata, MultipartUploadHandle, PutOptions, PutResult};

pub(crate) async fn get(path: &Path) -> Result<Bytes> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("Failed to read local file: {}", path.display()))?;
    Ok(Bytes::from(data))
}

/// Map a `ReaderStream` into an `anyhow::Result<Bytes>` stream.
fn into_anyhow_stream(
    stream: impl futures::Stream<Item = std::result::Result<Bytes, std::io::Error>> + Send + 'static,
) -> BoxStream<'static, Result<Bytes>> {
    Box::pin(stream.map(|r| r.map_err(|e| anyhow::anyhow!("File read error: {}", e))))
}

pub(crate) async fn get_stream(
    path: &Path,
    range: Option<Range<u64>>,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("Failed to open local file: {}", path.display()))?;

    if let Some(range) = range {
        let mut file = file;
        file.seek(std::io::SeekFrom::Start(range.start))
            .await
            .with_context(|| format!("Failed to seek in file: {}", path.display()))?;
        let limited = file.take(range.end.saturating_sub(range.start));
        Ok(into_anyhow_stream(ReaderStream::new(limited)))
    } else {
        Ok(into_anyhow_stream(ReaderStream::new(file)))
    }
}

pub(crate) async fn get_metadata(path: &Path) -> Result<BlobMetadata> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("Failed to get metadata for: {}", path.display()))?;
    Ok(BlobMetadata {
        size_bytes: metadata.len(),
    })
}

pub(crate) async fn put(
    path: &Path,
    data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    options: &PutOptions,
) -> Result<PutResult> {
    use sha2::{Digest, Sha256};

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut file = tokio::fs::File::create(path)
        .await
        .with_context(|| format!("Failed to create file: {}", path.display()))?;

    let mut hasher = if options.compute_sha256 {
        Some(Sha256::new())
    } else {
        None
    };
    let mut total_size = 0u64;

    futures::pin_mut!(data);
    while let Some(chunk_result) = data.next().await {
        let chunk = chunk_result?;
        total_size += chunk.len() as u64;
        if let Some(h) = &mut hasher {
            h.update(&chunk);
        }
        file.write_all(&chunk)
            .await
            .with_context(|| format!("Failed to write to file: {}", path.display()))?;
    }

    file.flush().await?;

    Ok(PutResult {
        uri: format!("file://{}", path.display()),
        size_bytes: total_size,
        sha256_hash: hasher.map(|h| format!("{:x}", h.finalize())),
    })
}

pub(crate) async fn delete(path: &Path) -> Result<()> {
    tokio::fs::remove_file(path)
        .await
        .with_context(|| format!("Failed to delete file: {}", path.display()))?;
    Ok(())
}

pub(crate) async fn create_multipart_upload(path: &Path) -> Result<MultipartUploadHandle> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }
    Ok(MultipartUploadHandle {
        uri: format!("file://{}", path.display()),
        upload_id: "local-multipart-upload-id".to_string(),
    })
}

pub(crate) async fn abort_multipart_upload(path: &Path) -> Result<()> {
    let _ = tokio::fs::remove_file(path).await;
    Ok(())
}
