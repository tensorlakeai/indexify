//! S3 backend implementation for blob store operations.

use std::{ops::Range, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::{Client as S3Client, presigning::PresigningConfig};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::{BlobMetadata, MultipartUploadHandle, PutOptions, PutResult};

/// Part size for S3 multipart uploads (100 MB).
///
/// S3 requires a minimum of 5 MB per part, but larger parts reduce the number
/// of API calls and improve throughput for big uploads (e.g. a 10 GB snapshot
/// is 100 parts at 100 MB vs 2,000 parts at 5 MB).
const PART_SIZE: usize = 100 * 1024 * 1024;

pub(crate) async fn get(client: &S3Client, bucket: &str, key: &str) -> Result<Bytes> {
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 get_object failed")?;
    let body = resp
        .body
        .collect()
        .await
        .context("Failed to read S3 object body")?;
    Ok(body.into_bytes())
}

pub(crate) async fn get_stream(
    client: &S3Client,
    bucket: &str,
    key: &str,
    range: Option<Range<u64>>,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    let mut req = client.get_object().bucket(bucket).key(key);
    if let Some(range) = &range &&
        range.end > range.start
    {
        req = req.range(format!("bytes={}-{}", range.start, range.end - 1));
    }
    let resp = req.send().await.context("S3 get_object failed")?;
    let reader = resp.body.into_async_read();
    let stream = ReaderStream::new(reader).map(|r| r.map_err(|e| anyhow!("S3 read error: {}", e)));
    Ok(Box::pin(stream))
}

/// Part size for concurrent S3 downloads (8 MB).
///
/// Each concurrent GetObject request fetches one part. Smaller parts increase
/// parallelism but add per-request overhead. 8 MB balances latency and
/// throughput for typical snapshot sizes (100 MB – 1 GB).
const DOWNLOAD_PART_SIZE: u64 = 8 * 1024 * 1024;

/// Maximum number of concurrent GetObject range requests.
const DOWNLOAD_CONCURRENCY: usize = 8;

/// Maximum parts in the FuturesOrdered buffer (in-flight + completed but
/// not yet forwarded). Caps peak memory to roughly
/// `MAX_BUFFERED_PARTS × DOWNLOAD_PART_SIZE` ≈ 200 MB.
const MAX_BUFFERED_PARTS: usize = 24;

/// Minimum object size to use concurrent downloads. Below this threshold a
/// single GetObject is faster because the HeadObject + request fan-out
/// overhead exceeds the benefit of parallelism.
const CONCURRENT_DOWNLOAD_THRESHOLD: u64 = 16 * 1024 * 1024; // 16 MB

/// Download an S3 object using concurrent byte-range GetObject requests.
///
/// Issues up to [`DOWNLOAD_CONCURRENCY`] parallel range requests and yields
/// parts in order. For objects smaller than [`CONCURRENT_DOWNLOAD_THRESHOLD`],
/// falls back to a single GetObject request.
pub(crate) async fn get_stream_concurrent(
    client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<BoxStream<'static, Result<Bytes>>> {
    // Get object size.
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 head_object failed for concurrent download")?;
    let size = head.content_length().unwrap_or(0) as u64;

    if size <= CONCURRENT_DOWNLOAD_THRESHOLD {
        return get_stream(client, bucket, key, None).await;
    }

    let num_parts = size.div_ceil(DOWNLOAD_PART_SIZE);

    tracing::info!(
        bucket,
        key,
        size,
        num_parts,
        part_size = DOWNLOAD_PART_SIZE,
        concurrency = DOWNLOAD_CONCURRENCY,
        "Starting concurrent S3 download"
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes>>(4);
    let client = client.clone();
    let bucket = bucket.to_string();
    let key = key.to_string();

    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(DOWNLOAD_CONCURRENCY));
        let mut futs = futures::stream::FuturesOrdered::new();
        let mut buffered: usize = 0;

        let mut part_start = 0u64;
        while part_start < size {
            // Drain completed parts before spawning more to cap memory.
            while buffered >= MAX_BUFFERED_PARTS {
                match futs.next().await {
                    Some(result) => {
                        buffered -= 1;
                        let bytes = match result {
                            Ok(Ok(b)) => Ok(b),
                            Ok(Err(e)) => Err(e),
                            Err(e) => Err(anyhow!("S3 download task panicked: {}", e)),
                        };
                        if tx.send(bytes).await.is_err() {
                            return;
                        }
                    }
                    None => break,
                }
            }

            let part_end = (part_start + DOWNLOAD_PART_SIZE).min(size) - 1;
            let range_header = format!("bytes={}-{}", part_start, part_end);

            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let permit = sem.clone().acquire_owned().await.unwrap();

            futs.push_back(tokio::spawn(async move {
                let result = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .range(&range_header)
                    .send()
                    .await;
                drop(permit);
                match result {
                    Ok(resp) => resp
                        .body
                        .collect()
                        .await
                        .map(|b| b.into_bytes())
                        .map_err(|e| anyhow!("S3 range read error: {}", e)),
                    Err(e) => Err(anyhow!("S3 GetObject range failed: {}", e)),
                }
            }));

            buffered += 1;
            part_start += DOWNLOAD_PART_SIZE;
        }

        // Drain remaining parts.
        while let Some(result) = futs.next().await {
            let bytes = match result {
                Ok(Ok(b)) => Ok(b),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(anyhow!("S3 download task panicked: {}", e)),
            };
            if tx.send(bytes).await.is_err() {
                break;
            }
        }
    });

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
}

pub(crate) async fn get_metadata(
    client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<BlobMetadata> {
    let resp = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 head_object failed")?;
    Ok(BlobMetadata {
        size_bytes: u64::try_from(resp.content_length().unwrap_or(0)).unwrap_or(0),
    })
}

pub(crate) async fn put(
    client: &S3Client,
    bucket: &str,
    key: &str,
    data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    options: &PutOptions,
) -> Result<PutResult> {
    let create_resp = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 create_multipart_upload failed")?;
    let upload_id = create_resp
        .upload_id()
        .ok_or_else(|| anyhow!("No upload_id in create_multipart_upload response"))?
        .to_string();

    match put_parts(client, bucket, key, &upload_id, data, options).await {
        Ok(result) => Ok(result),
        Err(e) => {
            let _ = client
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .send()
                .await;
            Err(e)
        }
    }
}

async fn put_parts(
    client: &S3Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    options: &PutOptions,
) -> Result<PutResult> {
    use sha2::{Digest, Sha256};

    let mut hasher = if options.compute_sha256 {
        Some(Sha256::new())
    } else {
        None
    };
    let mut buffer = Vec::new();
    let mut parts = Vec::new();
    let mut part_number = 1i32;
    let mut total_size = 0u64;

    futures::pin_mut!(data);
    while let Some(chunk_result) = data.next().await {
        let chunk = chunk_result?;
        total_size += chunk.len() as u64;
        if let Some(h) = &mut hasher {
            h.update(&chunk);
        }
        buffer.extend_from_slice(&chunk);

        if buffer.len() >= PART_SIZE {
            let part_data = Bytes::from(std::mem::take(&mut buffer));
            let resp = client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(part_data.into())
                .send()
                .await
                .context("S3 upload_part failed")?;
            parts.push(
                aws_sdk_s3::types::CompletedPart::builder()
                    .e_tag(resp.e_tag().unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );
            part_number += 1;
        }
    }

    // Upload remaining buffer (or a single empty part if no data was written)
    if !buffer.is_empty() || parts.is_empty() {
        let part_data = Bytes::from(std::mem::take(&mut buffer));
        let resp = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(part_data.into())
            .send()
            .await
            .context("S3 upload_part (final) failed")?;
        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(resp.e_tag().unwrap_or_default())
                .part_number(part_number)
                .build(),
        );
    }

    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();
    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .context("S3 complete_multipart_upload failed")?;

    debug!(bucket, key, upload_id, "Completed streaming put");

    Ok(PutResult {
        uri: format!("s3://{}/{}", bucket, key),
        size_bytes: total_size,
        sha256_hash: hasher.map(|h| format!("{:x}", h.finalize())),
    })
}

pub(crate) async fn delete(client: &S3Client, bucket: &str, key: &str) -> Result<()> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 delete_object failed")?;
    Ok(())
}

pub(crate) async fn presign_get(
    client: &S3Client,
    bucket: &str,
    key: &str,
    ttl: Duration,
) -> Result<String> {
    let presigning = PresigningConfig::builder()
        .expires_in(ttl)
        .build()
        .context("Failed to build presigning config")?;
    let request = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .presigned(presigning)
        .await
        .context("Failed to presign GET URL")?;
    Ok(request.uri().to_string())
}

pub(crate) async fn create_multipart_upload(
    client: &S3Client,
    bucket: &str,
    key: &str,
) -> Result<MultipartUploadHandle> {
    let resp = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 create_multipart_upload failed")?;
    let upload_id = resp
        .upload_id()
        .ok_or_else(|| anyhow!("No upload_id in create_multipart_upload response"))?
        .to_string();
    debug!(bucket, key, upload_id = %upload_id, "Created multipart upload");
    Ok(MultipartUploadHandle {
        uri: format!("s3://{}/{}", bucket, key),
        upload_id,
    })
}

pub(crate) async fn presign_upload_part(
    client: &S3Client,
    bucket: &str,
    key: &str,
    part_number: i32,
    upload_id: &str,
    ttl: Duration,
) -> Result<String> {
    let presigning = PresigningConfig::builder()
        .expires_in(ttl)
        .build()
        .context("Failed to build presigning config")?;
    let request = client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(part_number)
        .presigned(presigning)
        .await
        .context("Failed to presign upload_part URL")?;
    Ok(request.uri().to_string())
}

pub(crate) async fn complete_multipart_upload(
    client: &S3Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts_etags: &[String],
) -> Result<()> {
    let parts: Vec<aws_sdk_s3::types::CompletedPart> = parts_etags
        .iter()
        .enumerate()
        .map(|(i, etag)| {
            aws_sdk_s3::types::CompletedPart::builder()
                .e_tag(etag)
                .part_number((i + 1) as i32)
                .build()
        })
        .collect();

    let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .context("S3 complete_multipart_upload failed")?;

    debug!(bucket, key, upload_id, "Completed multipart upload");
    Ok(())
}

pub(crate) async fn abort_multipart_upload(
    client: &S3Client,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<()> {
    client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .context("S3 abort_multipart_upload failed")?;
    debug!(bucket, key, upload_id, "Aborted multipart upload");
    Ok(())
}
