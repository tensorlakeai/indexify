//! S3 backend implementation for blob store operations.

use std::{ops::Range, time::Duration};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::{Client as S3Client, presigning::PresigningConfig};
use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::{BlobMetadata, MultipartUploadHandle, PutOptions, PutResult};

/// Minimum part size for S3 multipart uploads (5 MB).
const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

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

        if buffer.len() >= MIN_PART_SIZE {
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
