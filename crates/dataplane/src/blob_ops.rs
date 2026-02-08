//! Blob store operations for allocation execution.
//!
//! Provides presigned URL generation, multipart upload management, and blob
//! download for function executor allocations. Supports both S3 and local
//! filesystem backends.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::{Client as S3Client, presigning::PresigningConfig};
use bytes::Bytes;
use tracing::debug;

use crate::metrics::{DataplaneCounters, DataplaneHistograms};

/// Maximum presigned URL expiration (7 days, S3 limit).
const MAX_PRESIGN_EXPIRATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Default presign expiration for allocation blobs.
const DEFAULT_PRESIGN_EXPIRATION: Duration = MAX_PRESIGN_EXPIRATION;

/// Optimal chunk size for S3 multipart uploads (100 MB).
pub const BLOB_OPTIMAL_CHUNK_SIZE: u64 = 100 * 1024 * 1024;

/// Max optimal chunks before switching to slower larger chunks.
const OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: u64 = 100;

/// Slower chunk size (1 GB) used when > OPTIMAL_CHUNKS_COUNT chunks.
const OUTPUT_BLOB_SLOWER_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;

/// Request error blob maximum size (10 MB).
pub const REQUEST_ERROR_MAX_SIZE: u64 = 10 * 1024 * 1024;

/// Metadata about a blob (size, etc.).
pub struct BlobMetadata {
    pub size_bytes: u64,
}

/// Blob store operations dispatcher, supporting S3 and local filesystem.
#[derive(Clone)]
pub struct BlobStore {
    inner: BlobStoreInner,
}

#[derive(Clone)]
enum BlobStoreInner {
    S3 { client: S3Client },
    LocalFs,
}

/// Result of initiating a multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUploadHandle {
    pub uri: String,
    pub upload_id: String,
}

impl BlobStore {
    /// Create a new S3-backed blob store.
    pub async fn new_s3() -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = S3Client::new(&config);
        Ok(Self {
            inner: BlobStoreInner::S3 { client },
        })
    }

    /// Create a local filesystem blob store.
    pub fn new_local() -> Self {
        Self {
            inner: BlobStoreInner::LocalFs,
        }
    }

    /// Auto-detect backend from a URI scheme.
    pub async fn from_uri(uri: &str) -> Result<Self> {
        if is_file_uri(uri) {
            Ok(Self::new_local())
        } else {
            Self::new_s3().await
        }
    }

    /// Get metadata (size) for a blob.
    pub async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata> {
        let counters = DataplaneCounters::new();
        let histograms = DataplaneHistograms::new();
        counters.blob_store_get_metadata_requests.add(1, &[]);
        let start = std::time::Instant::now();
        let result = self.get_metadata_inner(uri).await;
        histograms
            .blob_store_get_metadata_latency_seconds
            .record(start.elapsed().as_secs_f64(), &[]);
        if result.is_err() {
            counters.blob_store_get_metadata_errors.add(1, &[]);
        }
        result
    }

    async fn get_metadata_inner(&self, uri: &str) -> Result<BlobMetadata> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                let resp = client
                    .head_object()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .context("S3 head_object failed")?;
                let size = resp.content_length().unwrap_or(0) as u64;
                Ok(BlobMetadata { size_bytes: size })
            }
            BlobStoreInner::LocalFs => {
                let path = file_uri_to_path(uri)?;
                let metadata = tokio::fs::metadata(&path)
                    .await
                    .with_context(|| format!("Failed to get metadata for: {}", path.display()))?;
                Ok(BlobMetadata {
                    size_bytes: metadata.len(),
                })
            }
        }
    }

    /// Generate a presigned GET URL for reading a blob.
    ///
    /// For S3 URIs, returns a presigned HTTPS URL.
    /// For file:// URIs, returns the URI unchanged.
    pub async fn presign_get_uri(&self, uri: &str) -> Result<String> {
        let counters = DataplaneCounters::new();
        let histograms = DataplaneHistograms::new();
        counters.blob_store_presign_uri_requests.add(1, &[]);
        let start = std::time::Instant::now();
        let result = self.presign_get_uri_inner(uri).await;
        histograms
            .blob_store_presign_uri_latency_seconds
            .record(start.elapsed().as_secs_f64(), &[]);
        if result.is_err() {
            counters.blob_store_presign_uri_errors.add(1, &[]);
        }
        result
    }

    async fn presign_get_uri_inner(&self, uri: &str) -> Result<String> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                let presigning = PresigningConfig::builder()
                    .expires_in(DEFAULT_PRESIGN_EXPIRATION)
                    .build()
                    .context("Failed to build presigning config")?;
                let request = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
                    .presigned(presigning)
                    .await
                    .context("Failed to presign GET URL")?;
                Ok(request.uri().to_string())
            }
            BlobStoreInner::LocalFs => {
                // Local FS: URI is directly accessible
                Ok(uri.to_string())
            }
        }
    }

    /// Download a blob and return its contents.
    pub async fn get(&self, uri: &str) -> Result<Bytes> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                let resp = client
                    .get_object()
                    .bucket(&bucket)
                    .key(&key)
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
            BlobStoreInner::LocalFs => {
                let path = file_uri_to_path(uri)?;
                let data = tokio::fs::read(&path)
                    .await
                    .with_context(|| format!("Failed to read local file: {}", path.display()))?;
                Ok(Bytes::from(data))
            }
        }
    }

    /// Create a multipart upload session.
    pub async fn create_multipart_upload(&self, uri: &str) -> Result<MultipartUploadHandle> {
        let counters = DataplaneCounters::new();
        let histograms = DataplaneHistograms::new();
        counters
            .blob_store_create_multipart_upload_requests
            .add(1, &[]);
        let start = std::time::Instant::now();
        let result = self.create_multipart_upload_inner(uri).await;
        histograms
            .blob_store_create_multipart_upload_latency_seconds
            .record(start.elapsed().as_secs_f64(), &[]);
        if result.is_err() {
            counters
                .blob_store_create_multipart_upload_errors
                .add(1, &[]);
        }
        result
    }

    async fn create_multipart_upload_inner(&self, uri: &str) -> Result<MultipartUploadHandle> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                let resp = client
                    .create_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .send()
                    .await
                    .context("S3 create_multipart_upload failed")?;
                let upload_id = resp
                    .upload_id()
                    .ok_or_else(|| anyhow!("No upload_id in create_multipart_upload response"))?
                    .to_string();
                debug!(uri = %uri, upload_id = %upload_id, "Created multipart upload");
                Ok(MultipartUploadHandle {
                    uri: uri.to_string(),
                    upload_id,
                })
            }
            BlobStoreInner::LocalFs => {
                // Local FS: ensure parent directory exists
                let path = file_uri_to_path(uri)?;
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.with_context(|| {
                        format!("Failed to create directory: {}", parent.display())
                    })?;
                }
                Ok(MultipartUploadHandle {
                    uri: uri.to_string(),
                    upload_id: "local-multipart-upload-id".to_string(),
                })
            }
        }
    }

    /// Generate a presigned URL for uploading a part in a multipart upload.
    ///
    /// `part_number` starts from 1.
    pub async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
    ) -> Result<String> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                let presigning = PresigningConfig::builder()
                    .expires_in(DEFAULT_PRESIGN_EXPIRATION)
                    .build()
                    .context("Failed to build presigning config")?;
                let request = client
                    .upload_part()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .part_number(part_number)
                    .presigned(presigning)
                    .await
                    .context("Failed to presign upload_part URL")?;
                Ok(request.uri().to_string())
            }
            BlobStoreInner::LocalFs => Ok(uri.to_string()),
        }
    }

    /// Complete a multipart upload.
    ///
    /// `parts_etags` is an ordered list of ETags, one per part (starting from
    /// part 1).
    pub async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: &[String],
    ) -> Result<()> {
        let counters = DataplaneCounters::new();
        let histograms = DataplaneHistograms::new();
        counters
            .blob_store_complete_multipart_upload_requests
            .add(1, &[]);
        let start = std::time::Instant::now();
        let result = self
            .complete_multipart_upload_inner(uri, upload_id, parts_etags)
            .await;
        histograms
            .blob_store_complete_multipart_upload_latency_seconds
            .record(start.elapsed().as_secs_f64(), &[]);
        if result.is_err() {
            counters
                .blob_store_complete_multipart_upload_errors
                .add(1, &[]);
        }
        result
    }

    async fn complete_multipart_upload_inner(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: &[String],
    ) -> Result<()> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;

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
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .multipart_upload(completed)
                    .send()
                    .await
                    .context("S3 complete_multipart_upload failed")?;

                debug!(uri = %uri, upload_id = %upload_id, "Completed multipart upload");
                Ok(())
            }
            BlobStoreInner::LocalFs => {
                // Local FS: no-op, data was written directly
                Ok(())
            }
        }
    }

    /// Abort a multipart upload, cleaning up resources.
    pub async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
        let counters = DataplaneCounters::new();
        let histograms = DataplaneHistograms::new();
        counters
            .blob_store_abort_multipart_upload_requests
            .add(1, &[]);
        let start = std::time::Instant::now();
        let result = self.abort_multipart_upload_inner(uri, upload_id).await;
        histograms
            .blob_store_abort_multipart_upload_latency_seconds
            .record(start.elapsed().as_secs_f64(), &[]);
        if result.is_err() {
            counters
                .blob_store_abort_multipart_upload_errors
                .add(1, &[]);
        }
        result
    }

    async fn abort_multipart_upload_inner(&self, uri: &str, upload_id: &str) -> Result<()> {
        match &self.inner {
            BlobStoreInner::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                client
                    .abort_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(upload_id)
                    .send()
                    .await
                    .context("S3 abort_multipart_upload failed")?;
                debug!(uri = %uri, upload_id = %upload_id, "Aborted multipart upload");
                Ok(())
            }
            BlobStoreInner::LocalFs => {
                // Local FS: try to clean up the file
                if let Ok(path) = file_uri_to_path(uri) {
                    let _ = tokio::fs::remove_file(&path).await;
                }
                Ok(())
            }
        }
    }
}

/// Generate a read-only BLOB proto with presigned chunk URIs.
///
/// Creates a BLOB with chunks of optimal size, each pointing to a presigned
/// read URL. For S3, a single presigned URL is shared across all chunks
/// (the FE uses Range headers to access individual chunks).
pub async fn presign_read_only_blob(
    blob_id: &str,
    blob_uri: &str,
    blob_size: u64,
    blob_store: &BlobStore,
) -> Result<proto_api::function_executor_pb::Blob> {
    let presigned_uri = blob_store.presign_get_uri(blob_uri).await?;

    let mut chunks = Vec::new();
    let mut total = 0u64;

    while total < blob_size {
        let chunk_size = if total + BLOB_OPTIMAL_CHUNK_SIZE <= blob_size {
            BLOB_OPTIMAL_CHUNK_SIZE
        } else {
            blob_size - total
        };
        total += chunk_size;
        chunks.push(proto_api::function_executor_pb::BlobChunk {
            uri: Some(presigned_uri.clone()),
            size: Some(chunk_size),
            etag: None,
        });
    }

    // If blob_size is 0, create a single empty chunk
    if chunks.is_empty() {
        chunks.push(proto_api::function_executor_pb::BlobChunk {
            uri: Some(presigned_uri),
            size: Some(0),
            etag: None,
        });
    }

    Ok(proto_api::function_executor_pb::Blob {
        id: Some(blob_id.to_string()),
        chunks,
    })
}

/// Generate a write-only BLOB proto with presigned upload-part URIs.
///
/// Creates a multipart upload and generates presigned URIs for each chunk.
/// First 100 chunks use 100 MB size, subsequent chunks use 1 GB.
pub async fn presign_write_only_blob(
    blob_id: &str,
    blob_uri: &str,
    upload_id: &str,
    size: u64,
    blob_store: &BlobStore,
) -> Result<proto_api::function_executor_pb::Blob> {
    let mut chunks = Vec::new();
    let mut total = 0u64;

    while total < size {
        let part_number = (chunks.len() + 1) as i32;

        let upload_chunk_uri = blob_store
            .presign_upload_part_uri(blob_uri, part_number, upload_id)
            .await?;

        let chunk_size = if chunks.len() < OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT as usize {
            BLOB_OPTIMAL_CHUNK_SIZE
        } else {
            OUTPUT_BLOB_SLOWER_CHUNK_SIZE
        };
        let chunk_size = if total + chunk_size <= size {
            chunk_size
        } else {
            size - total
        };

        total += chunk_size;
        chunks.push(proto_api::function_executor_pb::BlobChunk {
            uri: Some(upload_chunk_uri),
            size: Some(chunk_size),
            etag: None,
        });
    }

    // If size is 0, create a single empty chunk
    if chunks.is_empty() {
        let uri = blob_store
            .presign_upload_part_uri(blob_uri, 1, upload_id)
            .await?;
        chunks.push(proto_api::function_executor_pb::BlobChunk {
            uri: Some(uri),
            size: Some(0),
            etag: None,
        });
    }

    Ok(proto_api::function_executor_pb::Blob {
        id: Some(blob_id.to_string()),
        chunks,
    })
}

/// Generate a write-only BLOB proto for an output blob request.
///
/// Creates a multipart upload and returns the handle + BLOB with presigned
/// URIs.
pub async fn create_output_blob(
    allocation_id: &str,
    blob_id: &str,
    uri_prefix: &str,
    size: u64,
    blob_store: &BlobStore,
) -> Result<(MultipartUploadHandle, proto_api::function_executor_pb::Blob)> {
    let blob_uri = format!("{}.{}.output_{}", uri_prefix, allocation_id, blob_id);

    let handle = blob_store.create_multipart_upload(&blob_uri).await?;

    let blob =
        presign_write_only_blob(blob_id, &blob_uri, &handle.upload_id, size, blob_store).await?;

    Ok((handle, blob))
}

// --- URI parsing helpers ---

fn is_file_uri(uri: &str) -> bool {
    uri.starts_with("file://")
}

fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    // s3://bucket-name/key/path → ("bucket-name", "key/path")
    let uri = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("Not an S3 URI: {}", uri))?;
    let (bucket, key) = uri
        .split_once('/')
        .ok_or_else(|| anyhow!("Invalid S3 URI (no key): s3://{}", uri))?;
    Ok((bucket.to_string(), key.to_string()))
}

fn file_uri_to_path(uri: &str) -> Result<PathBuf> {
    let path_str = uri
        .strip_prefix("file://")
        .ok_or_else(|| anyhow!("Not a file URI: {}", uri))?;
    Ok(PathBuf::from(path_str))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri() {
        let (bucket, key) = parse_s3_uri("s3://my-bucket/some/key/path.bin").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "some/key/path.bin");
    }

    #[test]
    fn test_parse_s3_uri_no_key() {
        assert!(parse_s3_uri("s3://my-bucket").is_err());
    }

    #[test]
    fn test_file_uri_to_path() {
        let path = file_uri_to_path("file:///tmp/some/path.bin").unwrap();
        assert_eq!(path, PathBuf::from("/tmp/some/path.bin"));
    }

    #[test]
    fn test_is_file_uri() {
        assert!(is_file_uri("file:///tmp/foo"));
        assert!(!is_file_uri("s3://bucket/key"));
    }

    #[tokio::test]
    async fn test_local_presign_get_returns_same_uri() {
        let store = BlobStore::new_local();
        let uri = "file:///tmp/test/blob.bin";
        let result = store.presign_get_uri(uri).await.unwrap();
        assert_eq!(result, uri);
    }

    #[tokio::test]
    async fn test_local_create_and_abort_multipart() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/test_blob", dir.path().display());
        let store = BlobStore::new_local();

        let handle = store.create_multipart_upload(&uri).await.unwrap();
        assert_eq!(handle.upload_id, "local-multipart-upload-id");

        // Abort should not fail
        store
            .abort_multipart_upload(&uri, &handle.upload_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_presign_read_only_blob_chunks() {
        let store = BlobStore::new_local();
        let uri = "file:///tmp/test.bin";
        let size = 250 * 1024 * 1024u64; // 250 MB → 3 chunks

        let blob = presign_read_only_blob("test-blob", uri, size, &store)
            .await
            .unwrap();

        assert_eq!(blob.id, Some("test-blob".to_string()));
        assert_eq!(blob.chunks.len(), 3);
        // First two chunks: 100 MB each
        assert_eq!(blob.chunks[0].size, Some(BLOB_OPTIMAL_CHUNK_SIZE));
        assert_eq!(blob.chunks[1].size, Some(BLOB_OPTIMAL_CHUNK_SIZE));
        // Last chunk: 50 MB
        assert_eq!(blob.chunks[2].size, Some(50 * 1024 * 1024));
    }

    #[tokio::test]
    async fn test_presign_write_only_blob_chunks() {
        let store = BlobStore::new_local();
        let uri = "file:///tmp/test_output.bin";
        let size = 250 * 1024 * 1024u64; // 250 MB → 3 chunks

        let blob = presign_write_only_blob("output-1", uri, "dummy-upload-id", size, &store)
            .await
            .unwrap();

        assert_eq!(blob.id, Some("output-1".to_string()));
        assert_eq!(blob.chunks.len(), 3);
        assert_eq!(blob.chunks[0].size, Some(BLOB_OPTIMAL_CHUNK_SIZE));
        assert_eq!(blob.chunks[1].size, Some(BLOB_OPTIMAL_CHUNK_SIZE));
        assert_eq!(blob.chunks[2].size, Some(50 * 1024 * 1024));
    }

    #[tokio::test]
    async fn test_presign_read_only_blob_zero_size() {
        let store = BlobStore::new_local();
        let blob = presign_read_only_blob("empty", "file:///tmp/empty", 0, &store)
            .await
            .unwrap();
        assert_eq!(blob.chunks.len(), 1);
        assert_eq!(blob.chunks[0].size, Some(0));
    }

    #[tokio::test]
    async fn test_local_get_and_put() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_data.bin");
        let uri = format!("file://{}", file_path.display());

        // Write data to the file
        let data = b"hello blob store";
        tokio::fs::write(&file_path, data).await.unwrap();

        let store = BlobStore::new_local();
        let result = store.get(&uri).await.unwrap();
        assert_eq!(result.as_ref(), data);
    }
}
