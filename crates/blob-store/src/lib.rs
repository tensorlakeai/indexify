//! Shared blob store crate supporting S3 and local filesystem backends.
//!
//! Provides a unified [`BlobStore`] API for reading, writing, and managing
//! blobs across both S3 and local filesystem backends. Also includes
//! presigned URL generation and multipart upload support.

mod local;
mod s3;
pub mod uri;

use std::{ops::Range, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use futures::stream::BoxStream;
use uri::{file_uri_to_path, is_file_uri, parse_s3_uri};

/// Maximum presigned URL expiration (7 days, S3 limit).
const MAX_PRESIGN_EXPIRATION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Default presign expiration for blob operations.
const DEFAULT_PRESIGN_EXPIRATION: Duration = MAX_PRESIGN_EXPIRATION;

/// Metadata about a blob (size, etc.).
pub struct BlobMetadata {
    pub size_bytes: u64,
}

/// Result of a put operation.
#[derive(Debug, Clone)]
pub struct PutResult {
    /// The full URI of the written blob.
    pub uri: String,
    /// Total bytes written.
    pub size_bytes: u64,
    /// SHA-256 hash of the data, if `PutOptions::compute_sha256` was set.
    pub sha256_hash: Option<String>,
}

/// Options for put operations.
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// Whether to compute a SHA-256 hash of the data as it streams through.
    pub compute_sha256: bool,
}

/// Handle for an in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct MultipartUploadHandle {
    pub uri: String,
    pub upload_id: String,
}

/// Unified blob store supporting S3 and local filesystem backends.
///
/// Created via [`BlobStore::new_s3`], [`BlobStore::new_local`], or
/// [`BlobStore::from_uri`]. All operations take full URIs
/// (`s3://bucket/key` or `file:///path`).
#[derive(Clone)]
pub struct BlobStore {
    inner: Arc<BlobStoreInner>,
}

struct BlobStoreInner {
    backend: BlobStoreBackend,
    base_url: String,
    url_scheme: String,
}

enum BlobStoreBackend {
    S3 { client: S3Client },
    LocalFs,
}

impl BlobStore {
    /// Create a new S3-backed blob store.
    ///
    /// `base_url` should be an S3 URI like `s3://my-bucket` or
    /// `s3://my-bucket/prefix`.
    ///
    /// The S3 client is configured with `force_path_style(true)` to support
    /// S3-compatible endpoints (MinIO, LocalStack) that may use HTTP.
    pub async fn new_s3(base_url: &str, region: Option<String>) -> Result<Self> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region {
            config_loader = config_loader.region(aws_config::Region::new(region));
        }
        let config = config_loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        let client = S3Client::from_conf(s3_config);
        let url: url::Url = base_url.parse().context("Invalid S3 URL")?;
        Ok(Self {
            inner: Arc::new(BlobStoreInner {
                backend: BlobStoreBackend::S3 { client },
                base_url: base_url.to_string(),
                url_scheme: url.scheme().to_string(),
            }),
        })
    }

    /// Create a local filesystem blob store.
    ///
    /// `base_url` should be a file URI like `file:///path/to/storage`.
    pub fn new_local(base_url: &str) -> Self {
        Self {
            inner: Arc::new(BlobStoreInner {
                backend: BlobStoreBackend::LocalFs,
                base_url: base_url.to_string(),
                url_scheme: "file".to_string(),
            }),
        }
    }

    /// Auto-detect backend from a URI scheme.
    ///
    /// - `file://...` → local filesystem
    /// - `s3://...` → S3
    pub async fn from_uri(uri: &str) -> Result<Self> {
        if is_file_uri(uri) {
            Ok(Self::new_local(uri))
        } else {
            Self::new_s3(uri, None).await
        }
    }

    /// Get the base URL this blob store was configured with.
    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    /// Get the URL scheme (e.g., `"file"` or `"s3"`).
    pub fn url_scheme(&self) -> &str {
        &self.inner.url_scheme
    }

    /// Download a blob and return its full contents.
    pub async fn get(&self, uri: &str) -> Result<Bytes> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::get(client, &bucket, &key).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::get(&path).await
            }
        }
    }

    /// Stream a blob's contents, optionally reading only a byte range.
    pub async fn get_stream(
        &self,
        uri: &str,
        range: Option<Range<u64>>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::get_stream(client, &bucket, &key, range).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::get_stream(&path, range).await
            }
        }
    }

    /// Get metadata (currently just size) for a blob.
    pub async fn get_metadata(&self, uri: &str) -> Result<BlobMetadata> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::get_metadata(client, &bucket, &key).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::get_metadata(&path).await
            }
        }
    }

    /// Write data from a stream to a blob.
    ///
    /// For S3, this uses multipart upload internally. For local FS, this
    /// writes directly to the file. Set `PutOptions::compute_sha256` to
    /// compute a SHA-256 hash of the data as it streams through.
    pub async fn put(
        &self,
        uri: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
        options: PutOptions,
    ) -> Result<PutResult> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::put(client, &bucket, &key, data, &options).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::put(&path, data, &options).await
            }
        }
    }

    /// Delete a blob.
    pub async fn delete(&self, uri: &str) -> Result<()> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::delete(client, &bucket, &key).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::delete(&path).await
            }
        }
    }

    /// Generate a presigned GET URL with custom TTL.
    ///
    /// For S3, returns a presigned HTTPS URL. For local FS, returns the URI
    /// unchanged.
    pub async fn presign_get(&self, uri: &str, ttl: Duration) -> Result<String> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::presign_get(client, &bucket, &key, ttl).await
            }
            BlobStoreBackend::LocalFs => Ok(uri.to_string()),
        }
    }

    /// Generate a presigned GET URL with the default expiration (7 days).
    pub async fn presign_get_uri(&self, uri: &str) -> Result<String> {
        self.presign_get(uri, DEFAULT_PRESIGN_EXPIRATION).await
    }

    /// Create a multipart upload session.
    ///
    /// Returns a [`MultipartUploadHandle`] with the upload ID needed for
    /// subsequent part uploads and completion/abort.
    pub async fn create_multipart_upload(&self, uri: &str) -> Result<MultipartUploadHandle> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::create_multipart_upload(client, &bucket, &key).await
            }
            BlobStoreBackend::LocalFs => {
                let path = file_uri_to_path(uri)?;
                local::create_multipart_upload(&path).await
            }
        }
    }

    /// Generate a presigned URL for uploading a single part in a multipart
    /// upload. `part_number` starts from 1.
    pub async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
    ) -> Result<String> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::presign_upload_part(
                    client,
                    &bucket,
                    &key,
                    part_number,
                    upload_id,
                    DEFAULT_PRESIGN_EXPIRATION,
                )
                .await
            }
            BlobStoreBackend::LocalFs => Ok(uri.to_string()),
        }
    }

    /// Complete a multipart upload.
    ///
    /// `parts_etags` must be ordered by part number (starting from part 1).
    pub async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: &[String],
    ) -> Result<()> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::complete_multipart_upload(client, &bucket, &key, upload_id, parts_etags).await
            }
            BlobStoreBackend::LocalFs => Ok(()),
        }
    }

    /// Abort a multipart upload, cleaning up resources.
    pub async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
        match &self.inner.backend {
            BlobStoreBackend::S3 { client } => {
                let (bucket, key) = parse_s3_uri(uri)?;
                s3::abort_multipart_upload(client, &bucket, &key, upload_id).await
            }
            BlobStoreBackend::LocalFs => {
                if let Ok(path) = file_uri_to_path(uri) {
                    local::abort_multipart_upload(&path).await?;
                }
                Ok(())
            }
        }
    }
}

/// Detect the AWS region of an S3 bucket using `HeadBucket`.
///
/// S3 always includes the `x-amz-bucket-region` header in `HeadBucket`
/// responses — even on redirect errors when the client's region doesn't
/// match the bucket's region. This function handles both cases:
/// - **Success**: extracts `bucket_region` from the response.
/// - **Error (redirect)**: extracts `x-amz-bucket-region` from the raw
///   error response headers.
///
/// Only requires `s3:ListBucket` permission (commonly granted).
pub async fn detect_bucket_region(bucket: &str) -> Result<String> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    // Use virtual-hosted style (the default) — force_path_style can interfere
    // with cross-region routing.
    let client = S3Client::new(&config);

    match client.head_bucket().bucket(bucket).send().await {
        Ok(resp) => resp
            .bucket_region()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "HeadBucket for '{bucket}' succeeded but returned no bucket_region header"
                )
            }),
        Err(err) => {
            // On redirect (PermanentRedirect), S3 still includes
            // x-amz-bucket-region in the response headers.
            extract_region_from_sdk_error(&err).ok_or_else(|| {
                anyhow::anyhow!(
                    "HeadBucket for '{bucket}' failed and no region in error response: {err:#}"
                )
            })
        }
    }
}

/// Extract `x-amz-bucket-region` from the raw HTTP response inside an SDK
/// error. Works for both `ServiceError` (parsed error) and `ResponseError`
/// (unparsed response).
fn extract_region_from_sdk_error<E>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> Option<String> {
    use aws_sdk_s3::error::SdkError;

    let headers = match err {
        SdkError::ServiceError(e) => e.raw().headers(),
        SdkError::ResponseError(e) => e.raw().headers(),
        _ => return None,
    };

    headers
        .get("x-amz-bucket-region")
        .map(|v| v.to_string())
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    #[test]
    fn test_new_local() {
        let store = BlobStore::new_local("file:///tmp/test");
        assert_eq!(store.base_url(), "file:///tmp/test");
        assert_eq!(store.url_scheme(), "file");
    }

    #[tokio::test]
    async fn test_from_uri_local() {
        let store = BlobStore::from_uri("file:///tmp/test").await.unwrap();
        assert_eq!(store.url_scheme(), "file");
    }

    #[tokio::test]
    async fn test_local_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test.bin");
        let uri = format!("file://{}", file_path.display());
        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let data = b"hello blob store";
        let data_stream = Box::pin(stream::once(async { Ok(Bytes::from_static(data)) }));
        let result = store
            .put(
                &uri,
                data_stream,
                PutOptions {
                    compute_sha256: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(result.size_bytes, data.len() as u64);
        assert!(result.sha256_hash.is_some());

        let got = store.get(&uri).await.unwrap();
        assert_eq!(got.as_ref(), data);
    }

    #[tokio::test]
    async fn test_local_put_without_sha256() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("test_nosha.bin");
        let uri = format!("file://{}", file_path.display());
        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let data_stream = Box::pin(stream::once(async { Ok(Bytes::from_static(b"data")) }));
        let result = store
            .put(&uri, data_stream, PutOptions::default())
            .await
            .unwrap();
        assert!(result.sha256_hash.is_none());
    }

    #[tokio::test]
    async fn test_local_get_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("meta_test.bin");
        let uri = format!("file://{}", file_path.display());

        tokio::fs::write(&file_path, b"test data").await.unwrap();

        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));
        let meta = store.get_metadata(&uri).await.unwrap();
        assert_eq!(meta.size_bytes, 9);
    }

    #[tokio::test]
    async fn test_local_get_stream_full() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("stream_test.bin");
        let uri = format!("file://{}", file_path.display());

        tokio::fs::write(&file_path, b"hello world").await.unwrap();

        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let mut stream = store.get_stream(&uri, None).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            data.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn test_local_get_stream_range() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("range_test.bin");
        let uri = format!("file://{}", file_path.display());

        tokio::fs::write(&file_path, b"hello world").await.unwrap();

        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let mut stream = store.get_stream(&uri, Some(6..11)).await.unwrap();
        let mut data = Vec::new();
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            data.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(data, b"world");
    }

    #[tokio::test]
    async fn test_local_delete() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("delete_test.bin");
        let uri = format!("file://{}", file_path.display());

        tokio::fs::write(&file_path, b"to delete").await.unwrap();
        assert!(file_path.exists());

        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));
        store.delete(&uri).await.unwrap();
        assert!(!file_path.exists());
    }

    #[tokio::test]
    async fn test_local_presign_get_returns_same_uri() {
        let store = BlobStore::new_local("file:///tmp/test");
        let uri = "file:///tmp/test/blob.bin";
        let result = store.presign_get_uri(uri).await.unwrap();
        assert_eq!(result, uri);
    }

    #[tokio::test]
    async fn test_local_multipart_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/test_blob", dir.path().display());
        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let handle = store.create_multipart_upload(&uri).await.unwrap();
        assert_eq!(handle.upload_id, "local-multipart-upload-id");

        // Abort should not fail
        store
            .abort_multipart_upload(&uri, &handle.upload_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_local_multipart_complete() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/test_blob", dir.path().display());
        let store = BlobStore::new_local(&format!("file://{}", dir.path().display()));

        let handle = store.create_multipart_upload(&uri).await.unwrap();

        // Complete should not fail (no-op for local FS)
        store
            .complete_multipart_upload(&uri, &handle.upload_id, &[])
            .await
            .unwrap();
    }
}
