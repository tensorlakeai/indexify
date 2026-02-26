//! Blob store operations for allocation execution.
//!
//! Provides a metrics-instrumented wrapper around
//! [`indexify_blob_store::BlobStore`], plus presigned URL generation, multipart
//! upload management, and blob download for function executor allocations.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
// Re-export types that consumers use directly.
pub use indexify_blob_store::MultipartUploadHandle;
use opentelemetry::metrics::{Counter, Histogram};
use tokio::sync::RwLock;

use crate::metrics::DataplaneMetrics;

/// Optimal chunk size for S3 multipart uploads (100 MB).
pub const BLOB_OPTIMAL_CHUNK_SIZE: u64 = 100 * 1024 * 1024;

/// Max optimal chunks before switching to slower larger chunks.
const OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: u64 = 100;

/// Slower chunk size (1 GB) used when > OPTIMAL_CHUNKS_COUNT chunks.
const OUTPUT_BLOB_SLOWER_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;

/// Request error blob maximum size (10 MB).
pub const REQUEST_ERROR_MAX_SIZE: u64 = 10 * 1024 * 1024;

/// Record metrics around a blob store operation: request count, latency, and
/// error count.
async fn record_blob_op<T>(
    requests: &Counter<u64>,
    latency: &Histogram<f64>,
    errors: &Counter<u64>,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    requests.add(1, &[]);
    let start = std::time::Instant::now();
    let result = fut.await;
    latency.record(start.elapsed().as_secs_f64(), &[]);
    if result.is_err() {
        errors.add(1, &[]);
    }
    result
}

/// Metrics-instrumented blob store with dynamic URI-based backend dispatch.
///
/// Holds a local FS backend (always available, zero-cost) and a per-bucket
/// cache of S3 backends. Each S3 client is created with the correct region
/// for its bucket (auto-detected via `GetBucketLocation`), supporting
/// multi-tenant deployments where different namespaces use buckets in
/// different AWS regions.
#[derive(Clone)]
pub struct BlobStore {
    local: indexify_blob_store::BlobStore,
    /// Per-bucket S3 clients, keyed by bucket name. Each client is configured
    /// for the bucket's actual region.
    s3_clients: Arc<RwLock<HashMap<String, indexify_blob_store::BlobStore>>>,
    metrics: Arc<DataplaneMetrics>,
}

impl BlobStore {
    /// Create a new blob store with dynamic URI-based dispatch.
    ///
    /// The local FS backend is always available. S3 backends are lazily
    /// initialized per-bucket on the first URI encountered for each bucket,
    /// with automatic region detection.
    pub fn new(metrics: Arc<DataplaneMetrics>) -> Self {
        Self {
            local: indexify_blob_store::BlobStore::new_local("file:///"),
            s3_clients: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        }
    }

    /// Return the backend appropriate for the given URI.
    ///
    /// For S3 URIs, returns a client configured for the bucket's region.
    /// Clients are cached per-bucket so region detection only happens once
    /// per bucket.
    async fn backend_for(&self, uri: &str) -> Result<indexify_blob_store::BlobStore> {
        if indexify_blob_store::uri::is_file_uri(uri) {
            return Ok(self.local.clone());
        }

        let (bucket, _key) = indexify_blob_store::uri::parse_s3_uri(uri)?;

        // Fast path: check if we already have a client for this bucket.
        {
            let clients = self.s3_clients.read().await;
            if let Some(client) = clients.get(&bucket) {
                return Ok(client.clone());
            }
        }

        // Slow path: detect the bucket's region and create a new client.
        let region = match indexify_blob_store::detect_bucket_region(&bucket).await {
            Ok(region) => {
                tracing::info!(
                    bucket = %bucket,
                    region = %region,
                    "Detected S3 bucket region"
                );
                Some(region)
            }
            Err(e) => {
                tracing::warn!(
                    bucket = %bucket,
                    error = %e,
                    "Failed to detect S3 bucket region, falling back to default"
                );
                None
            }
        };

        let base_url = format!("s3://{bucket}");
        let new_client = indexify_blob_store::BlobStore::new_s3(&base_url, region).await?;

        let mut clients = self.s3_clients.write().await;
        let client = clients.entry(bucket).or_insert(new_client);
        Ok(client.clone())
    }

    /// Get metadata (size) for a blob.
    pub async fn get_metadata(&self, uri: &str) -> Result<indexify_blob_store::BlobMetadata> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_get_metadata_requests,
            &self
                .metrics
                .histograms
                .blob_store_get_metadata_latency_seconds,
            &self.metrics.counters.blob_store_get_metadata_errors,
            backend.get_metadata(uri),
        )
        .await
    }

    /// Generate a presigned GET URL for reading a blob.
    pub async fn presign_get_uri(&self, uri: &str) -> Result<String> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_presign_uri_requests,
            &self
                .metrics
                .histograms
                .blob_store_presign_uri_latency_seconds,
            &self.metrics.counters.blob_store_presign_uri_errors,
            backend.presign_get_uri(uri),
        )
        .await
    }

    /// Download a blob and return its contents.
    pub async fn get(&self, uri: &str) -> Result<Bytes> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_get_requests,
            &self.metrics.histograms.blob_store_get_latency_seconds,
            &self.metrics.counters.blob_store_get_errors,
            backend.get(uri),
        )
        .await
    }

    /// Write data from a stream to a blob.
    pub async fn put(
        &self,
        uri: &str,
        data: impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin,
        options: indexify_blob_store::PutOptions,
    ) -> Result<indexify_blob_store::PutResult> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_put_requests,
            &self.metrics.histograms.blob_store_put_latency_seconds,
            &self.metrics.counters.blob_store_put_errors,
            backend.put(uri, data, options),
        )
        .await
    }

    /// Stream a blob's contents.
    pub async fn get_stream(
        &self,
        uri: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<Bytes>>> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_get_requests,
            &self.metrics.histograms.blob_store_get_latency_seconds,
            &self.metrics.counters.blob_store_get_errors,
            backend.get_stream(uri, None),
        )
        .await
    }

    /// Create a multipart upload session.
    pub async fn create_multipart_upload(&self, uri: &str) -> Result<MultipartUploadHandle> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self
                .metrics
                .counters
                .blob_store_create_multipart_upload_requests,
            &self
                .metrics
                .histograms
                .blob_store_create_multipart_upload_latency_seconds,
            &self
                .metrics
                .counters
                .blob_store_create_multipart_upload_errors,
            backend.create_multipart_upload(uri),
        )
        .await
    }

    /// Generate a presigned URL for uploading a part in a multipart upload.
    pub async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: i32,
        upload_id: &str,
    ) -> Result<String> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self.metrics.counters.blob_store_presign_uri_requests,
            &self
                .metrics
                .histograms
                .blob_store_presign_uri_latency_seconds,
            &self.metrics.counters.blob_store_presign_uri_errors,
            backend.presign_upload_part_uri(uri, part_number, upload_id),
        )
        .await
    }

    /// Complete a multipart upload.
    pub async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: &[String],
    ) -> Result<()> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self
                .metrics
                .counters
                .blob_store_complete_multipart_upload_requests,
            &self
                .metrics
                .histograms
                .blob_store_complete_multipart_upload_latency_seconds,
            &self
                .metrics
                .counters
                .blob_store_complete_multipart_upload_errors,
            backend.complete_multipart_upload(uri, upload_id, parts_etags),
        )
        .await
    }

    /// Abort a multipart upload, cleaning up resources.
    pub async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
        let backend = self.backend_for(uri).await?;
        record_blob_op(
            &self
                .metrics
                .counters
                .blob_store_abort_multipart_upload_requests,
            &self
                .metrics
                .histograms
                .blob_store_abort_multipart_upload_latency_seconds,
            &self
                .metrics
                .counters
                .blob_store_abort_multipart_upload_errors,
            backend.abort_multipart_upload(uri, upload_id),
        )
        .await
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metrics() -> Arc<DataplaneMetrics> {
        Arc::new(DataplaneMetrics::default())
    }

    #[tokio::test]
    async fn test_local_presign_get_returns_same_uri() {
        let store = BlobStore::new(test_metrics());
        let uri = "file:///tmp/test/blob.bin";
        let result = store.presign_get_uri(uri).await.unwrap();
        assert_eq!(result, uri);
    }

    #[tokio::test]
    async fn test_local_create_and_abort_multipart() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/test_blob", dir.path().display());
        let store = BlobStore::new(test_metrics());

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
        let store = BlobStore::new(test_metrics());
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
        let store = BlobStore::new(test_metrics());
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
        let store = BlobStore::new(test_metrics());
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

        let store = BlobStore::new(test_metrics());
        let result = store.get(&uri).await.unwrap();
        assert_eq!(result.as_ref(), data);
    }

    #[tokio::test]
    async fn test_s3_uri_triggers_s3_backend() {
        let store = BlobStore::new(test_metrics());
        assert!(store.s3_clients.read().await.is_empty());
        // An S3 URI should attempt to initialize an S3 backend for that bucket.
        // Without real AWS credentials, detect_bucket_region will fail but
        // backend_for falls back to creating a client with the default region.
        let result = store.backend_for("s3://test-bucket/key").await;
        assert!(result.is_ok());
        assert!(store.s3_clients.read().await.contains_key("test-bucket"));
    }

    #[tokio::test]
    async fn test_different_buckets_get_separate_clients() {
        let store = BlobStore::new(test_metrics());
        let _ = store.backend_for("s3://bucket-a/key1").await.unwrap();
        let _ = store.backend_for("s3://bucket-b/key2").await.unwrap();
        let clients = store.s3_clients.read().await;
        assert!(clients.contains_key("bucket-a"));
        assert!(clients.contains_key("bucket-b"));
        assert_eq!(clients.len(), 2);
    }

    #[tokio::test]
    async fn test_same_bucket_reuses_client() {
        let store = BlobStore::new(test_metrics());
        let _ = store.backend_for("s3://bucket-a/key1").await.unwrap();
        let _ = store.backend_for("s3://bucket-a/key2").await.unwrap();
        let clients = store.s3_clients.read().await;
        assert_eq!(clients.len(), 1);
    }

    #[tokio::test]
    async fn test_cloned_store_shares_s3_clients() {
        let store = BlobStore::new(test_metrics());
        let clone = store.clone();
        // Initialize via the original
        let _ = store.backend_for("s3://bucket/key").await.unwrap();
        // Clone should see the same cached client
        assert!(clone.s3_clients.read().await.contains_key("bucket"));
    }

    #[tokio::test]
    async fn test_file_uri_uses_local_backend() {
        let store = BlobStore::new(test_metrics());
        let _ = store.backend_for("file:///tmp/test").await.unwrap();
        // file:// URIs should not create any S3 clients
        assert!(store.s3_clients.read().await.is_empty());
    }
}
