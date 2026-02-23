//! Blob store operations for allocation execution.
//!
//! Provides a metrics-instrumented wrapper around
//! [`indexify_blob_store::BlobStore`], plus presigned URL generation, multipart
//! upload management, and blob download for function executor allocations.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
// Re-export types that consumers use directly.
pub use indexify_blob_store::MultipartUploadHandle;
use opentelemetry::metrics::{Counter, Histogram};

use crate::metrics::DataplaneMetrics;

/// Lazily-initialized blob store that detects the backend (S3 vs local FS)
/// from the URI scheme of the first request it sees.
///
/// The server sends full URIs (`s3://…` or `file://…`) with every snapshot
/// command. `LazyBlobStore` uses the first URI to create the right
/// [`BlobStore`] client and caches it for all subsequent calls.
pub struct LazyBlobStore {
    client: tokio::sync::OnceCell<BlobStore>,
    metrics: Arc<DataplaneMetrics>,
}

impl LazyBlobStore {
    /// Create a new lazy blob store. The underlying client will be initialized
    /// on the first operation.
    pub fn new(metrics: Arc<DataplaneMetrics>) -> Self {
        Self {
            client: tokio::sync::OnceCell::new(),
            metrics,
        }
    }

    /// Return the cached client, initializing it from `uri` on the first call.
    async fn get_or_init(&self, uri: &str) -> Result<&BlobStore> {
        self.client
            .get_or_try_init(|| async { BlobStore::from_uri(uri, self.metrics.clone()).await })
            .await
    }

    /// Write data from a stream to a blob.
    pub async fn put(
        &self,
        uri: &str,
        data: impl futures_util::Stream<Item = Result<Bytes>> + Send + Unpin,
        options: indexify_blob_store::PutOptions,
    ) -> Result<indexify_blob_store::PutResult> {
        self.get_or_init(uri).await?.put(uri, data, options).await
    }

    /// Stream a blob's contents.
    pub async fn get_stream(
        &self,
        uri: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<Bytes>>> {
        self.get_or_init(uri).await?.get_stream(uri).await
    }
}

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

/// Metrics-instrumented blob store wrapping [`indexify_blob_store::BlobStore`].
#[derive(Clone)]
pub struct BlobStore {
    inner: indexify_blob_store::BlobStore,
    metrics: Arc<DataplaneMetrics>,
}

impl BlobStore {
    /// Create a local filesystem blob store.
    ///
    /// The base URL is not used for operations (the dataplane always passes
    /// full URIs), but is required by the shared crate constructor.
    pub fn new_local(metrics: Arc<DataplaneMetrics>) -> Self {
        Self {
            inner: indexify_blob_store::BlobStore::new_local("file:///"),
            metrics,
        }
    }

    /// Auto-detect backend from a URI scheme.
    pub async fn from_uri(uri: &str, metrics: Arc<DataplaneMetrics>) -> Result<Self> {
        let inner = indexify_blob_store::BlobStore::from_uri(uri).await?;
        Ok(Self { inner, metrics })
    }

    /// Get metadata (size) for a blob.
    pub async fn get_metadata(&self, uri: &str) -> Result<indexify_blob_store::BlobMetadata> {
        record_blob_op(
            &self.metrics.counters.blob_store_get_metadata_requests,
            &self
                .metrics
                .histograms
                .blob_store_get_metadata_latency_seconds,
            &self.metrics.counters.blob_store_get_metadata_errors,
            self.inner.get_metadata(uri),
        )
        .await
    }

    /// Generate a presigned GET URL for reading a blob.
    pub async fn presign_get_uri(&self, uri: &str) -> Result<String> {
        record_blob_op(
            &self.metrics.counters.blob_store_presign_uri_requests,
            &self
                .metrics
                .histograms
                .blob_store_presign_uri_latency_seconds,
            &self.metrics.counters.blob_store_presign_uri_errors,
            self.inner.presign_get_uri(uri),
        )
        .await
    }

    /// Download a blob and return its contents.
    pub async fn get(&self, uri: &str) -> Result<Bytes> {
        record_blob_op(
            &self.metrics.counters.blob_store_get_requests,
            &self.metrics.histograms.blob_store_get_latency_seconds,
            &self.metrics.counters.blob_store_get_errors,
            self.inner.get(uri),
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
        record_blob_op(
            &self.metrics.counters.blob_store_put_requests,
            &self.metrics.histograms.blob_store_put_latency_seconds,
            &self.metrics.counters.blob_store_put_errors,
            self.inner.put(uri, data, options),
        )
        .await
    }

    /// Stream a blob's contents.
    pub async fn get_stream(
        &self,
        uri: &str,
    ) -> Result<futures_util::stream::BoxStream<'static, Result<Bytes>>> {
        record_blob_op(
            &self.metrics.counters.blob_store_get_requests,
            &self.metrics.histograms.blob_store_get_latency_seconds,
            &self.metrics.counters.blob_store_get_errors,
            self.inner.get_stream(uri, None),
        )
        .await
    }

    /// Create a multipart upload session.
    pub async fn create_multipart_upload(&self, uri: &str) -> Result<MultipartUploadHandle> {
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
            self.inner.create_multipart_upload(uri),
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
        self.inner
            .presign_upload_part_uri(uri, part_number, upload_id)
            .await
    }

    /// Complete a multipart upload.
    pub async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: &[String],
    ) -> Result<()> {
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
            self.inner
                .complete_multipart_upload(uri, upload_id, parts_etags),
        )
        .await
    }

    /// Abort a multipart upload, cleaning up resources.
    pub async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> Result<()> {
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
            self.inner.abort_multipart_upload(uri, upload_id),
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
        let store = BlobStore::new_local(test_metrics());
        let uri = "file:///tmp/test/blob.bin";
        let result = store.presign_get_uri(uri).await.unwrap();
        assert_eq!(result, uri);
    }

    #[tokio::test]
    async fn test_local_create_and_abort_multipart() {
        let dir = tempfile::tempdir().unwrap();
        let uri = format!("file://{}/test_blob", dir.path().display());
        let store = BlobStore::new_local(test_metrics());

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
        let store = BlobStore::new_local(test_metrics());
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
        let store = BlobStore::new_local(test_metrics());
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
        let store = BlobStore::new_local(test_metrics());
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

        let store = BlobStore::new_local(test_metrics());
        let result = store.get(&uri).await.unwrap();
        assert_eq!(result.as_ref(), data);
    }
}
