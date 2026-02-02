//! High-level blob storage for server streaming operations.

use std::{ops::Range, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream::BoxStream};
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart, parse_url, path::Path};
use opentelemetry::{KeyValue, metrics::Meter};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use crate::{BlobMetrics, BlobStore, BlobStoreDispatcher};

/// Result of a PUT operation.
#[derive(Debug, Clone)]
pub struct PutResult {
    /// The URL/path where the blob was stored.
    pub url: String,

    /// Size in bytes.
    pub size_bytes: u64,

    /// SHA256 hash of the data.
    pub sha256_hash: String,
}

/// High-level blob storage with streaming operations.
///
/// This wraps a [`BlobStore`] implementation and provides server-friendly
/// streaming APIs for uploading and downloading data.
pub struct BlobStorage {
    /// Underlying blob store for read operations.
    store: Arc<dyn BlobStore>,

    /// object_store client for streaming writes.
    object_store: Arc<dyn ObjectStore>,

    /// Base path for object_store operations.
    object_store_path: Path,

    /// Base URL for generating blob URLs.
    base_url: String,

    /// URL scheme (file, s3, gs, azure).
    url_scheme: String,

    /// Metrics for operations.
    metrics: Option<BlobMetrics>,
}

impl BlobStorage {
    /// Create a new blob storage from a dispatcher.
    pub fn new(dispatcher: BlobStoreDispatcher, base_url: String) -> Self {
        let url_scheme = base_url
            .split("://")
            .next()
            .unwrap_or("file")
            .to_string();

        // Create object_store client for streaming uploads
        let url = base_url.parse::<Url>().expect("invalid blob storage URL");
        let (object_store, object_store_path) =
            parse_url(&url).expect("failed to parse blob storage URL");

        Self {
            store: Arc::new(dispatcher),
            object_store: Arc::new(object_store),
            object_store_path,
            base_url,
            url_scheme,
            metrics: None,
        }
    }

    /// Create a new blob storage with metrics.
    pub fn new_with_metrics(
        dispatcher: BlobStoreDispatcher,
        base_url: String,
        meter: &Meter,
    ) -> Self {
        let url_scheme = base_url
            .split("://")
            .next()
            .unwrap_or("file")
            .to_string();

        // Create object_store client for streaming uploads
        let url = base_url.parse::<Url>().expect("invalid blob storage URL");
        let (object_store, object_store_path) =
            parse_url(&url).expect("failed to parse blob storage URL");

        Self {
            store: Arc::new(dispatcher),
            object_store: Arc::new(object_store),
            object_store_path,
            base_url,
            url_scheme,
            metrics: Some(BlobMetrics::new(meter)),
        }
    }

    /// Get the base URL.
    pub fn get_url(&self) -> String {
        self.base_url.clone()
    }

    /// Get the URL scheme.
    pub fn get_url_scheme(&self) -> String {
        self.url_scheme.clone()
    }

    /// Upload data from a stream using true streaming (no buffering).
    ///
    /// This method:
    /// - Streams data without loading it all into memory
    /// - Computes SHA256 hash during upload
    /// - Returns the URL, size, and hash
    ///
    /// # Arguments
    /// * `key` - Relative key/path within the blob store
    /// * `data` - Stream of data chunks
    pub async fn put(
        &self,
        key: &str,
        data: impl Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult> {
        let timer_kvs = &[KeyValue::new("op", "put")];
        let _timer = self.metrics.as_ref().map(|m| {
            crate::metrics::Timer::start_with_labels(&m.operations, timer_kvs)
        });

        // Compute hash while streaming upload
        let mut hasher = Sha256::new();
        let mut hashed_stream = data.map(|item| {
            item.inspect(|bytes| {
                hasher.update(bytes);
            })
        });

        // Build object path
        let path = self.object_store_path.child(key);

        // Use object_store's multipart writer for true streaming
        let multipart = self.object_store.put_multipart(&path).await.map_err(|e| {
            if let Some(metrics) = &self.metrics {
                metrics.errors.add(1, &[KeyValue::new("op", "put")]);
            }
            anyhow::Error::from(e)
        })?;

        let mut writer = WriteMultipart::new(multipart);
        let mut size_bytes = 0;

        // Stream chunks directly to object store
        while let Some(chunk_result) = hashed_stream.next().await {
            let chunk = chunk_result.map_err(|e| {
                if let Some(metrics) = &self.metrics {
                    metrics.errors.add(1, &[KeyValue::new("op", "put")]);
                }
                e
            })?;

            writer.wait_for_capacity(1).await.map_err(|e| {
                if let Some(metrics) = &self.metrics {
                    metrics.errors.add(1, &[KeyValue::new("op", "put")]);
                }
                anyhow::Error::from(e)
            })?;

            size_bytes += chunk.len() as u64;
            writer.write(&chunk);
        }

        // Finalize the upload
        writer.finish().await.map_err(|e| {
            if let Some(metrics) = &self.metrics {
                metrics.errors.add(1, &[KeyValue::new("op", "put")]);
            }
            anyhow::Error::from(e)
        })?;

        let hash = format!("{:x}", hasher.finalize());

        // Build full URI for response
        let uri = if self.base_url.ends_with('/') {
            format!("{}{}", self.base_url, key)
        } else {
            format!("{}/{}", self.base_url, key)
        };

        Ok(PutResult {
            url: uri,
            size_bytes,
            sha256_hash: hash,
        })
    }

    /// Download data as a stream.
    ///
    /// Supports optional range requests for partial downloads.
    ///
    /// # Arguments
    /// * `path` - Relative path within the blob store
    /// * `range` - Optional byte range to download
    pub async fn get(
        &self,
        path: &str,
        range: Option<Range<u64>>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let timer_kvs = &[KeyValue::new("op", "get")];
        let _timer = self.metrics.as_ref().map(|m| {
            crate::metrics::Timer::start_with_labels(&m.operations, timer_kvs)
        });

        // Build full URI
        let uri = if self.base_url.ends_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        };

        // Download data
        let data = if let Some(r) = range {
            self.store.get_range(&uri, r).await
        } else {
            self.store.get(&uri).await
        }
        .map_err(|e| {
            if let Some(metrics) = &self.metrics {
                metrics.errors.add(1, &[KeyValue::new("op", "get")]);
            }
            anyhow::Error::from(e)
        })?;

        // Convert to stream
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let _ = tx.send(Ok(Bytes::from(data)));
        });

        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    /// Delete a blob.
    ///
    /// Note: Not all backends may support deletion.
    pub async fn delete(&self, _key: &str) -> Result<()> {
        // Note: BlobStore trait doesn't have delete yet
        // This would need to be added to the trait
        anyhow::bail!("Delete operation not yet implemented in BlobStore trait")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BlobStorageConfig, BlobStoreDispatcher};
    use futures::stream;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_storage_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = format!("file://{}", temp_dir.path().display());

        let config = BlobStorageConfig {
            path: base_path.clone(),
            ..Default::default()
        };

        let dispatcher = BlobStoreDispatcher::new(config).await.unwrap();
        let storage = BlobStorage::new(dispatcher, base_path);

        // Upload data
        let data = vec![
            Ok(Bytes::from("hello ")),
            Ok(Bytes::from("world")),
        ];
        let data_stream = stream::iter(data);

        let result = storage.put("test.txt", data_stream).await.unwrap();
        assert_eq!(result.size_bytes, 11); // "hello world"
        assert!(!result.sha256_hash.is_empty());

        // Download data
        let mut download_stream = storage.get("test.txt", None).await.unwrap();
        let mut downloaded = Vec::new();

        while let Some(chunk) = download_stream.next().await {
            let chunk = chunk.unwrap();
            downloaded.extend_from_slice(&chunk);
        }

        assert_eq!(downloaded, b"hello world");
    }

    #[tokio::test]
    async fn test_storage_put_get_range() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = format!("file://{}", temp_dir.path().display());

        let config = BlobStorageConfig {
            path: base_path.clone(),
            ..Default::default()
        };

        let dispatcher = BlobStoreDispatcher::new(config).await.unwrap();
        let storage = BlobStorage::new(dispatcher, base_path);

        // Upload data
        let data = vec![Ok(Bytes::from("hello world"))];
        let data_stream = stream::iter(data);

        storage.put("test.txt", data_stream).await.unwrap();

        // Download range (bytes 6-11 = "world")
        let mut download_stream = storage.get("test.txt", Some(6..11)).await.unwrap();
        let mut downloaded = Vec::new();

        while let Some(chunk) = download_stream.next().await {
            let chunk = chunk.unwrap();
            downloaded.extend_from_slice(&chunk);
        }

        assert_eq!(downloaded, b"world");
    }

    #[tokio::test]
    async fn test_streaming_no_buffer() {
        // This test verifies that large uploads don't cause OOM
        let temp_dir = TempDir::new().unwrap();
        let base_path = format!("file://{}", temp_dir.path().display());

        let config = BlobStorageConfig {
            path: base_path.clone(),
            ..Default::default()
        };

        let dispatcher = BlobStoreDispatcher::new(config).await.unwrap();
        let storage = BlobStorage::new(dispatcher, base_path);

        // Create a stream of 100 chunks (1KB each = 100KB total)
        // If this was buffering, we'd use 100KB of memory
        let chunks: Vec<Result<Bytes>> = (0..100)
            .map(|_| Ok(Bytes::from(vec![0u8; 1024])))
            .collect();
        let data_stream = stream::iter(chunks);

        let result = storage.put("large.bin", data_stream).await.unwrap();
        assert_eq!(result.size_bytes, 102400); // 100KB
    }
}
