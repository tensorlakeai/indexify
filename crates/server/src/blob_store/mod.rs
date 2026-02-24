use std::{env, ops::Range};

use anyhow::Result;
use bytes::Bytes;
use futures::stream::BoxStream;
use indexify_blob_store::uri;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};

use crate::metrics::{Timer, blob_storage};

pub mod registry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    #[serde(default = "default_blob_store_path")]
    pub path: String,
    #[serde(default)]
    pub region: Option<String>,
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        BlobStorageConfig {
            path: default_blob_store_path(),
            region: None,
        }
    }
}

fn default_blob_store_path() -> String {
    format!(
        "file://{}",
        env::current_dir()
            .expect("unable to get current directory")
            .join("indexify_storage/blobs")
            .to_str()
            .expect("unable to get path as string")
    )
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub url: String,
    pub size_bytes: u64,
    pub sha256_hash: String,
}

/// Blob storage backed by [`indexify_blob_store::BlobStore`].
///
/// This wrapper translates between the server's stored-path convention
/// (relative paths as kept in the database) and the full URIs that the
/// shared blob store crate expects.
pub struct BlobStorage {
    inner: indexify_blob_store::BlobStore,
    metrics: blob_storage::Metrics,
}

impl BlobStorage {
    pub async fn new(config: BlobStorageConfig) -> Result<Self> {
        let url = &config.path;
        tracing::debug!("using blob store path: {}", url);
        let inner = if indexify_blob_store::uri::is_s3_uri(url) {
            indexify_blob_store::BlobStore::new_s3(url, config.region).await?
        } else {
            indexify_blob_store::BlobStore::new_local(url)
        };
        Ok(Self {
            inner,
            metrics: blob_storage::Metrics::new(),
        })
    }

    pub fn get_url(&self) -> String {
        self.inner.base_url().to_string()
    }

    pub fn get_url_scheme(&self) -> String {
        self.inner.url_scheme().to_string()
    }

    /// Convert a stored path to a full URI using the blob store's scheme and
    /// base URL.
    fn stored_path_to_uri(&self, path: &str) -> String {
        uri::blob_store_path_to_url(path, self.inner.url_scheme(), self.inner.base_url())
    }

    pub async fn put(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        let timer_kvs = &[KeyValue::new("op", "put")];
        let _timer = Timer::start_with_labels(&self.metrics.operations, timer_kvs);

        // Construct the full URI from the base URL and key.
        let full_uri = format!(
            "{}/{}",
            self.inner.base_url().trim_end_matches('/'),
            key.trim_start_matches('/')
        );

        let result = self
            .inner
            .put(
                &full_uri,
                data,
                indexify_blob_store::PutOptions {
                    compute_sha256: true,
                },
            )
            .await?;

        // Convert the full URI back to a stored path.
        let stored_path = uri::blob_store_url_to_path(
            &result.uri,
            self.inner.url_scheme(),
            self.inner.base_url(),
        );

        Ok(PutResult {
            url: stored_path,
            size_bytes: result.size_bytes,
            sha256_hash: result.sha256_hash.unwrap_or_default(),
        })
    }

    /// Get an object from blob storage as a stream.
    ///
    /// `path` is a stored path (as returned by `put`). If `range` is provided,
    /// returns only the specified byte range.
    pub async fn get(
        &self,
        path: &str,
        range: Option<Range<u64>>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let timer_kvs = &[KeyValue::new("op", "get")];
        let _timer = Timer::start_with_labels(&self.metrics.operations, timer_kvs);
        let full_uri = self.stored_path_to_uri(path);
        self.inner.get_stream(&full_uri, range).await
    }

    /// Delete a blob by its stored path.
    #[allow(dead_code)]
    pub async fn delete(&self, key: &str) -> Result<()> {
        let full_uri = self.stored_path_to_uri(key);
        self.inner.delete(&full_uri).await
    }

    /// Delete a blob by its full URI (e.g., for snapshot URIs that are already
    /// full URIs from the dataplane).
    pub async fn delete_by_uri(&self, uri: &str) -> Result<()> {
        self.inner.delete(uri).await
    }

    #[cfg(test)]
    pub async fn _read_bytes(&self, key: &str) -> Result<Bytes> {
        use bytes::BytesMut;
        use futures::StreamExt;

        let mut reader = self.get(key, None).await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = reader.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.into())
    }
}
