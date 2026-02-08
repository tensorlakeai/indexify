//! Application code cache.
//!
//! Downloads and caches application code blobs from the blob store,
//! using atomic file operations for thread-safe shared access.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use crate::blob_ops::BlobStore;

/// Cache for application code downloaded from the blob store.
pub struct CodeCache {
    cache_dir: PathBuf,
    blob_store: Arc<BlobStore>,
}

impl CodeCache {
    /// Create a new code cache.
    ///
    /// `cache_dir` is the root directory for the cache (e.g.,
    /// `/tmp/indexify_cache`).
    pub fn new(cache_dir: PathBuf, blob_store: Arc<BlobStore>) -> Self {
        Self {
            cache_dir,
            blob_store,
        }
    }

    /// Get application code, using cache if available.
    ///
    /// Returns the raw bytes of the serialized application code.
    /// Downloads from the blob store on cache miss, and writes to cache
    /// asynchronously.
    pub async fn get_or_download(
        &self,
        namespace: &str,
        app_name: &str,
        app_version: &str,
        code_uri: &str,
        expected_sha256: Option<&str>,
    ) -> Result<Bytes> {
        let counters = crate::metrics::DataplaneCounters::new();
        let histograms = crate::metrics::DataplaneHistograms::new();

        let cache_path = self
            .cache_dir
            .join("application_cache")
            .join(namespace)
            .join(app_name)
            .join(app_version);

        // Check local cache
        if cache_path.exists() {
            match tokio::fs::read(&cache_path).await {
                Ok(data) => {
                    debug!(
                        namespace = %namespace,
                        app_name = %app_name,
                        app_version = %app_version,
                        "Application code loaded from cache"
                    );
                    counters.application_downloads_from_cache.add(1, &[]);
                    return Ok(Bytes::from(data));
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        cache_path = %cache_path.display(),
                        "Cache read failed, downloading"
                    );
                }
            }
        }

        // Cache miss — download from blob store
        counters.application_downloads.add(1, &[]);
        let download_start = std::time::Instant::now();
        info!(
            namespace = %namespace,
            app_name = %app_name,
            app_version = %app_version,
            code_uri = %code_uri,
            "Downloading application code"
        );

        let data = match self.blob_store.get(code_uri).await {
            Ok(data) => {
                histograms
                    .application_download_latency_seconds
                    .record(download_start.elapsed().as_secs_f64(), &[]);
                data
            }
            Err(e) => {
                histograms
                    .application_download_latency_seconds
                    .record(download_start.elapsed().as_secs_f64(), &[]);
                counters.application_download_errors.add(1, &[]);
                return Err(e).context("Failed to download application code");
            }
        };

        // Verify SHA256 if expected hash provided
        if let Some(expected) = expected_sha256 {
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let actual = format!("{:x}", hasher.finalize());
            if actual != expected {
                anyhow::bail!(
                    "SHA256 mismatch for application code: expected {}, got {}",
                    expected,
                    actual
                );
            }
        }

        // Write to cache asynchronously (atomic via temp file + rename)
        let data_clone = data.clone();
        let cache_path_clone = cache_path.clone();
        let cache_dir = self.cache_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = write_cache_atomically(&cache_dir, &cache_path_clone, &data_clone).await
            {
                warn!(
                    error = %e,
                    path = %cache_path_clone.display(),
                    "Failed to write application code to cache"
                );
            }
        });

        Ok(data)
    }
}

/// Write data to cache path atomically using a temp file and rename.
async fn write_cache_atomically(cache_dir: &Path, target: &Path, data: &[u8]) -> Result<()> {
    // Ensure parent directories exist
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create cache directory: {}", parent.display()))?;
    }

    // Write to temp file in the cache root (same filesystem for atomic rename)
    let temp_dir = cache_dir.join("task_application_cache");
    tokio::fs::create_dir_all(&temp_dir)
        .await
        .with_context(|| format!("Failed to create temp dir: {}", temp_dir.display()))?;

    let temp_file = temp_dir.join(format!("tmp-{}", uuid::Uuid::new_v4()));
    tokio::fs::write(&temp_file, data)
        .await
        .with_context(|| format!("Failed to write temp file: {}", temp_file.display()))?;

    // Atomic rename to target path
    tokio::fs::rename(&temp_file, target)
        .await
        .with_context(|| {
            format!(
                "Failed to rename {} to {}",
                temp_file.display(),
                target.display()
            )
        })?;

    debug!(path = %target.display(), "Cached application code");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");
        let cache_dir = dir.path().join("cache");

        // Write "application code" to a file that BlobStore can read
        tokio::fs::create_dir_all(&data_dir).await.unwrap();
        let code_path = data_dir.join("app_code.bin");
        let code_data = b"print('hello')";
        tokio::fs::write(&code_path, code_data).await.unwrap();

        let blob_store = Arc::new(BlobStore::new_local());
        let cache = CodeCache::new(cache_dir.clone(), blob_store);

        let code_uri = format!("file://{}", code_path.display());

        // First call: cache miss, downloads
        let result = cache
            .get_or_download("test-ns", "my-app", "v1", &code_uri, None)
            .await
            .unwrap();
        assert_eq!(result.as_ref(), code_data);

        // Wait for async cache write
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Verify cache file exists
        let cached_path = cache_dir
            .join("application_cache")
            .join("test-ns")
            .join("my-app")
            .join("v1");
        assert!(cached_path.exists());

        // Second call: cache hit
        let result2 = cache
            .get_or_download("test-ns", "my-app", "v1", &code_uri, None)
            .await
            .unwrap();
        assert_eq!(result2.as_ref(), code_data);
    }

    #[tokio::test]
    async fn test_sha256_verification() {
        let dir = tempfile::tempdir().unwrap();
        let code_path = dir.path().join("code.bin");
        let code_data = b"test data";
        tokio::fs::write(&code_path, code_data).await.unwrap();

        let blob_store = Arc::new(BlobStore::new_local());
        let cache = CodeCache::new(dir.path().join("cache"), blob_store);
        let code_uri = format!("file://{}", code_path.display());

        // Correct hash
        let mut hasher = Sha256::new();
        hasher.update(code_data);
        let correct_hash = format!("{:x}", hasher.finalize());

        let result = cache
            .get_or_download("ns", "app", "v1", &code_uri, Some(&correct_hash))
            .await;
        assert!(result.is_ok());

        // Wrong hash
        let result = cache
            .get_or_download("ns", "app", "v2", &code_uri, Some("wrong_hash"))
            .await;
        assert!(result.is_err());
    }
}
