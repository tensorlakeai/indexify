//! Application code cache.
//!
//! Downloads and caches application code blobs from the blob store,
//! using atomic file operations for thread-safe shared access.
//! Downloads are streamed to disk to avoid buffering large blobs in memory.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

use crate::blob_ops::BlobStore;

/// Cache for application code downloaded from the blob store.
pub struct CodeCache {
    cache_dir: PathBuf,
    blob_store: Arc<BlobStore>,
    metrics: Arc<crate::metrics::DataplaneMetrics>,
}

impl CodeCache {
    /// Create a new code cache.
    ///
    /// `cache_dir` is the root directory for the cache (e.g.,
    /// `/tmp/indexify_cache`).
    pub fn new(
        cache_dir: PathBuf,
        blob_store: Arc<BlobStore>,
        metrics: Arc<crate::metrics::DataplaneMetrics>,
    ) -> Self {
        Self {
            cache_dir,
            blob_store,
            metrics,
        }
    }

    /// Get application code, using cache if available.
    ///
    /// Returns the path to the cached file on disk. On cache miss, streams
    /// the download from the blob store directly to a temp file (verifying
    /// SHA-256 along the way), then atomically renames it into the cache.
    pub async fn get_or_download(
        &self,
        namespace: &str,
        app_name: &str,
        app_version: &str,
        code_uri: &str,
        expected_sha256: Option<&str>,
    ) -> Result<Bytes> {
        let cache_path = self
            .cache_dir
            .join("application_cache")
            .join(namespace)
            .join(app_name)
            .join(app_version);

        // Check local cache
        if let Some(data) = try_read_cache(&cache_path).await {
            debug!(
                namespace = %namespace,
                app_name = %app_name,
                app_version = %app_version,
                "Application code loaded from cache"
            );
            self.metrics
                .counters
                .application_downloads_from_cache
                .add(1, &[]);
            return Ok(data);
        }

        // Cache miss — stream download to temp file, verify, and cache
        info!(
            namespace = %namespace,
            app_name = %app_name,
            app_version = %app_version,
            code_uri = %code_uri,
            "Downloading application code"
        );
        self.stream_download_to_cache(code_uri, expected_sha256, &cache_path)
            .await?;

        // Read the cached file into memory for the caller
        let data = tokio::fs::read(&cache_path)
            .await
            .with_context(|| format!("Failed to read cached file: {}", cache_path.display()))?;
        Ok(Bytes::from(data))
    }

    /// Stream download from blob store to a temp file, verify SHA-256, and
    /// atomically move into the cache.
    async fn stream_download_to_cache(
        &self,
        code_uri: &str,
        expected_sha256: Option<&str>,
        cache_path: &Path,
    ) -> Result<()> {
        let counters = &self.metrics.counters;
        let histograms = &self.metrics.histograms;

        counters.application_downloads.add(1, &[]);
        let download_start = std::time::Instant::now();

        // Ensure temp directory exists (same filesystem as cache for atomic rename)
        let temp_dir = self.cache_dir.join("tmp_download");
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .with_context(|| format!("Failed to create temp dir: {}", temp_dir.display()))?;

        let temp_path = temp_dir.join(format!("dl-{}", uuid::Uuid::new_v4()));

        // Stream from blob store → temp file, computing SHA-256 as we go
        let result = self
            .stream_to_file(code_uri, &temp_path, expected_sha256)
            .await;

        histograms
            .application_download_latency_seconds
            .record(download_start.elapsed().as_secs_f64(), &[]);

        if let Err(e) = &result {
            counters.application_download_errors.add(1, &[]);
            // Clean up temp file on failure
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(anyhow::anyhow!("{e:#}")).context("Failed to download application code");
        }

        // Ensure cache parent directory exists
        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("Failed to create cache directory: {}", parent.display())
            })?;
        }

        // Atomic rename into cache
        tokio::fs::rename(&temp_path, cache_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename {} to {}",
                    temp_path.display(),
                    cache_path.display()
                )
            })?;

        debug!(path = %cache_path.display(), "Cached application code");
        Ok(())
    }

    /// Stream blob content to a file, optionally verifying SHA-256.
    async fn stream_to_file(
        &self,
        code_uri: &str,
        dest: &Path,
        expected_sha256: Option<&str>,
    ) -> Result<()> {
        let mut stream = self.blob_store.get_stream(code_uri).await?;
        let mut file = tokio::fs::File::create(dest)
            .await
            .with_context(|| format!("Failed to create temp file: {}", dest.display()))?;

        let mut hasher = expected_sha256.map(|_| Sha256::new());

        while let Some(chunk_result) = stream.next().await {
            let chunk: Bytes = chunk_result?;
            if let Some(h) = &mut hasher {
                h.update(&chunk);
            }
            file.write_all(&chunk)
                .await
                .with_context(|| format!("Failed to write to temp file: {}", dest.display()))?;
        }

        file.flush().await?;

        // Verify SHA-256 if expected
        if let (Some(hasher), Some(expected)) = (hasher, expected_sha256) {
            let actual = format!("{:x}", hasher.finalize());
            if actual != expected {
                anyhow::bail!(
                    "SHA256 mismatch for application code: expected {}, got {}",
                    expected,
                    actual
                );
            }
        }

        Ok(())
    }
}

/// Try to read cached application code from the given path.
async fn try_read_cache(cache_path: &Path) -> Option<Bytes> {
    if !cache_path.exists() {
        return None;
    }
    match tokio::fs::read(cache_path).await {
        Ok(data) => Some(Bytes::from(data)),
        Err(e) => {
            warn!(
                error = ?e,
                cache_path = %cache_path.display(),
                "Cache read failed, will download"
            );
            None
        }
    }
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

        let metrics = Arc::new(crate::metrics::DataplaneMetrics::new());
        let blob_store = Arc::new(BlobStore::new(metrics.clone()));
        let cache = CodeCache::new(cache_dir.clone(), blob_store, metrics);

        let code_uri = format!("file://{}", code_path.display());

        // First call: cache miss, streams download to cache
        let result = cache
            .get_or_download("test-ns", "my-app", "v1", &code_uri, None)
            .await
            .unwrap();
        assert_eq!(result.as_ref(), code_data);

        // Verify cache file exists (written synchronously now, no sleep needed)
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

        let metrics = Arc::new(crate::metrics::DataplaneMetrics::new());
        let blob_store = Arc::new(BlobStore::new(metrics.clone()));
        let cache = CodeCache::new(dir.path().join("cache"), blob_store, metrics);
        let code_uri = format!("file://{}", code_path.display());

        // Correct hash
        let mut hasher = Sha256::new();
        hasher.update(code_data);
        let correct_hash = format!("{:x}", hasher.finalize());

        let result = cache
            .get_or_download("ns", "app", "v1", &code_uri, Some(&correct_hash))
            .await;
        assert!(result.is_ok());

        // Wrong hash — should fail and NOT cache the file
        let result = cache
            .get_or_download("ns", "app", "v2", &code_uri, Some("wrong_hash"))
            .await;
        assert!(result.is_err());

        // Verify the bad file was cleaned up
        let bad_cache_path = dir.path().join("cache/application_cache/ns/app/v2");
        assert!(!bad_cache_path.exists());
    }
}
