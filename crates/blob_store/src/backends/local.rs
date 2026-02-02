//! Local filesystem blob store backend.

use std::{ops::Range, path::PathBuf, time::Duration};

use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::{BlobError, BlobMetadata, BlobResult, BlobStore};

/// Local filesystem blob store.
pub struct LocalBlobStore;

impl LocalBlobStore {
    /// Create a new local filesystem blob store.
    pub fn new() -> Self {
        Self
    }

    /// Extract filesystem path from file:// URI.
    fn path_from_uri(uri: &str) -> BlobResult<PathBuf> {
        if !uri.starts_with("file://") {
            return Err(BlobError::InvalidUri {
                uri: uri.to_string(),
                reason: "URI must start with file://".to_string(),
            });
        }
        let path_str = uri.strip_prefix("file://").unwrap();
        Ok(PathBuf::from(path_str))
    }
}

impl Default for LocalBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BlobStore for LocalBlobStore {
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>> {
        let path = Self::path_from_uri(uri)?;
        tokio::fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BlobError::NotFound {
                    uri: uri.to_string(),
                }
            } else {
                BlobError::IoError { source: e }
            }
        })
    }

    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>> {
        let path = Self::path_from_uri(uri)?;
        let mut file = tokio::fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BlobError::NotFound {
                    uri: uri.to_string(),
                }
            } else {
                BlobError::IoError { source: e }
            }
        })?;

        // Seek to start position
        file.seek(std::io::SeekFrom::Start(range.start)).await?;

        // Read the specified range
        let size = (range.end - range.start) as usize;
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    async fn get_metadata(&self, uri: &str) -> BlobResult<BlobMetadata> {
        let path = Self::path_from_uri(uri)?;
        let metadata = tokio::fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BlobError::NotFound {
                    uri: uri.to_string(),
                }
            } else {
                BlobError::IoError { source: e }
            }
        })?;

        Ok(BlobMetadata {
            size_bytes: metadata.len(),
            sha256_hash: None,
            etag: None,
            content_type: None,
        })
    }

    async fn upload(&self, uri: &str, data: Vec<u8>) -> BlobResult<()> {
        let path = Self::path_from_uri(uri)?;

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        tokio::fs::write(&path, data).await?;
        Ok(())
    }

    async fn presign_get_uri(&self, uri: &str, _expires_in: Duration) -> BlobResult<String> {
        // For local files, the URI itself is sufficient (shared filesystem assumption)
        // Validate the URI format
        Self::path_from_uri(uri)?;
        Ok(uri.to_string())
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        _upload_id: &str,
        _expires_in: Duration,
    ) -> BlobResult<String> {
        // Return a specific path for this part
        Self::path_from_uri(uri)?;
        Ok(format!("{}.part.{}", uri, part_number))
    }

    async fn create_multipart_upload(&self, _uri: &str) -> BlobResult<String> {
        // For local files, we don't need a real multipart upload session
        // Just return a dummy upload ID
        Ok("local-dummy-upload-id".to_string())
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        _upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()> {
        // Concatenate all parts into the final file
        let path = Self::path_from_uri(uri)?;

        // Create parent directories
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut final_file = tokio::fs::File::create(&path).await?;

        // Concatenate parts in order
        for part_num in 1..=parts_etags.len() {
            let part_uri = format!("{}.part.{}", uri, part_num);
            let part_path = Self::path_from_uri(&part_uri)?;

            if part_path.exists() {
                let part_data = tokio::fs::read(&part_path).await?;
                final_file.write_all(&part_data).await?;

                // Clean up part file
                let _ = tokio::fs::remove_file(&part_path).await;
            }
        }

        final_file.flush().await?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, uri: &str, _upload_id: &str) -> BlobResult<()> {
        // Try to clean up any .part.* files
        let path = Self::path_from_uri(uri)?;
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));

        if let Ok(mut entries) = tokio::fs::read_dir(parent).await {
            let uri_string = uri.to_string();
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with(&uri_string) && name.contains(".part.") {
                        let _ = tokio::fs::remove_file(entry.path()).await;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_local_upload_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let uri = format!("file://{}", file_path.display());

        let store = LocalBlobStore::new();
        let data = b"hello world".to_vec();

        // Upload
        store.upload(&uri, data.clone()).await.unwrap();

        // Get
        let retrieved = store.get(&uri).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_local_get_range() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let uri = format!("file://{}", file_path.display());

        let store = LocalBlobStore::new();
        let data = b"hello world".to_vec();

        store.upload(&uri, data).await.unwrap();

        // Get range "world" (bytes 6-11)
        let range_data = store.get_range(&uri, 6..11).await.unwrap();
        assert_eq!(range_data, b"world");
    }

    #[tokio::test]
    async fn test_local_get_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let uri = format!("file://{}", file_path.display());

        let store = LocalBlobStore::new();
        let data = b"hello world".to_vec();

        store.upload(&uri, data.clone()).await.unwrap();

        let metadata = store.get_metadata(&uri).await.unwrap();
        assert_eq!(metadata.size_bytes, data.len() as u64);
    }

    #[tokio::test]
    async fn test_local_not_found() {
        let uri = "file:///nonexistent/file.txt";
        let store = LocalBlobStore::new();

        let result = store.get(uri).await;
        assert!(matches!(result, Err(BlobError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_local_multipart() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("multipart.txt");
        let uri = format!("file://{}", file_path.display());

        let store = LocalBlobStore::new();

        // Create multipart upload
        let upload_id = store.create_multipart_upload(&uri).await.unwrap();

        // Upload parts
        let part1_uri = store
            .presign_upload_part_uri(&uri, 1, &upload_id, Duration::from_secs(3600))
            .await
            .unwrap();
        let part2_uri = store
            .presign_upload_part_uri(&uri, 2, &upload_id, Duration::from_secs(3600))
            .await
            .unwrap();

        store.upload(&part1_uri, b"hello ".to_vec()).await.unwrap();
        store.upload(&part2_uri, b"world".to_vec()).await.unwrap();

        // Complete multipart
        store
            .complete_multipart_upload(
                &uri,
                &upload_id,
                vec!["etag1".to_string(), "etag2".to_string()],
            )
            .await
            .unwrap();

        // Verify concatenated result
        let data = store.get(&uri).await.unwrap();
        assert_eq!(data, b"hello world");
    }
}
