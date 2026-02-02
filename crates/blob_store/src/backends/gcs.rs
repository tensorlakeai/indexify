//! Google Cloud Storage blob store backend using object_store +
//! google-cloud-storage for presigning.

use std::{ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_storage::{
    client::{Client as GcsClient, ClientConfig},
    sign::{SignedURLMethod, SignedURLOptions},
};
use object_store::{
    GetOptions,
    GetRange,
    ObjectStore,
    ObjectStoreExt,
    gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder},
    path::Path as ObjectPath,
};
use tracing::{debug, error};

use crate::{BlobError, BlobMetadata, BlobResult, BlobStore, presign};

/// GCS blob store backend.
pub struct GcsBlobStore {
    /// object_store client for data I/O.
    object_store: Arc<GoogleCloudStorage>,

    /// GCS client for presigning.
    gcs_client: GcsClient,

    /// Bucket name extracted from base path.
    bucket: String,

    /// Optional prefix for all keys.
    prefix: String,
}

impl GcsBlobStore {
    /// Create a new GCS blob store from a base URL.
    ///
    /// # Arguments
    /// * `url` - GCS URL (e.g., `gs://bucket/prefix`)
    /// * `project_id` - Optional GCP project ID
    pub async fn new(url: &str, project_id: Option<String>) -> BlobResult<Self> {
        if !url.starts_with("gs://") {
            return Err(BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "URI must start with gs://".to_string(),
            });
        }

        // Parse bucket and prefix from URL
        let (bucket, prefix) = Self::parse_gcs_url(url)?;

        // Build object_store client
        let mut builder = GoogleCloudStorageBuilder::from_env().with_url(url);
        if let Some(ref p) = project_id {
            builder = builder.with_service_account_key(p);
        }
        let object_store = builder.build().map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        // Build GCS client for presigning
        let config =
            ClientConfig::default()
                .with_auth()
                .await
                .map_err(|e| BlobError::NetworkError {
                    source: anyhow::Error::msg(format!("Failed to create GCS client: {}", e)),
                })?;
        let gcs_client = GcsClient::new(config);

        debug!(
            bucket = %bucket,
            prefix = %prefix,
            "Created GCS blob store"
        );

        Ok(Self {
            object_store: Arc::new(object_store),
            gcs_client,
            bucket,
            prefix,
        })
    }

    /// Parse GCS URL into bucket and prefix.
    fn parse_gcs_url(url: &str) -> BlobResult<(String, String)> {
        let without_scheme = url
            .strip_prefix("gs://")
            .ok_or_else(|| BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "Must start with gs://".to_string(),
            })?;

        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
        let bucket = parts[0].to_string();
        let prefix = if parts.len() > 1 {
            parts[1].trim_end_matches('/').to_string()
        } else {
            String::new()
        };

        Ok((bucket, prefix))
    }

    /// Extract bucket and key from full GCS URI.
    fn parse_uri(&self, uri: &str) -> BlobResult<(String, String)> {
        Self::parse_gcs_url(uri)
    }

    /// Convert a key to an ObjectPath for object_store.
    fn key_to_path(&self, key: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(key)
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix, key))
        }
    }
}

#[async_trait]
impl BlobStore for GcsBlobStore {
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await.map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        Ok(bytes.to_vec())
    }

    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let options = GetOptions {
            range: Some(GetRange::Bounded(range.clone())),
            ..Default::default()
        };

        let result = self.object_store.get_opts(&path, options).await?;
        let bytes = result.bytes().await.map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        Ok(bytes.to_vec())
    }

    async fn get_metadata(&self, uri: &str) -> BlobResult<BlobMetadata> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let metadata = self.object_store.head(&path).await?;

        Ok(BlobMetadata {
            size_bytes: metadata.size as u64,
            sha256_hash: None,
            etag: metadata.e_tag.clone(),
            content_type: None,
        })
    }

    async fn upload(&self, uri: &str, data: Vec<u8>) -> BlobResult<()> {
        let (_bucket, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        self.object_store
            .put(&path, Bytes::from(data).into())
            .await?;

        Ok(())
    }

    async fn presign_get_uri(&self, uri: &str, expires_in: Duration) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (bucket, key) = self.parse_uri(uri)?;

        let opts = SignedURLOptions {
            method: SignedURLMethod::GET,
            expires: expires_in,
            ..Default::default()
        };

        let url = self
            .gcs_client
            .signed_url(&bucket, &key, None, None, opts)
            .await
            .map_err(|e| BlobError::PresignError {
                reason: format!("Failed to generate presigned GET URL: {}", e),
            })?;

        Ok(url)
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        upload_id: &str,
        expires_in: Duration,
    ) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (bucket, key) = self.parse_uri(uri)?;

        // GCS uses resumable uploads, but for simplicity we'll use signed PUT URLs
        // The upload_id is stored in query params for tracking
        let key_with_part = format!("{}.part.{}", key, part_number);

        let opts = SignedURLOptions {
            method: SignedURLMethod::PUT,
            expires: expires_in,
            ..Default::default()
        };

        let url = self
            .gcs_client
            .signed_url(&bucket, &key_with_part, None, None, opts)
            .await
            .map_err(|e| BlobError::PresignError {
                reason: format!("Failed to generate presigned upload part URL: {}", e),
            })?;

        Ok(url)
    }

    async fn create_multipart_upload(&self, uri: &str) -> BlobResult<String> {
        // GCS uses resumable uploads, which are different from S3 multipart
        // For now, return a dummy upload ID and handle parts manually
        let (_bucket, key) = self.parse_uri(uri)?;

        // Return the key as upload_id for tracking
        Ok(format!("gcs-upload-{}", key))
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        _upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()> {
        let (bucket, key) = self.parse_uri(uri)?;

        // Download all parts and concatenate them
        let mut all_data = Vec::new();

        for (i, _etag) in parts_etags.iter().enumerate() {
            let part_key = format!("{}.part.{}", key, i + 1);
            let part_uri = format!("gs://{}/{}", bucket, part_key);

            match self.get(&part_uri).await {
                Ok(part_data) => {
                    all_data.extend_from_slice(&part_data);

                    // Delete the part after reading
                    let part_path = self.key_to_path(&part_key);
                    let _ = self.object_store.delete(&part_path).await;
                }
                Err(e) => {
                    error!("Failed to read part {}: {}", i + 1, e);
                    return Err(BlobError::MultipartError {
                        reason: format!("Failed to read part {}: {}", i + 1, e),
                    });
                }
            }
        }

        // Upload the concatenated data
        self.upload(uri, all_data).await?;

        Ok(())
    }

    async fn abort_multipart_upload(&self, uri: &str, _upload_id: &str) -> BlobResult<()> {
        let (bucket, key) = self.parse_uri(uri)?;

        // Try to delete all part files
        // This is best-effort cleanup
        for part_num in 1..=100 {
            // Arbitrary limit of 100 parts
            let part_key = format!("{}.part.{}", key, part_num);
            let part_path = self.key_to_path(&part_key);

            if self.object_store.delete(&part_path).await.is_err() {
                // Part doesn't exist or error - stop trying
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gcs_url() {
        let (bucket, prefix) =
            GcsBlobStore::parse_gcs_url("gs://my-bucket/path/to/prefix").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "path/to/prefix");

        let (bucket, prefix) = GcsBlobStore::parse_gcs_url("gs://my-bucket").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(prefix, "");
    }
}
