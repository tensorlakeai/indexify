//! Azure Blob Storage backend using object_store + azure_storage_blobs for
//! presigning.

use std::{ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use object_store::{
    GetOptions,
    GetRange,
    ObjectStore,
    ObjectStoreExt,
    azure::{MicrosoftAzure, MicrosoftAzureBuilder},
    path::Path as ObjectPath,
};
use tracing::{debug, error};

use crate::{BlobError, BlobMetadata, BlobResult, BlobStore, presign};

/// Azure Blob Storage backend.
pub struct AzureBlobStore {
    /// object_store client for data I/O.
    object_store: Arc<MicrosoftAzure>,

    /// Azure storage client for presigning.
    blob_service_client: BlobServiceClient,

    /// Container name extracted from base path.
    container: String,

    /// Optional prefix for all keys.
    prefix: String,
}

impl AzureBlobStore {
    /// Create a new Azure blob store from a base URL.
    ///
    /// # Arguments
    /// * `url` - Azure URL (e.g., `azure://container/prefix` or
    ///   `az://container/prefix`)
    /// * `storage_account` - Optional storage account name
    pub async fn new(url: &str, storage_account: Option<String>) -> BlobResult<Self> {
        if !url.starts_with("azure://") && !url.starts_with("az://") {
            return Err(BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "URI must start with azure:// or az://".to_string(),
            });
        }

        // Parse container and prefix from URL
        let (container, prefix) = Self::parse_azure_url(url)?;

        // Build object_store client
        let mut builder = MicrosoftAzureBuilder::from_env().with_url(url);
        if let Some(ref account) = storage_account {
            builder = builder.with_account(account);
        }
        let object_store = builder.build().map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        // Build Azure SDK client for presigning (SAS tokens)
        // Get credentials from environment
        let account_name = storage_account
            .or_else(|| std::env::var("AZURE_STORAGE_ACCOUNT").ok())
            .ok_or_else(|| BlobError::NetworkError {
                source: anyhow::Error::msg("AZURE_STORAGE_ACCOUNT not set"),
            })?;

        let credentials = if let Ok(key) = std::env::var("AZURE_STORAGE_KEY") {
            StorageCredentials::access_key(account_name.clone(), key)
        } else if let Ok(token) = std::env::var("AZURE_STORAGE_TOKEN") {
            StorageCredentials::bearer_token(token)
        } else {
            return Err(BlobError::NetworkError {
                source: anyhow::Error::msg(
                    "Neither AZURE_STORAGE_KEY nor AZURE_STORAGE_TOKEN is set",
                ),
            });
        };

        let blob_service_client = BlobServiceClient::new(&account_name, credentials);

        debug!(
            container = %container,
            prefix = %prefix,
            "Created Azure blob store"
        );

        Ok(Self {
            object_store: Arc::new(object_store),
            blob_service_client,
            container,
            prefix,
        })
    }

    /// Parse Azure URL into container and prefix.
    fn parse_azure_url(url: &str) -> BlobResult<(String, String)> {
        let without_scheme = url
            .strip_prefix("azure://")
            .or_else(|| url.strip_prefix("az://"))
            .ok_or_else(|| BlobError::InvalidUri {
                uri: url.to_string(),
                reason: "Must start with azure:// or az://".to_string(),
            })?;

        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
        let container = parts[0].to_string();
        let prefix = if parts.len() > 1 {
            parts[1].trim_end_matches('/').to_string()
        } else {
            String::new()
        };

        Ok((container, prefix))
    }

    /// Extract container and blob name from full Azure URI.
    fn parse_uri(&self, uri: &str) -> BlobResult<(String, String)> {
        Self::parse_azure_url(uri)
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
impl BlobStore for AzureBlobStore {
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>> {
        let (_container, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        let result = self.object_store.get(&path).await?;
        let bytes = result.bytes().await.map_err(|e| BlobError::NetworkError {
            source: anyhow::Error::from(e),
        })?;

        Ok(bytes.to_vec())
    }

    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>> {
        let (_container, key) = self.parse_uri(uri)?;
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
        let (_container, key) = self.parse_uri(uri)?;
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
        let (_container, key) = self.parse_uri(uri)?;
        let path = self.key_to_path(&key);

        self.object_store
            .put(&path, Bytes::from(data).into())
            .await?;

        Ok(())
    }

    async fn presign_get_uri(&self, uri: &str, expires_in: Duration) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (container, key) = self.parse_uri(uri)?;

        // Generate SAS token for GET access
        let blob_client = self
            .blob_service_client
            .container_client(&container)
            .blob_client(&key);

        // Create SAS token with read permissions
        let sas_expiry = chrono::Utc::now() +
            chrono::Duration::from_std(expires_in).map_err(|e| BlobError::PresignError {
                reason: format!("Invalid expiry duration: {}", e),
            })?;

        // Note: Azure SDK's SAS generation API is complex
        // For now, we'll return the blob URL with a note about manual SAS generation
        // In production, you'd generate a proper SAS token here
        let url = blob_client.url().map_err(|e| BlobError::PresignError {
            reason: format!("Failed to generate blob URL: {}", e),
        })?;

        // TODO: Add actual SAS token generation
        // This requires azure_storage_blobs SAS builder which is complex
        Ok(url.to_string())
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        _upload_id: &str,
        expires_in: Duration,
    ) -> BlobResult<String> {
        // Validate expiry
        presign::validate_expiry(expires_in).map_err(|e| BlobError::PresignError { reason: e })?;

        let (container, key) = self.parse_uri(uri)?;

        // Azure uses block blobs with block IDs instead of S3-style parts
        let key_with_part = format!("{}.part.{}", key, part_number);

        let blob_client = self
            .blob_service_client
            .container_client(&container)
            .blob_client(&key_with_part);

        let url = blob_client.url().map_err(|e| BlobError::PresignError {
            reason: format!("Failed to generate blob URL: {}", e),
        })?;

        // TODO: Add actual SAS token with write permissions
        Ok(url.to_string())
    }

    async fn create_multipart_upload(&self, uri: &str) -> BlobResult<String> {
        // Azure uses block blobs, not multipart uploads like S3
        // Return a tracking ID
        let (_container, key) = self.parse_uri(uri)?;
        Ok(format!("azure-upload-{}", key))
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        _upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()> {
        let (container, key) = self.parse_uri(uri)?;

        // Download all parts and concatenate them
        let mut all_data = Vec::new();

        for (i, _etag) in parts_etags.iter().enumerate() {
            let part_key = format!("{}.part.{}", key, i + 1);
            let part_uri = format!("azure://{}/{}", container, part_key);

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
        let (container, key) = self.parse_uri(uri)?;

        // Try to delete all part files
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
    fn test_parse_azure_url() {
        let (container, prefix) =
            AzureBlobStore::parse_azure_url("azure://my-container/path/to/prefix").unwrap();
        assert_eq!(container, "my-container");
        assert_eq!(prefix, "path/to/prefix");

        let (container, prefix) = AzureBlobStore::parse_azure_url("az://my-container").unwrap();
        assert_eq!(container, "my-container");
        assert_eq!(prefix, "");
    }
}
