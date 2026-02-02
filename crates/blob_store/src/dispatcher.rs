//! Blob store dispatcher that routes to appropriate backend based on URI
//! scheme.

use std::{collections::HashMap, ops::Range, sync::Arc, time::Duration};

use async_trait::async_trait;

#[cfg(feature = "azure")]
use crate::backends::azure::AzureBlobStore;
#[cfg(feature = "gcp")]
use crate::backends::gcs::GcsBlobStore;
#[cfg(feature = "aws")]
use crate::backends::s3::S3BlobStore;
use crate::{
    BlobError,
    BlobMetadata,
    BlobResult,
    BlobStorageConfig,
    BlobStore,
    backends::local::LocalBlobStore,
};

/// Dispatcher that routes blob operations to the appropriate backend.
pub struct BlobStoreDispatcher {
    backends: HashMap<String, Arc<dyn BlobStore>>,
    default_backend: Option<Arc<dyn BlobStore>>,
}

impl BlobStoreDispatcher {
    /// Create a new dispatcher from configuration.
    pub async fn new(config: BlobStorageConfig) -> BlobResult<Self> {
        let mut backends: HashMap<String, Arc<dyn BlobStore>> = HashMap::new();
        let mut default_backend: Option<Arc<dyn BlobStore>> = None;

        // Parse the main storage path to determine default backend
        let scheme = Self::extract_scheme(&config.path)?;

        match scheme.as_str() {
            "file" => {
                let local = Arc::new(LocalBlobStore::new());
                backends.insert("file".to_string(), local.clone());
                default_backend = Some(local);
            }
            #[cfg(feature = "aws")]
            "s3" => {
                let s3 = Arc::new(S3BlobStore::new(&config.path, config.region).await?);
                backends.insert("s3".to_string(), s3.clone());
                default_backend = Some(s3);
            }
            #[cfg(not(feature = "aws"))]
            "s3" => {
                return Err(BlobError::UnsupportedBackend {
                    scheme: "s3 (feature not enabled)".to_string(),
                });
            }
            #[cfg(feature = "gcp")]
            "gs" => {
                let gcs = Arc::new(GcsBlobStore::new(&config.path, config.gcp_project_id).await?);
                backends.insert("gs".to_string(), gcs.clone());
                default_backend = Some(gcs);
            }
            #[cfg(not(feature = "gcp"))]
            "gs" => {
                return Err(BlobError::UnsupportedBackend {
                    scheme: "gs (feature not enabled)".to_string(),
                });
            }
            #[cfg(feature = "azure")]
            "azure" | "az" => {
                let azure = Arc::new(
                    AzureBlobStore::new(&config.path, config.azure_storage_account).await?,
                );
                backends.insert("azure".to_string(), azure.clone());
                backends.insert("az".to_string(), azure.clone());
                default_backend = Some(azure);
            }
            #[cfg(not(feature = "azure"))]
            "azure" | "az" => {
                return Err(BlobError::UnsupportedBackend {
                    scheme: "azure (feature not enabled)".to_string(),
                });
            }
            scheme => {
                return Err(BlobError::UnsupportedBackend {
                    scheme: scheme.to_string(),
                });
            }
        }

        // Always include local backend as fallback
        if !backends.contains_key("file") {
            backends.insert("file".to_string(), Arc::new(LocalBlobStore::new()));
        }

        Ok(Self {
            backends,
            default_backend,
        })
    }

    /// Extract URI scheme (e.g., "s3", "file", "gs", "azure").
    fn extract_scheme(uri: &str) -> BlobResult<String> {
        let parts: Vec<&str> = uri.splitn(2, "://").collect();
        if parts.len() != 2 {
            return Err(BlobError::InvalidUri {
                uri: uri.to_string(),
                reason: "Missing scheme (expected format: scheme://...)".to_string(),
            });
        }
        Ok(parts[0].to_string())
    }

    /// Route to the appropriate backend based on URI scheme.
    fn route(&self, uri: &str) -> BlobResult<&dyn BlobStore> {
        let scheme = Self::extract_scheme(uri)?;

        self.backends
            .get(&scheme)
            .map(|b| b.as_ref())
            .or_else(|| self.default_backend.as_ref().map(|b| b.as_ref()))
            .ok_or_else(|| BlobError::UnsupportedBackend { scheme })
    }
}

#[async_trait]
impl BlobStore for BlobStoreDispatcher {
    async fn get(&self, uri: &str) -> BlobResult<Vec<u8>> {
        self.route(uri)?.get(uri).await
    }

    async fn get_range(&self, uri: &str, range: Range<u64>) -> BlobResult<Vec<u8>> {
        self.route(uri)?.get_range(uri, range).await
    }

    async fn get_metadata(&self, uri: &str) -> BlobResult<BlobMetadata> {
        self.route(uri)?.get_metadata(uri).await
    }

    async fn upload(&self, uri: &str, data: Vec<u8>) -> BlobResult<()> {
        self.route(uri)?.upload(uri, data).await
    }

    async fn presign_get_uri(&self, uri: &str, expires_in: Duration) -> BlobResult<String> {
        self.route(uri)?.presign_get_uri(uri, expires_in).await
    }

    async fn presign_upload_part_uri(
        &self,
        uri: &str,
        part_number: u32,
        upload_id: &str,
        expires_in: Duration,
    ) -> BlobResult<String> {
        self.route(uri)?
            .presign_upload_part_uri(uri, part_number, upload_id, expires_in)
            .await
    }

    async fn create_multipart_upload(&self, uri: &str) -> BlobResult<String> {
        self.route(uri)?.create_multipart_upload(uri).await
    }

    async fn complete_multipart_upload(
        &self,
        uri: &str,
        upload_id: &str,
        parts_etags: Vec<String>,
    ) -> BlobResult<()> {
        self.route(uri)?
            .complete_multipart_upload(uri, upload_id, parts_etags)
            .await
    }

    async fn abort_multipart_upload(&self, uri: &str, upload_id: &str) -> BlobResult<()> {
        self.route(uri)?
            .abort_multipart_upload(uri, upload_id)
            .await
    }
}
