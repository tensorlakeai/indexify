//! Blob storage configuration.

use std::env;

use serde::{Deserialize, Serialize};

/// Configuration for blob storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    /// Storage path (e.g., `file:///path`, `s3://bucket/prefix`, `gs://bucket/prefix`).
    #[serde(default = "default_blob_store_path")]
    pub path: String,

    /// AWS region (for S3).
    #[serde(default)]
    pub region: Option<String>,

    /// Azure storage account name.
    #[serde(default)]
    pub azure_storage_account: Option<String>,

    /// GCP project ID.
    #[serde(default)]
    pub gcp_project_id: Option<String>,
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        Self {
            path: default_blob_store_path(),
            region: None,
            azure_storage_account: None,
            gcp_project_id: None,
        }
    }
}

/// Default blob store path (local filesystem).
pub fn default_blob_store_path() -> String {
    format!(
        "file://{}",
        env::current_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("."))
            .join("indexify_storage/blobs")
            .to_str()
            .unwrap_or("./indexify_storage/blobs")
    )
}
