use std::env;

use serde::{Deserialize, Serialize};

pub mod registry;

// Re-export types from shared blob_store crate
pub use blob_store::{BlobStorage, PutResult};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    #[serde(default = "default_blob_store_path")]
    pub path: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub gcp_project_id: Option<String>,
    #[serde(default)]
    pub azure_storage_account: Option<String>,
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        BlobStorageConfig {
            path: default_blob_store_path(),
            region: None,
            gcp_project_id: None,
            azure_storage_account: None,
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

impl BlobStorageConfig {
    /// Convert to shared blob_store crate's config
    pub fn to_blob_store_config(&self) -> blob_store::BlobStorageConfig {
        blob_store::BlobStorageConfig {
            path: self.path.clone(),
            region: self.region.clone(),
            gcp_project_id: self.gcp_project_id.clone(),
            azure_storage_account: self.azure_storage_account.clone(),
        }
    }
}
