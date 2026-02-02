use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use blob_store::{BlobStorage, BlobStoreDispatcher};

use super::BlobStorageConfig;

pub struct BlobStorageRegistry {
    pub blob_storage_buckets: Mutex<HashMap<String, Arc<BlobStorage>>>,
    pub default_blob_storage: Arc<BlobStorage>,
}

impl BlobStorageRegistry {
    pub async fn new(
        default_blob_storage_path: &str,
        default_blob_storage_region: Option<String>,
    ) -> Result<Self> {
        let config = BlobStorageConfig {
            path: default_blob_storage_path.to_string(),
            region: default_blob_storage_region,
            gcp_project_id: None,
            azure_storage_account: None,
        };

        let default_blob_storage = Self::create_blob_storage(config).await?;
        Ok(Self {
            blob_storage_buckets: Mutex::new(HashMap::new()),
            default_blob_storage: Arc::new(default_blob_storage),
        })
    }

    pub async fn create_new_blob_store(
        &self,
        namespace: &str,
        path: &str,
        region: Option<String>,
    ) -> Result<()> {
        let config = BlobStorageConfig {
            path: path.to_string(),
            region,
            gcp_project_id: None,
            azure_storage_account: None,
        };

        let blob_storage = Self::create_blob_storage(config).await?;
        self.blob_storage_buckets
            .lock()
            .unwrap()
            .insert(namespace.to_string(), Arc::new(blob_storage));
        Ok(())
    }

    pub fn get_blob_store(&self, namespace: &str) -> Arc<BlobStorage> {
        self.blob_storage_buckets
            .lock()
            .unwrap()
            .get(namespace)
            .cloned()
            .unwrap_or_else(|| self.default_blob_storage.clone())
    }

    async fn create_blob_storage(config: BlobStorageConfig) -> Result<BlobStorage> {
        let blob_config = config.to_blob_store_config();
        let dispatcher = BlobStoreDispatcher::new(blob_config.clone()).await?;
        Ok(BlobStorage::new(dispatcher, blob_config.path))
    }
}
