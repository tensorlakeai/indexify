use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use anyhow::Result;

use super::{BlobStorage, BlobStorageConfig};

pub struct BlobStorageRegistry {
    pub blob_storage_buckets: Mutex<HashMap<String, Arc<BlobStorage>>>,
    pub default_blob_storage: Arc<BlobStorage>,
}

impl BlobStorageRegistry {
    pub fn new(
        default_blob_storage_path: &str,
        default_blob_storage_region: Option<String>,
    ) -> Result<Self> {
        let default_blob_storage = BlobStorage::new(BlobStorageConfig {
            path: default_blob_storage_path.to_string(),
            region: default_blob_storage_region,
        })?;
        Ok(Self {
            blob_storage_buckets: Mutex::new(HashMap::new()),
            default_blob_storage: Arc::new(default_blob_storage),
        })
    }

    pub fn create_new_blob_store(
        &self,
        namespace: &str,
        path: &str,
        region: Option<String>,
    ) -> Result<()> {
        let blob_storage = BlobStorage::new(BlobStorageConfig {
            path: path.to_string(),
            region,
        })?;
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
}
