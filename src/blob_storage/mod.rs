use crate::BlobStorageConfig;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

pub mod disk;

pub type BlobStorageTS = Arc<dyn BlobStorage + Sync + Send>;
#[async_trait]
pub trait BlobStorage {
    async fn get(&self, key: &str) -> Result<Vec<u8>, anyhow::Error>;
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error>;
    fn delete(&self, key: &str) -> Result<(), anyhow::Error>;
}

pub struct BlobStorageBuilder {
    config: Arc<BlobStorageConfig>,
}

impl BlobStorageBuilder {
    pub fn new(config: Arc<BlobStorageConfig>) -> BlobStorageBuilder {
        Self { config }
    }

    pub fn build(&self) -> Result<BlobStorageTS, anyhow::Error> {
        match self.config.backend.as_str() {
            "disk" => {
                let disk_config = self.config.disk.clone().unwrap();
                let storage = disk::DiskStorage::new(disk_config.path)?;
                Ok(Arc::new(storage))
            }
            _ => Err(anyhow::anyhow!("Unknown blob storage backend")),
        }
    }

    #[allow(dead_code)]
    pub fn new_disk_storage(path: String) -> Result<BlobStorageTS, anyhow::Error> {
        let storage = disk::DiskStorage::new(path)?;
        Ok(Arc::new(storage))
    }
}
