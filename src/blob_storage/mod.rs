use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;

use self::s3::S3Storage;
use crate::server_config::BlobStorageConfig;

pub mod disk;
pub mod s3;

pub type BlobStorageTS = Arc<dyn BlobStorage + Sync + Send>;

pub type BlobStorageReaderTS = Arc<dyn BlobStorageReader + Sync + Send>;

#[async_trait]
pub trait BlobStorage {
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error>;
    fn delete(&self, key: &str) -> Result<()>;
}

#[async_trait]
pub trait BlobStorageReader {
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
}

pub struct BlobStorageBuilder {
    config: Arc<BlobStorageConfig>,
}

impl BlobStorageBuilder {
    pub fn new(config: Arc<BlobStorageConfig>) -> BlobStorageBuilder {
        Self { config }
    }

    pub fn reader_from_link(link: &str) -> Result<BlobStorageReaderTS, anyhow::Error> {
        if link.starts_with("file://") {
            return Ok(Arc::new(disk::DiskStorageReader {}));
        }
        if link.starts_with("s3://") {
            let client = AmazonS3Builder::from_env().with_url(link).build()?;
            return Ok(Arc::new(S3Storage::new(client)));
        }
        Err(anyhow!("Unknown blob storage backend {}", link))
    }

    pub fn build(&self) -> Result<BlobStorageTS, anyhow::Error> {
        match self.config.backend.as_str() {
            "disk" => {
                let disk_config = self
                    .config
                    .disk
                    .clone()
                    .ok_or(anyhow!("disk config not found"))?;
                let storage = disk::DiskStorage::new(disk_config.path)?;
                Ok(Arc::new(storage))
            }
            "s3" => {
                let s3_config = self
                    .config
                    .s3
                    .clone()
                    .ok_or(anyhow!("s3 config not found"))?;
                let client = AmazonS3Builder::from_env()
                    .with_region(s3_config.region)
                    .with_bucket_name(s3_config.bucket)
                    .build()
                    .map_err(|e| anyhow!("Failed to create S3 client: {}", e))?;
                Ok(Arc::new(S3Storage::new(client)))
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
