use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{aws::AmazonS3Builder, parse_url};
use url::Url;

use self::s3::{S3Reader, S3Storage};
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
    async fn get(&self) -> Result<Vec<u8>>;
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
            return Ok(Arc::new(disk::DiskStorageReader::new(link.to_string())));
        }
        if link.starts_with("s3://") {
            let (bucket, key) =
                parse_s3_url(link).map_err(|e| anyhow!("unable to parse s3 url: {}", e))?;
            let client = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()?;
            return Ok(Arc::new(S3Reader::new(client, &key)));
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
                    .with_bucket_name(s3_config.bucket.clone())
                    .build()
                    .map_err(|e| anyhow!("Failed to create S3 client: {}", e))?;
                Ok(Arc::new(S3Storage::new(&s3_config.bucket, client)))
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

fn parse_s3_url(s3_url: &str) -> Result<(String, String), &'static str> {
    let parts: Vec<&str> = s3_url.splitn(2, "://").collect();
    if parts.len() != 2 {
        return Err("Invalid S3 URL format");
    }

    let bucket_and_key: Vec<&str> = parts[1].splitn(2, '/').collect();
    if bucket_and_key.len() != 2 {
        return Err("Invalid S3 URL format");
    }

    Ok((bucket_and_key[0].to_string(), bucket_and_key[1].to_string()))
}
