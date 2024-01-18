use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{aws::AmazonS3, ObjectStore};

use super::{BlobStorage, BlobStorageReader};

pub struct S3Storage {
    client: AmazonS3,
}

impl S3Storage {
    pub fn new(client: AmazonS3) -> Self {
        S3Storage { client }
    }
}

#[async_trait]
impl BlobStorage for S3Storage {
    async fn put(&self, key: &str, data: Bytes) -> Result<String> {
        self.client.put(&key.into(), data).await?;
        Ok(key.to_string())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let _ = self.client.delete(&key.into());
        Ok(())
    }
}

#[async_trait]
impl BlobStorageReader for S3Storage {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.client.get(&key.into()).await?;
        let data = data
            .bytes()
            .await
            .map_err(|e| anyhow!("Failed to read bytes from key: {}, {}", key, e))?;
        Ok(data.to_vec())
    }
}
