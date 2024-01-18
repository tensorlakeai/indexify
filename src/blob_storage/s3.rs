use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::{aws::AmazonS3, ObjectStore};

use super::{BlobStorage, BlobStorageReader};

pub struct S3Storage {
    bucket: String,
    client: AmazonS3,
}

impl S3Storage {
    pub fn new(bucket: &str, client: AmazonS3) -> Self {
        S3Storage {
            bucket: bucket.to_string(),
            client,
        }
    }
}

#[async_trait]
impl BlobStorage for S3Storage {
    async fn put(&self, key: &str, data: Bytes) -> Result<String> {
        self.client.put(&key.into(), data).await?;
        Ok(format!("s3://{}/{}", self.bucket, key))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let _ = self
            .client
            .delete(&key.into())
            .await
            .map_err(|e| anyhow!("Failed to delete key: {}, error: {}", key, e.to_string()))?;
        Ok(())
    }
}

pub struct S3Reader {
    client: AmazonS3,
    key: String,
}

impl S3Reader {
    pub fn new(client: AmazonS3, key: &str) -> Self {
        S3Reader {
            client,
            key: key.to_string(),
        }
    }
}

#[async_trait]
impl BlobStorageReader for S3Reader {
    async fn get(&self) -> Result<Vec<u8>> {
        let data = self.client.get(&self.key.clone().as_str().into()).await?;
        let data = data
            .bytes()
            .await
            .map_err(|e| anyhow!("Failed to read bytes from key: {}, {}", self.key, e))?;
        Ok(data.to_vec())
    }
}
