use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::FuturesOrdered, StreamExt};
use object_store::{aws::AmazonS3, ObjectStore};

use super::{BlobStorageReader, BlobStorageWriter};

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
impl BlobStorageWriter for S3Storage {
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

#[async_trait]
impl BlobStorageReader for S3Storage {
    async fn get(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        let mut readers = FuturesOrdered::new();
        for &key in keys {
            readers.push_back(async {
                Ok::<_, Error>(
                    self.client
                        .get(&key.into())
                        .await?
                        .bytes()
                        .await
                        .map_err(|e| {
                            anyhow!("Failed to read bytes from key: {}, {}", key.to_owned(), e)
                        })?
                        .to_vec(),
                )
            });
        }

        let mut buffers = Vec::with_capacity(keys.len());

        while let Some(blob) = readers.next().await {
            buffers.push(blob?);
        }

        Ok(buffers)
    }
}
