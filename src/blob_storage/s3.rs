use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    ObjectStore,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

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

pub struct S3FileReader {
    client: Arc<dyn ObjectStore>,
}

impl S3FileReader {
    pub fn new(bucket: &str) -> Self {
        let client = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region("us-west-2")
            .build()
            .unwrap();
        S3FileReader {
            client: Arc::new(client),
        }
    }
}

#[async_trait]
impl BlobStorageReader for S3FileReader {
    fn get(&self, key: &str) -> BoxStream<Result<Bytes>> {
        let client_clone = self.client.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let key = key.to_string();
        tokio::spawn(async move {
            let mut stream = client_clone.get(&key.into()).await.unwrap().into_stream();
            while let Some(chunk) = stream.next().await {
                if let Ok(chunk) = chunk {
                    let _ = tx.send(Ok(chunk));
                }
            }
        });
        Box::pin(UnboundedReceiverStream::new(rx))
    }
}
