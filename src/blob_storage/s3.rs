use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, Stream, StreamExt};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    ObjectStore,
};
use tokio::{io::AsyncWriteExt, sync::mpsc};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{BlobStoragePartWriter, BlobStorageReader, BlobStorageWriter, StoragePartWriter};
use crate::blob_storage::PutResult;

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
    async fn put(
        &self,
        key: &str,
        mut data: impl Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult> {
        let (_, mut writer) = self.client.put_multipart(&key.into()).await?;
        let mut size_bytes: u64 = 0;
        while let Some(chunk) = data.next().await {
            let chunk = chunk?;
            size_bytes += chunk.len() as u64;
            writer.write_all(&chunk).await?;
        }
        writer.shutdown().await?;
        Ok(PutResult {
            url: format!("s3://{}/{}", self.bucket, key),
            size_bytes,
        })
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
impl BlobStoragePartWriter for S3Storage {
    async fn writer(&self, key: &str) -> Result<StoragePartWriter> {
        let (_, writer) = self.client.put_multipart(&key.into()).await?;
        Ok(StoragePartWriter {
            writer,
            url: format!("s3://{}/{}", self.bucket, key),
        })
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

#[cfg(test)]
mod tests {
    use core::pin::pin;

    use futures::{stream, TryStreamExt};

    use super::*;

    // This test requires localstack to be running.
    // Configure with key: test and secret: test
    // Make bucket 'test-bucket'

    fn test_client() -> AmazonS3 {
        AmazonS3Builder::new()
            .with_endpoint("http://localhost:4566")
            .with_access_key_id("test")
            .with_secret_access_key("test")
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_put() {
        let client = test_client();

        let list_result = client.list(None).try_collect::<Vec<_>>().await;
        if list_result.is_err() {
            println!("localstack not configured skipping test");
            return;
        }
        let storage = S3Storage::new("test-bucket", client);
        let data = vec![
            Bytes::from("test_data_1"),
            Bytes::from("test_data_2"),
            Bytes::from("test_data_3"),
        ];
        let stream = stream::iter(data.into_iter().map(Ok));
        let result = storage.put("test-key-2", pin!(stream)).await;
        assert!(result.is_ok());

        let read_client = test_client();
        let path = object_store::path::Path::from("test-key-2");
        let res = read_client.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(res, "test_data_1test_data_2test_data_3");

        storage.delete("test-key-2").await.unwrap();
    }
}
