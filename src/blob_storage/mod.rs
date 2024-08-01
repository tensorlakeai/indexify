use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream::BoxStream, StreamExt};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;

use self::{disk::DiskFileReader, s3::S3FileReader};
use crate::server_config::ServerConfig;

pub mod disk;
pub mod http;
pub mod s3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskStorageConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    pub s3: Option<S3Config>,
    pub disk: Option<DiskStorageConfig>,
}

#[derive(Debug)]
pub struct PutResult {
    pub url: String,
    pub size_bytes: u64,
}

#[async_trait]
pub trait BlobStorageWriter {
    async fn put(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error>;
    async fn delete(&self, key: &str) -> Result<()>;
}

#[async_trait]
pub trait BlobStoragePartWriter {
    async fn writer(&self, key: &str) -> Result<StoragePartWriter>;
}

type BlobStorageReaderTS = Arc<dyn BlobStorageReader + Sync + Send>;

#[async_trait]
pub trait BlobStorageReader {
    async fn get(&self, key: &str) -> Result<BoxStream<'static, Result<Bytes>>>;
}

#[derive(Clone)]
pub struct BlobStorage {
    config: BlobStorageConfig,
}

impl Debug for BlobStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlobStorage")
            .field("config", &"<hidden>")
            .finish()
    }
}

pub struct StoragePartWriter {
    pub writer: Box<dyn AsyncWrite + Send + Unpin>,
    pub url: String,
}

impl std::fmt::Debug for StoragePartWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoragePartWriter")
            .field("writer", &"<hidden>")
            .field("url", &self.url)
            .finish()
    }
}

impl BlobStorage {
    pub fn new_with_config(config: BlobStorageConfig) -> Self {
        Self { config }
    }

    fn s3_storage(&self, s3: &S3Config) -> Result<s3::S3Storage> {
        Ok(s3::S3Storage::new(
            &s3.bucket,
            AmazonS3Builder::from_env()
                .with_region(s3.region.as_str())
                .with_allow_http(true)
                .with_bucket_name(s3.bucket.clone())
                .build()
                .context("unable to build S3 builder")?,
        ))
    }

    pub async fn writer(&self, _namespace: &str, key: &str) -> Result<StoragePartWriter> {
        if let Some(s3) = self.config.s3.as_ref() {
            self.s3_storage(s3)?.writer(key).await
        } else {
            // If it's not S3, assume it's a file

            // We need a default implementation for `DiskStorageConfig`
            let storage = disk::DiskStorage::new(self.config.disk.clone().unwrap_or_else(|| {
                DiskStorageConfig {
                    path: "blobs".to_string(),
                }
            }))?;
            storage.writer(key).await
        }
    }
}

#[async_trait]
impl BlobStorageWriter for BlobStorage {
    async fn put(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        if let Some(s3) = self.config.s3.as_ref() {
            self.s3_storage(s3)?.put(key, data).await
        } else {
            // If it's not S3, assume it's a file

            // We need a default implementation for `DiskStorageConfig`
            disk::DiskStorage::new(
                self.config
                    .disk
                    .clone()
                    .unwrap_or_else(|| DiskStorageConfig {
                        path: "blobs".to_string(),
                    }),
            )?
            .put(key, data)
            .await
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))?;
            return s3::S3Storage::new(
                key,
                AmazonS3Builder::from_env()
                    .with_region(
                        self.config
                            .s3
                            .as_ref()
                            .map(|config| config.region.as_str())
                            .unwrap_or("us-east-1"),
                    )
                    .with_bucket_name(bucket)
                    .build()
                    .context("unable to build S3 builder")?,
            )
            .delete(key)
            .await;
        } else if key.starts_with("http://") || key.starts_with("https://") {
            return Ok(());
        }

        // If it's not S3, assume it's a file

        // We need a default implementation for `DiskStorageConfig`
        disk::DiskStorage::new(
            self.config
                .disk
                .clone()
                .unwrap_or_else(|| DiskStorageConfig {
                    path: "blobs".to_string(),
                }),
        )?
        .delete(key)
        .await
    }
}

fn parse_s3_url(s3_url: &str) -> Result<(&str, &str), &str> {
    let Some(("s3", url)) = s3_url.split_once("://") else {
        return Err("Invalid S3 URL format");
    };

    let Some((bucket, key)) = url.split_once('/') else {
        return Err("Invalid S3 URL format");
    };

    Ok((bucket, key))
}

#[derive(Debug)]
pub struct ContentReader {
    config: Arc<ServerConfig>,
}

impl ContentReader {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self { config }
    }

    pub fn get(&self, key: &str) -> BlobStorageReaderTS {
        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))
                .unwrap();
            return Arc::new(S3FileReader::new(bucket, key, &self.config));
        }

        if key.starts_with("http") {
            return Arc::new(http::HttpReader {});
        }

        // If it's not S3, assume it's a file
        Arc::new(DiskFileReader::new())
    }

    pub async fn bytes(&self, key: &str) -> Result<Bytes> {
        let reader = self.get(key);
        let mut stream = reader.get(key).await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.into())
    }
}

#[cfg(test)]
mod tests {
    use core::pin::pin;

    use futures::{stream, TryStreamExt};
    use object_store::{aws::AmazonS3, ObjectStore};
    use tokio::io::AsyncWriteExt;

    use super::*;

    // This test requires localstack to be running.
    // Configure with key: test and secret: test
    // Make bucket 'test-bucket'

    fn test_client() -> AmazonS3 {
        AmazonS3Builder::from_env()
            .with_bucket_name("test-bucket")
            .with_region("us-east-1")
            .build()
            .unwrap()
    }

    fn set_aws_env() {
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
        std::env::set_var("AWS_ENDPOINT", "http://localhost:4566");
        std::env::set_var("AWS_ALLOW_HTTP", "1");
    }

    #[tokio::test]
    async fn test_put() {
        set_aws_env();

        let client = test_client();

        let list_result = client.list(None).try_collect::<Vec<_>>().await;
        if list_result.is_err() {
            println!("localstack not configured skipping test");
            return;
        }
        let data = vec![
            Bytes::from("test_data_1"),
            Bytes::from("test_data_2"),
            Bytes::from("test_data_3"),
        ];
        let stream = stream::iter(data.into_iter().map(Ok));

        let storage = BlobStorage::new_with_config(BlobStorageConfig {
            s3: Some(S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
            }),
            disk: None,
        });
        let result = storage.put("test-key-2", pin!(stream)).await;

        assert!(result.is_ok());

        let read_client = test_client();
        let path = object_store::path::Path::from("test-key-2");
        let res = read_client.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(res, "test_data_1test_data_2test_data_3");

        storage.delete("s3://test-bucket/test-key-2").await.unwrap();
    }

    #[tokio::test]
    async fn test_writer() {
        set_aws_env();

        let client = test_client();

        let list_result = client.list(None).try_collect::<Vec<_>>().await;
        if list_result.is_err() {
            println!("localstack not configured skipping test");
            return;
        }
        let data = vec![
            Bytes::from("test_data_1"),
            Bytes::from("test_data_2"),
            Bytes::from("test_data_3"),
        ];

        let storage = BlobStorage::new_with_config(BlobStorageConfig {
            s3: Some(S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
            }),
            disk: None,
        });
        let mut writer = storage
            .writer("test-namespace", "test-key-3")
            .await
            .unwrap();
        for chunk in data {
            writer.writer.write_all(&chunk).await.unwrap();
        }
        writer.writer.shutdown().await.unwrap();

        let read_client = test_client();
        let path = object_store::path::Path::from("test-key-3");
        let res = read_client.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(res, "test_data_1test_data_2test_data_3");

        storage.delete("s3://test-bucket/test-key-3").await.unwrap();
    }
}
