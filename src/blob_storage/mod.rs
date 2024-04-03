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

pub mod disk;
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
    async fn put(&self, key: &str, data: Bytes) -> Result<PutResult, anyhow::Error>;
    async fn put_stream(
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

pub trait BlobStorageReader {
    fn get(&self, key: &str) -> BoxStream<Result<Bytes>>;
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

    pub async fn writer(&self, _namespace: &str, key: &str) -> Result<StoragePartWriter> {
        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))?;
            s3::S3Storage::new(
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
            .writer(key)
            .await
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
    async fn put(&self, key: &str, data: Bytes) -> Result<PutResult, anyhow::Error> {
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
            .put(key, data)
            .await;
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
        .put(key, data)
        .await
    }

    async fn put_stream(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        println!("in put_stream in blob store");
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
            .put_stream(key, data)
            .await;
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
        .put_stream(key, data)
        .await
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
pub struct ContentReader {}

impl Default for ContentReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentReader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get(&self, key: &str) -> BlobStorageReaderTS {
        if key.starts_with("s3://") {
            let (bucket, _key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))
                .unwrap();
            return Arc::new(S3FileReader::new(bucket));
        }

        // If it's not S3, assume it's a file
        Arc::new(DiskFileReader::new())
    }

    pub async fn bytes(&self, key: &str) -> Result<Bytes> {
        let reader = self.get(key);
        let mut stream = reader.get(key);
        let mut bytes = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.into())
    }
}
