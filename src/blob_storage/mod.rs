use std::fmt::{self, Debug, Formatter};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};

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

#[async_trait]
pub trait BlobStorageWriter {
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error>;
    async fn delete(&self, key: &str) -> Result<()>;
}

#[async_trait]
pub trait BlobStorageReader {
    async fn get(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>>;
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

impl BlobStorage {
    pub fn new() -> Self {
        Self {
            config: BlobStorageConfig {
                disk: None,
                s3: None,
            },
        }
    }

    pub fn new_with_config(config: BlobStorageConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl BlobStorageReader for BlobStorage {
    async fn get(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        // Note: If the keys are a mix of S3 URLs and disk URLs, only the first one will
        // be considered as the storage mechanism for ALL the keys
        let Some(&key) = keys.first() else {
            return Ok(vec![]);
        };

        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))?;
            return s3::S3Storage::new(
                key,
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .context("unable to build S3 builder")?,
            )
            .get(keys)
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
        .get(keys)
        .await
    }
}

#[async_trait]
impl BlobStorageWriter for BlobStorage {
    async fn put(&self, key: &str, data: Bytes) -> Result<String, anyhow::Error> {
        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))?;
            return s3::S3Storage::new(
                key,
                AmazonS3Builder::from_env()
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

    async fn delete(&self, key: &str) -> Result<()> {
        if key.starts_with("s3://") {
            let (bucket, key) = parse_s3_url(key)
                .map_err(|err| anyhow::anyhow!("unable to parse s3 url: {}", err))?;
            return s3::S3Storage::new(
                key,
                AmazonS3Builder::from_env()
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
