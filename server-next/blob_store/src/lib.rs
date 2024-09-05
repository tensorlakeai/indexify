use std::{fmt::Debug, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    local, ObjectStore, WriteMultipart,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;

use self::{disk::DiskFileReader, s3::S3FileReader};

pub mod disk;
pub mod http;
pub mod s3;

type BlobStorageReaderTS = Arc<dyn BlobStorageReader + Sync + Send>;

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

#[async_trait]
pub trait BlobStorageReader {
    async fn get(&self, key: &str) -> Result<BoxStream<'static, Result<Bytes>>>;
}

#[derive(Clone)]
pub struct BlobStorage {
    object_store: Arc<dyn ObjectStore>,
    config: BlobStorageConfig,
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

fn s3_storage(s3: &S3Config) -> Result<AmazonS3> {
    Ok(AmazonS3Builder::from_env()
        .with_region(s3.region.as_str())
        .with_allow_http(true)
        .with_bucket_name(s3.bucket.clone())
        .build()
        .context("unable to build S3 builder")?)
}

fn file_storage(disk: DiskStorageConfig) -> Result<local::LocalFileSystem> {
    std::fs::create_dir_all(&disk.path)?;
    let s = local::LocalFileSystem::new_with_prefix(disk.path)?;
    Ok(s)
}

impl BlobStorage {
    pub fn new(config: BlobStorageConfig) -> Result<Self> {
        let object_store: Arc<dyn ObjectStore> = if let Some(s3) = config.s3.as_ref() {
            let s = s3_storage(s3)?;
            Arc::new(s)
        } else {
            // If it's not S3, assume it's a file
            let s = file_storage(config.disk.clone().unwrap_or_else(|| DiskStorageConfig {
                path: "blobs".to_string(),
            }))?;
            Arc::new(s)
        };
        Ok(Self {
            object_store,
            config,
        })
    }

    pub async fn put(
        &self,
        key: &str,
        mut data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        let path = object_store::path::Path::from(key);
        let m = self.object_store.put_multipart(&path).await?;
        let mut w = WriteMultipart::new(m);
        let mut size_bytes = 0;
        while let Some(chunk) = data.next().await {
            w.wait_for_capacity(1).await?;
            let chunk = chunk?;
            size_bytes += chunk.len() as u64;
            w.write(&chunk);
        }
        w.finish().await?;
        Ok(PutResult {
            url: self.path_url(&path),
            size_bytes,
        })
    }

    pub fn path_url(&self, path: &object_store::path::Path) -> String {
        if let Some(s3) = &self.config.s3 {
            format!("s3://{}/{}", s3.bucket, path)
        } else {
            // If it's not S3, assume it's a file
            format!(
                "file://{}/{}",
                self.config.disk.as_ref().unwrap().path,
                path
            )
        }
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

    pub async fn read_bytes(&self, key: &str) -> Result<Bytes> {
        let reader = self.get(key);
        let mut stream = reader.get(key).await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.into())
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
