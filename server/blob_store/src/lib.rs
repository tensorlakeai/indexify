use std::{env, fmt::Debug, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::{Bytes, BytesMut};
use futures::{stream::BoxStream, StreamExt};
use metrics::{blob_storage, Timer};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    parse_url,
    path::Path,
    ObjectStore,
    ObjectStoreScheme,
    WriteMultipart,
};
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::debug;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobStorageConfig {
    pub path: String,
}

impl BlobStorageConfig {
    pub fn new(path: &str) -> Self {
        BlobStorageConfig {
            path: format!("file://{}", path),
        }
    }
}

impl Default for BlobStorageConfig {
    fn default() -> Self {
        let blob_store_path = format!(
            "file://{}",
            env::current_dir()
                .expect("unable to get current directory")
                .join("indexify_storage/blobs")
                .to_str()
                .expect("unable to get path as string")
        );
        BlobStorageConfig {
            path: blob_store_path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub url: String,
    pub size_bytes: u64,
    pub sha256_hash: String,
}

pub struct BlobStorage {
    object_store: Arc<dyn ObjectStore>,
    url_scheme: String,
    url: String,
    path: Path,
    metrics: blob_storage::Metrics,
}

impl BlobStorage {
    pub fn new(config: BlobStorageConfig) -> Result<Self> {
        let url = &config.path.clone();
        debug!("using blob store path: {}", url);
        let (object_store, path) = Self::build_object_store(url)?;
        Ok(Self {
            object_store: Arc::new(object_store),
            url_scheme: url.parse::<Url>()?.scheme().to_string(),
            url: url.clone(),
            path,
            metrics: blob_storage::Metrics::new(),
        })
    }

    pub fn build_object_store(url_str: &str) -> Result<(Box<dyn ObjectStore>, Path)> {
        let url = &url_str.parse::<Url>()?;
        let (scheme, _) = ObjectStoreScheme::parse(url)?;
        match scheme {
            ObjectStoreScheme::AmazonS3 => {
                // inject AWS environment variables to prioritize keys over instance metadata
                // credentials.
                let s3_builder = AmazonS3Builder::from_env()
                    .with_url(url_str)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()
                    .expect("failed to create object store");
                let (_, path) = parse_url(url)?;
                Ok((Box::new(s3_builder), path))
            }
            _ => Ok(parse_url(url)?),
        }
    }

    pub fn get_object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    pub fn get_url(&self) -> String {
        self.url.clone()
    }

    pub fn get_url_scheme(&self) -> String {
        self.url_scheme.clone()
    }

    pub fn get_path(&self) -> Path {
        self.path.clone()
    }

    pub async fn put(
        &self,
        key: &str,
        data: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        let timer_kvs = &[KeyValue::new("op", "put")];
        let _timer = Timer::start_with_labels(&self.metrics.operations, timer_kvs);
        let mut hasher = Sha256::new();
        let mut hashed_stream = data.map(|item| {
            item.map(|bytes| {
                hasher.update(&bytes);
                bytes
            })
        });

        let path = self.path.child(key);
        let m = self.object_store.put_multipart(&path).await?;
        let mut w = WriteMultipart::new(m);
        let mut size_bytes = 0;
        while let Some(chunk) = hashed_stream.next().await {
            w.wait_for_capacity(1).await?;
            let chunk = chunk?;
            size_bytes += chunk.len() as u64;
            w.write(&chunk);
        }
        w.finish().await?;

        let hash = format!("{:x}", hasher.finalize());
        Ok(PutResult {
            url: path.to_string(),
            size_bytes,
            sha256_hash: hash,
        })
    }

    pub async fn get(&self, path: &str) -> Result<BoxStream<'static, Result<Bytes>>> {
        let timer_kvs = &[KeyValue::new("op", "get")];
        let _timer = Timer::start_with_labels(&self.metrics.operations, timer_kvs);
        let client_clone = self.object_store.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let get_result = client_clone
            .get(&path.into())
            .await
            .map_err(|e| anyhow!("can't get s3 object {:?}: {:?}", path, e))?;
        let path = path.to_string();
        tokio::spawn(async move {
            let mut stream = get_result.into_stream();
            while let Some(chunk) = stream.next().await {
                let _ =
                    tx.send(chunk.map_err(|e| {
                        anyhow!("error reading s3 object {:?}: {:?}", path.clone(), e)
                    }));
            }
        });
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.object_store
            .delete(&object_store::path::Path::from(key))
            .await?;
        Ok(())
    }

    pub async fn read_bytes(&self, key: &str) -> Result<Bytes> {
        let mut reader = self.get(key).await?;
        let mut bytes = BytesMut::new();
        while let Some(chunk) = reader.next().await {
            bytes.extend_from_slice(&chunk?);
        }
        Ok(bytes.into())
    }
}
