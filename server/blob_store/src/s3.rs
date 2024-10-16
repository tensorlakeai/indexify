use std::{env, sync::Arc};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::{aws::AmazonS3Builder, ObjectStore};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{BlobStorageConfig, BlobStorageReader};

pub struct S3FileReader {
    client: Arc<dyn ObjectStore>,
    key: String,
}

impl S3FileReader {
    pub fn new(bucket: &str, key: &str, config: &BlobStorageConfig) -> Self {
        let mut builder = AmazonS3Builder::from_env();
        if let Some(s3) = &config.s3 {
            builder = builder.with_region(&s3.region);
        }

        // For supporting localstack/minio for testing
        if let Ok(val) = env::var("AWS_ENDPOINT_URL") {
            builder = builder.with_endpoint(val.clone());
            if val.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        let client = builder.with_bucket_name(bucket).build().unwrap();
        S3FileReader {
            client: Arc::new(client),
            key: key.to_string(),
        }
    }
}

#[async_trait]
impl BlobStorageReader for S3FileReader {
    async fn get(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        let client_clone = self.client.clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let key = self.key.clone();
        let get_result = client_clone
            .get(&key.clone().into())
            .await
            .map_err(|e| anyhow!("can't get s3 object {:?}: {:?}", key.clone(), e))?;
        tokio::spawn(async move {
            let mut stream = get_result.into_stream();
            while let Some(chunk) = stream.next().await {
                let _ = tx
                    .send(chunk.map_err(|e| {
                        anyhow!("error reading s3 object {:?}: {:?}", key.clone(), e)
                    }));
            }
        });
        Ok(Box::pin(UnboundedReceiverStream::new(rx)))
    }
}
