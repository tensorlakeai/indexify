use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};

use super::BlobStorageReader;

pub struct HttpReader {
    url: String,
}

impl HttpReader {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
        }
    }
}

#[async_trait]
impl BlobStorageReader for HttpReader {
    async fn get(&self) -> Result<BoxStream<'static, Result<Bytes>>> {
        let client = reqwest::Client::new();
        let response = client.get(&self.url).send().await?;
        let stream = async_stream::stream! {
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                yield chunk.map_err(|e| anyhow!("Failed to read chunk: {}", e));
            }
        };
        Ok(Box::pin(stream))
    }
}
