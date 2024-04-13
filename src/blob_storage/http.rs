use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};

use super::BlobStorageReader;

pub struct HttpReader {}

impl BlobStorageReader for HttpReader {
    fn get(&self, key: &str) -> BoxStream<Result<Bytes>> {
        let client = reqwest::Client::new();
        let key = key.to_string();
        let stream = async_stream::stream! {
            let response = client.get(key).send().await?;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                yield chunk.map_err(|e| anyhow!("Failed to read chunk: {}", e));
            }
        };
        Box::pin(stream)
    }
}
