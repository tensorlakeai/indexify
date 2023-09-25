use crate::{
    blob_storage::BlobStorageTS,
    persistence::{ContentPayload, PayloadType},
};

pub struct ContentReaderBuilder {
    blob_storage: BlobStorageTS,
}

impl ContentReaderBuilder {
    pub fn new(blob_storage: BlobStorageTS) -> Self {
        Self { blob_storage }
    }

    pub fn build(&self, payload: ContentPayload) -> ContentReader {
        ContentReader::new(self.blob_storage.clone(), payload)
    }
}

pub struct ContentReader {
    blob_storage: BlobStorageTS,
    payload: ContentPayload,
}

impl ContentReader {
    pub fn new(blob_storage: BlobStorageTS, payload: ContentPayload) -> Self {
        Self {
            blob_storage,
            payload,
        }
    }

    pub async fn read(&self) -> Result<Vec<u8>, anyhow::Error> {
        match self.payload.payload_type {
            PayloadType::EmbeddedStorage => Ok(self.payload.payload.clone().into_bytes()),
            PayloadType::BlobStorageLink => {
                let blob = self.blob_storage.get(&self.payload.id).await;
                blob
            }
        }
    }
}
