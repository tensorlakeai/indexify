use crate::{blob_storage::BlobStorageBuilder, internal_api::ContentPayload};

pub struct ContentReader {
    payload: ContentPayload,
}

impl ContentReader {
    pub fn new(payload: ContentPayload) -> Self {
        Self { payload }
    }

    pub async fn read(&self) -> Result<Vec<u8>, anyhow::Error> {
        if let Some(external_url) = &self.payload.external_url {
            let blob_storage_reader = BlobStorageBuilder::reader_from_link(external_url)?;
            return blob_storage_reader.get(external_url).await;
        }
        Ok(self.payload.content.clone().into_bytes())
    }
}
