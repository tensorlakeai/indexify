use crate::{
    blob_storage::BlobStorageBuilder,
    internal_api::ContentMetadata,
};

pub struct ContentReader {
    metadata: ContentMetadata,
}

impl ContentReader {
    pub fn new(metadata: ContentMetadata) -> Self {
        Self { metadata }
    }

    pub async fn read(&self) -> Result<Vec<u8>, anyhow::Error> {
        let blob_storage_reader = BlobStorageBuilder::reader_from_link(&self.metadata.storage_url)?;
        blob_storage_reader.get(&self.metadata.storage_url).await
    }
}
