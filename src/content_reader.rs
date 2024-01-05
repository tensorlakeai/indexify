use crate::{blob_storage::BlobStorageBuilder, internal_api::ContentMetadata};

pub struct ContentReader {
    content_metadata: ContentMetadata,
}

impl ContentReader {
    pub fn new(content_metadata: ContentMetadata) -> Self {
        Self { content_metadata }
    }

    pub async fn read(&self) -> Result<Vec<u8>, anyhow::Error> {
        let blob_storage_reader =
            BlobStorageBuilder::reader_from_link(&self.content_metadata.storage_url)?;
        return blob_storage_reader
            .get(&self.content_metadata.storage_url)
            .await;
    }
}
