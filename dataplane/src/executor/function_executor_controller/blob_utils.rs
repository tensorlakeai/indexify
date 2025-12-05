use crate::executor::{
    blob_store::{BlobStore, BlobStoreImpl},
    function_executor::function_executor_service::{Blob, BlobChunk},
};

const MAX_PRESIGNED_URI_EXPIRATION_SEC: u64 = 7 * 24 * 60 * 60;
const BLOB_OPTIMAL_CHUNK_SIZE_BYTES: u64 = 100 * 1024 * 1024;
const OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT: u64 = 100;
const OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES: u64 = 1 * 1024 * 1024 * 1024;

// pub async fn presign_read_only_blob(
//     blob_uri: &str,
//     size: u32,
//     blob_store: BlobStore,
// ) -> Result<Blob, Box<dyn std::error::Error>> {
//     let mut chunks: Vec<BlobChunk> = Vec::new();

//     while let mut chunk_total_size = 0 < size {
//         let upload_chunk_uri = blob_store
//             .presign_upload_part_uri(
//                 blob_uri,
//                 chunks.len() + 1 as i32,
//                 upload_id,
//                 MAX_PRESIGNED_URI_EXPIRATION_SEC,
//             )
//             .await?;
//         let chunk_size = if chunks.len() < OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT {
//             BLOB_OPTIMAL_CHUNK_SIZE_BYTES
//         } else {
//             OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES
//         };
//         chunk_total_size += chunk_size;
//         chunks.push(BlobChunk {
//             uri: Some(upload_chunk_uri),
//             size: Some(chunk_size),
//             etag: None,
//         });
//     }

//     return Blob {
//         id: Some(blob_id.to_string()),
//         chunks,
//     };
// }

pub async fn presign_write_only_blob(
    blob_id: &str,
    blob_uri: &str,
    upload_id: &str,
    size: u64,
    blob_store: BlobStore,
) -> Result<Blob, Box<dyn std::error::Error>> {
    let mut chunks: Vec<BlobChunk> = Vec::new();

    while let mut chunk_total_size = 0 < size {
        let upload_chunk_uri = blob_store
            .presign_upload_part_uri(
                blob_uri,
                chunks.len() + 1 as i32,
                upload_id,
                MAX_PRESIGNED_URI_EXPIRATION_SEC,
            )
            .await?;
        let chunk_size = if chunks.len() < OUTPUT_BLOB_OPTIMAL_CHUNKS_COUNT {
            BLOB_OPTIMAL_CHUNK_SIZE_BYTES
        } else {
            OUTPUT_BLOB_SLOWER_CHUNK_SIZE_BYTES
        };
        chunk_total_size += chunk_size;
        chunks.push(BlobChunk {
            uri: Some(upload_chunk_uri),
            size: Some(chunk_size),
            etag: None,
        });
    }

    return Blob {
        id: Some(blob_id.to_string()),
        chunks,
    };
}
