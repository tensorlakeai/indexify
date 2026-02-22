// Re-export URI helpers from the shared blob store crate.
pub use indexify_blob_store::uri::{blob_store_path_to_url, blob_store_url_to_path};

use crate::executor_api::executor_api_pb;

pub fn string_to_data_payload_encoding(value: &str) -> executor_api_pb::DataPayloadEncoding {
    match value {
        "application/json" => executor_api_pb::DataPayloadEncoding::Utf8Json,
        "application/python-pickle" => executor_api_pb::DataPayloadEncoding::BinaryPickle,
        "application/zip" => executor_api_pb::DataPayloadEncoding::BinaryZip,
        "text/plain" => executor_api_pb::DataPayloadEncoding::Utf8Text,
        // User supplied content type for tensorlake.File.
        _ => executor_api_pb::DataPayloadEncoding::Raw,
    }
}
