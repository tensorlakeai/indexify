use tracing::error;

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

pub fn blob_store_path_to_url(
    path: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        // Local file blob store implementation is always using absolute paths without
        // "/"" prefix. The paths are not relative to the configure blob_store_url path.
        format!("{blob_store_url_scheme}:///{path}")
    } else if blob_store_url_scheme == "s3" {
        // S3 blob store implementation uses paths relative to its bucket from
        // blob_store_url.
        format!(
            "{}://{}/{}",
            blob_store_url_scheme,
            bucket_name_from_s3_blob_store_url(blob_store_url),
            path
        )
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

pub fn blob_store_url_to_path(
    url: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        // Local file blob store implementation is always using absolute paths without
        // "/"" prefix. The paths are not relative to the configure blob_store_url path.
        let prefix = format!("{blob_store_url_scheme}:///");
        url.strip_prefix(&prefix)
            // The url doesn't include blob_store_scheme if this payload was uploaded to server
            // instead of directly to blob storage.
            .unwrap_or(url)
            .to_string()
    } else if blob_store_url_scheme == "s3" {
        // S3 blob store implementation uses paths relative to its bucket from
        // blob_store_url.
        let prefix = format!(
            "{}://{}/",
            blob_store_url_scheme,
            bucket_name_from_s3_blob_store_url(blob_store_url)
        );
        url.strip_prefix(&prefix)
            // The url doesn't include blob_store_url if this payload was uploaded to server instead
            // of directly to blob storage.
            .unwrap_or(url)
            .to_string()
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

fn bucket_name_from_s3_blob_store_url(blob_store_url: &str) -> String {
    match url::Url::parse(blob_store_url) {
        Ok(url) => match url.host_str() {
            Some(bucket) => bucket.into(),
            None => {
                error!("Didn't find bucket name in S3 url: {}", blob_store_url);
                String::new()
            }
        },
        Err(e) => {
            error!(
                "Failed to parse blob_store_url: {}. Error: {}",
                blob_store_url, e
            );
            String::new()
        }
    }
}
