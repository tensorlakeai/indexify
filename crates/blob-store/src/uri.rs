//! URI parsing and conversion utilities for blob store URIs.
//!
//! Supports `s3://bucket/key` and `file:///path` URI schemes.

use std::path::PathBuf;

use anyhow::{Result, anyhow};

/// Check if a URI uses the `file://` scheme.
pub fn is_file_uri(uri: &str) -> bool {
    uri.starts_with("file://")
}

/// Check if a URI uses the `s3://` scheme.
pub fn is_s3_uri(uri: &str) -> bool {
    uri.starts_with("s3://")
}

/// Parse an S3 URI into `(bucket, key)`.
///
/// `s3://my-bucket/some/key` → `("my-bucket", "some/key")`
pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let uri = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("Not an S3 URI: {}", uri))?;
    let (bucket, key) = uri
        .split_once('/')
        .ok_or_else(|| anyhow!("Invalid S3 URI (no key): s3://{}", uri))?;
    Ok((bucket.to_string(), key.to_string()))
}

/// Convert a `file://` URI to a filesystem path.
///
/// `file:///tmp/data.bin` → `/tmp/data.bin`
pub fn file_uri_to_path(uri: &str) -> Result<PathBuf> {
    let path_str = uri
        .strip_prefix("file://")
        .ok_or_else(|| anyhow!("Not a file URI: {}", uri))?;
    Ok(PathBuf::from(path_str))
}

/// Convert a stored blob path to a full URL using the blob store scheme and
/// base URL.
///
/// This is used by the server to convert internal storage paths (as stored in
/// the database) into full URIs that can be sent to executors/dataplanes.
///
/// - For `file` scheme: `file:///{path}`
/// - For `s3` scheme: `s3://{bucket}/{path}`
pub fn blob_store_path_to_url(
    path: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        // Local file blob store uses absolute paths without "/" prefix.
        format!("{blob_store_url_scheme}:///{path}")
    } else if blob_store_url_scheme == "s3" {
        // S3 blob store uses paths relative to its bucket.
        format!(
            "{}://{}/{}",
            blob_store_url_scheme,
            bucket_name_from_s3_url(blob_store_url),
            path
        )
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

/// Convert a full blob URL back to a stored path using the blob store scheme
/// and base URL.
///
/// This is the inverse of [`blob_store_path_to_url`].
pub fn blob_store_url_to_path(
    url: &str,
    blob_store_url_scheme: &str,
    blob_store_url: &str,
) -> String {
    if blob_store_url_scheme == "file" {
        let prefix = format!("{blob_store_url_scheme}:///");
        url.strip_prefix(&prefix)
            // The url may not include the scheme if this payload was uploaded
            // to server instead of directly to blob storage.
            .unwrap_or(url)
            .to_string()
    } else if blob_store_url_scheme == "s3" {
        let prefix = format!(
            "{}://{}/",
            blob_store_url_scheme,
            bucket_name_from_s3_url(blob_store_url)
        );
        url.strip_prefix(&prefix).unwrap_or(url).to_string()
    } else {
        format!("not supported blob store scheme: {blob_store_url_scheme}")
    }
}

/// Extract the bucket name from an S3 blob store URL.
///
/// `s3://my-bucket/prefix` → `"my-bucket"`
fn bucket_name_from_s3_url(blob_store_url: &str) -> String {
    match url::Url::parse(blob_store_url) {
        Ok(url) => match url.host_str() {
            Some(bucket) => bucket.into(),
            None => {
                tracing::error!("Didn't find bucket name in S3 url: {}", blob_store_url);
                String::new()
            }
        },
        Err(e) => {
            tracing::error!(
                "Failed to parse blob_store_url: {}. Error: {}",
                blob_store_url,
                e
            );
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_file_uri() {
        assert!(is_file_uri("file:///tmp/foo"));
        assert!(!is_file_uri("s3://bucket/key"));
    }

    #[test]
    fn test_is_s3_uri() {
        assert!(is_s3_uri("s3://bucket/key"));
        assert!(!is_s3_uri("file:///tmp/foo"));
    }

    #[test]
    fn test_parse_s3_uri() {
        let (bucket, key) = parse_s3_uri("s3://my-bucket/some/key/path.bin").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "some/key/path.bin");
    }

    #[test]
    fn test_parse_s3_uri_no_key() {
        assert!(parse_s3_uri("s3://my-bucket").is_err());
    }

    #[test]
    fn test_parse_s3_uri_not_s3() {
        assert!(parse_s3_uri("file:///tmp/foo").is_err());
    }

    #[test]
    fn test_file_uri_to_path() {
        let path = file_uri_to_path("file:///tmp/some/path.bin").unwrap();
        assert_eq!(path, PathBuf::from("/tmp/some/path.bin"));
    }

    #[test]
    fn test_file_uri_to_path_not_file() {
        assert!(file_uri_to_path("s3://bucket/key").is_err());
    }

    #[test]
    fn test_blob_store_path_to_url_file() {
        let url = blob_store_path_to_url(
            "Users/x/storage/blobs/ns/test.bin",
            "file",
            "file:///Users/x/storage/blobs",
        );
        assert_eq!(url, "file:///Users/x/storage/blobs/ns/test.bin");
    }

    #[test]
    fn test_blob_store_path_to_url_s3() {
        let url = blob_store_path_to_url("ns/test.bin", "s3", "s3://my-bucket");
        assert_eq!(url, "s3://my-bucket/ns/test.bin");
    }

    #[test]
    fn test_blob_store_url_to_path_file() {
        let path = blob_store_url_to_path(
            "file:///Users/x/storage/blobs/ns/test.bin",
            "file",
            "file:///Users/x/storage/blobs",
        );
        assert_eq!(path, "Users/x/storage/blobs/ns/test.bin");
    }

    #[test]
    fn test_blob_store_url_to_path_s3() {
        let path = blob_store_url_to_path("s3://my-bucket/ns/test.bin", "s3", "s3://my-bucket");
        assert_eq!(path, "ns/test.bin");
    }

    #[test]
    fn test_blob_store_url_to_path_no_prefix() {
        // URL doesn't include blob_store_scheme (uploaded to server directly)
        let path = blob_store_url_to_path("ns/test.bin", "file", "file:///Users/x/storage/blobs");
        assert_eq!(path, "ns/test.bin");
    }
}
