//! Presigned URL structures for direct client access.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A presigned URL for GET or PUT operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresignedUrl {
    /// The presigned URL.
    pub url: String,

    /// Expiration duration from creation time.
    pub expires_in: Duration,

    /// HTTP method (GET or PUT).
    pub method: HttpMethod,
}

/// HTTP method for presigned requests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Put,
}

/// A presigned request including headers.
#[derive(Debug, Clone)]
pub struct PresignedRequest {
    /// The URL.
    pub url: String,

    /// Required headers (e.g., for S3 multipart uploads).
    pub headers: Vec<(String, String)>,

    /// HTTP method.
    pub method: HttpMethod,
}

impl PresignedUrl {
    /// Create a presigned GET URL.
    pub fn get(url: String, expires_in: Duration) -> Self {
        Self {
            url,
            expires_in,
            method: HttpMethod::Get,
        }
    }

    /// Create a presigned PUT URL.
    pub fn put(url: String, expires_in: Duration) -> Self {
        Self {
            url,
            expires_in,
            method: HttpMethod::Put,
        }
    }
}

/// Maximum presigned URL expiry (7 days for S3).
pub const MAX_PRESIGN_EXPIRY: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Validate presigned URL expiry duration.
pub fn validate_expiry(expires_in: Duration) -> Result<(), String> {
    if expires_in > MAX_PRESIGN_EXPIRY {
        Err(format!(
            "Expiry duration {:?} exceeds maximum allowed {:?}",
            expires_in, MAX_PRESIGN_EXPIRY
        ))
    } else if expires_in.is_zero() {
        Err("Expiry duration must be greater than zero".to_string())
    } else {
        Ok(())
    }
}
