use std::fmt::{self, Debug};

use flexbuffers::{DeserializationError, ReaderError};
use redis::RedisError;

#[derive(Debug)]
pub enum IndexifyCachingError {
    Serialization(String),
    Deserialization(String),

    Io(std::io::Error),
    Redis(RedisError),
}

impl fmt::Display for IndexifyCachingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IndexifyCachingError::Serialization(msg) => {
                write!(
                    f,
                    "Indexify Cache was unable to serialize an object: {}",
                    msg
                )
            }
            IndexifyCachingError::Deserialization(msg) => {
                write!(
                    f,
                    "Indexify Cache was unable to deserialize an object: {}",
                    msg
                )
            }
            IndexifyCachingError::Io(e) => {
                write!(f, "Indexify Cache encountered an IO error in communicating with the caching layer: {}", e)
            }
            IndexifyCachingError::Redis(e) => {
                write!(f, "Indexify Cache encountered an error in communicating with the Redis caching layer: {}", e)
            }
        }
    }
}

impl std::error::Error for IndexifyCachingError {}

impl From<RedisError> for IndexifyCachingError {
    fn from(e: redis::RedisError) -> Self {
        match e.kind() {
            redis::ErrorKind::IoError => IndexifyCachingError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Redis IO Error: {}", e),
            )),
            _ => IndexifyCachingError::Redis(e),
        }
    }
}

impl From<serde::de::value::Error> for IndexifyCachingError {
    fn from(e: serde::de::value::Error) -> Self {
        IndexifyCachingError::Deserialization(e.to_string())
    }
}

impl From<DeserializationError> for IndexifyCachingError {
    fn from(e: DeserializationError) -> Self {
        IndexifyCachingError::Deserialization(e.to_string())
    }
}

impl From<ReaderError> for IndexifyCachingError {
    fn from(e: ReaderError) -> Self {
        IndexifyCachingError::Deserialization(e.to_string())
    }
}

// Tests for error formatting
#[cfg(test)]
mod tests {
    use super::IndexifyCachingError;

    #[test]
    fn test_indexify_caching_error_display() {
        let err = IndexifyCachingError::Serialization("test".to_string());
        assert_eq!(
            err.to_string(),
            "Indexify Cache was unable to serialize an object: test"
        );
        let err = IndexifyCachingError::Deserialization("test".to_string());
        assert_eq!(
            err.to_string(),
            "Indexify Cache was unable to deserialize an object: test"
        );
        let err = IndexifyCachingError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        assert_eq!(
            err.to_string(),
            "Indexify Cache encountered an IO error in communicating with the caching layer: test"
        );
        let redis_error =
            redis::RedisError::from(std::io::Error::new(std::io::ErrorKind::Other, "test"));
        let err = IndexifyCachingError::from(redis_error);
        assert_eq!(err.to_string(), "Indexify Cache encountered an IO error in communicating with the caching layer: Redis IO Error: test");
    }
}
