//! The driver module centralizes the logic for
//! reading and writing data in the state store.
//!
//! It defines a series of traits that all DB drivers
//! must implement to be a compliant state store.
//!
//! It also executes snapshot tests across different
//! drivers to ensure that behaviors across drivers
//! stay consistent.

use std::{fmt, sync::Arc};

use bytes::Bytes;
use derive_builder::Builder;

use crate::{metrics::StateStoreMetrics, state_store::scanner::CursorDirection};

pub mod foundationdb;
use foundationdb::*;

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(dead_code)]
pub enum Error {
    #[error(
        "Failed to store record, the records didn't match the vector clock. table: {}, key: {}",
        table,
        key
    )]
    MismatchedClock { table: String, key: String },

    #[error("Failed to decode a serialized record. error: {}", source)]
    JsonDecoderFailed { source: anyhow::Error },

    #[error("Failed to encode a new serialized record. error: {}", source)]
    JsonEncoderFailed { source: anyhow::Error },

    #[error(transparent)]
    FoundationDBFailure {
        #[from]
        source: foundationdb::Error,
    },

    #[error("Generic driver error: {}", message)]
    GenericFailure { message: String },
}

impl Error {
    /// Identifies failed operations that can be
    /// retried.
    ///
    /// At the moment, only `Self::MismatchedClock` errors
    /// can be retried.
    #[allow(dead_code)]
    pub fn is_retryable(&self) -> bool {
        matches!(&self, Self::MismatchedClock { .. })
    }

    /// Identifies failed operations that MUST not
    /// be retried.
    #[allow(dead_code)]
    pub fn is_permanent(&self) -> bool {
        !self.is_retryable()
    }
}

/// Writer defines all the write operations for a given driver.
#[allow(dead_code)]
#[async_trait::async_trait]
pub trait Writer {
    /// Start a new Transaction in the database.
    fn transaction(&self) -> Transaction;

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error>;

    fn drop(&mut self, cf: &str) -> Result<(), Error>;

    fn create(&mut self, cf: &str, opts: &CreateOptions) -> Result<(), Error>;
}

#[allow(dead_code)]
pub enum CreateOptions {
    FoundationDB(foundationdb::Options),
}

impl CreateOptions {
    pub fn new_foundationdb_options() -> Self {
        Self::FoundationDB(Default::default())
    }
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self::new_foundationdb_options()
    }
}

/// Reader defines all the read operations for a give driver.
#[async_trait::async_trait]
pub trait Reader {
    // Get an item from the database.
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Return the items in the database that match the keys passed as
    /// arguments. If the key does not exist in the database, the value is
    /// not returned, it's just ignored.
    async fn list_existent_items(&self, cf: &str, keys: Vec<&[u8]>) -> Result<Vec<Bytes>, Error>;

    /// Return a list of keys from a given range in an iterator.
    async fn get_key_range(&self, cf: &str, options: RangeOptions) -> Result<Range, Error>;

    /// Iterate over a list of Key/Value pairs.
    async fn iter(
        &self,
        cf: &str,
        options: IterOptions,
    ) -> Box<dyn Iterator<Item = Result<KVBytes, Error>> + Send + '_>;
}

/// Struct that holds the information returned by `Reader::get_key_range`.
#[derive(Builder, Clone, Debug)]
pub struct Range {
    pub items: Vec<Bytes>,
    pub direction: CursorDirection,
    pub prev_cursor: Option<Bytes>,
    pub next_cursor: Option<Bytes>,
}

/// Options that you can provide to perform a key range search.
#[derive(Builder, Clone, Debug, Default)]
pub struct RangeOptions {
    #[builder(default)]
    cursor: Option<Bytes>,
    #[builder(setter(strip_option), default)]
    upper_bound: Option<Bytes>,
    #[builder(setter(strip_option), default)]
    lower_bound: Option<Bytes>,
    #[builder(default)]
    direction: Option<CursorDirection>,
    #[builder(default = "100")]
    limit: usize,
}

/// Options that you can provide to iterate over a Key/Value pair.
pub enum IterOptions {
    FoundationDB(()), // FoundationDB doesn't need special iteration options
}

impl IterOptions {
    /// 4MB
    pub const DEFAULT_BLOCK_SIZE: usize = 4_194_304;
    /// 10MB
    pub const LARGE_BLOCK_SIZE: usize = 10_194_304;

    pub fn new_foundationdb_options() -> Self {
        IterOptions::FoundationDB(())
    }

    pub fn with_block_size(self, block_size: usize) -> Self {
        self // FoundationDB doesn't use block size
    }

    pub fn starting_at(self, _token: &[u8]) -> Self {
        self // FoundationDB doesn't use starting tokens in the same way
    }

    #[cfg(test)]
    pub fn scan_fully(self) -> Self {
        self // FoundationDB doesn't need special scan options
    }
}

impl Default for IterOptions {
    fn default() -> Self {
        Self::new_foundationdb_options()
    }
}

/// Driver defines all the operations a database driver needs to support.
/// It combines Writer + Reader to make implementing a driver more ergonomic.
#[expect(unused)]
pub trait Driver: Writer + Reader {}

/// Enum to hold different driver implementations
#[allow(dead_code)]
pub enum DriverEnum {
    FoundationDB(FoundationDBDriver),
}

impl DriverEnum {
    pub fn transaction(&self) -> Transaction {
        match self {
            DriverEnum::FoundationDB(driver) => driver.transaction(),
        }
    }

    pub async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match self {
            DriverEnum::FoundationDB(driver) => driver.put(cf, key, value).await,
        }
    }

    pub async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self {
            DriverEnum::FoundationDB(driver) => driver.get(cf, key).await,
        }
    }

    pub async fn list_existent_items(
        &self,
        cf: &str,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<Bytes>, Error> {
        match self {
            DriverEnum::FoundationDB(driver) => driver.list_existent_items(cf, keys).await,
        }
    }

    pub async fn get_key_range(&self, cf: &str, options: RangeOptions) -> Result<Range, Error> {
        match self {
            DriverEnum::FoundationDB(driver) => driver.get_key_range(cf, options).await,
        }
    }

    pub async fn iter(
        &self,
        cf: &str,
        options: IterOptions,
    ) -> Box<dyn Iterator<Item = Result<KVBytes, Error>> + Send + '_> {
        match self {
            DriverEnum::FoundationDB(driver) => driver.iter(cf, options).await,
        }
    }
}

/// Multiple options to configure different database drivers.
#[non_exhaustive]
pub enum ConnectionOptions {
    FoundationDB(foundationdb::Options),
}

/// Open a connection to a database.
///
/// This is the main entry point to connect Indexify server with a database to
/// keep the state.
pub fn open_database(
    options: ConnectionOptions,
    metrics: Arc<StateStoreMetrics>,
) -> Result<DriverEnum, Error> {
    match options {
        ConnectionOptions::FoundationDB(options) => {
            foundationdb::FoundationDBDriver::open(options, metrics)
                .map(DriverEnum::FoundationDB)
                .map_err(Into::into)
        }
    }
}

/// Transaction is a wrapper around specific database transactions.
/// Since different databases have different transaction semantics,
/// this enum allow us to hide those semantics from the caller's
/// point of view.
///
/// We use an enum instead of a trait because it easier to validate
/// that the inner transaction uses the right driver.
pub enum Transaction {
    FoundationDB(FoundationDBTransaction),
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transaction::FoundationDB(_) => write!(f, "Transaction::FoundationDB"),
        }
    }
}

impl Transaction {
    pub async fn commit(self) -> Result<(), Error> {
        match self {
            Self::FoundationDB(tx) => tx.commit().await,
        }
    }

    pub async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self {
            Self::FoundationDB(tx) => tx.get(cf, key).await,
        }
    }

    pub async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match self {
            Self::FoundationDB(tx) => tx.put(cf, key, value).await,
        }
    }

    pub async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), Error> {
        match self {
            Self::FoundationDB(tx) => tx.delete(cf, key).await,
        }
    }

    pub async fn iter(
        &self,
        cf: &str,
        prefix: &[u8],
        options: IterOptions,
    ) -> Box<dyn Iterator<Item = Result<KVBytes, Error>> + Send + '_> {
        match self {
            Self::FoundationDB(tx) => tx.iter(cf, prefix, options).await,
        }
    }
}
