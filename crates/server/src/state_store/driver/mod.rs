//! The driver module centralizes the logic for
//! reading and writing data in the state store.
//!
//! It defines a series of traits that all DB drivers
//! must implement to be a compliant state store.
//!
//! It also executes snapshot tests across different
//! drivers to ensure that behaviors across drivers
//! stay consistent.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use derive_builder::Builder;

use crate::{metrics::StateStoreMetrics, state_store::scanner::CursorDirection};

pub mod rocksdb;
use rocksdb::*;

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

    #[error(
        "A request with the same id already exists. namespace: {namespace}, application: {application}, request_id: {request_id}"
    )]
    RequestAlreadyExists {
        namespace: String,
        application: String,
        request_id: String,
    },

    #[error(transparent)]
    RocksDBFailure {
        #[from]
        source: rocksdb::Error,
    },
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

    /// Identifies failed operations that are caused by a request ID already
    /// existing.
    pub fn is_request_already_exists(&self) -> bool {
        matches!(&self, Self::RequestAlreadyExists { .. })
    }
}

/// Writer defines all the write operations for a given driver.
#[async_trait]
#[allow(dead_code)]
pub trait Writer {
    /// Start a new Transaction in the database.
    fn transaction(&self) -> Transaction;

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error>;

    async fn drop(&mut self, cf: &str) -> Result<(), Error>;

    async fn create(&mut self, cf: &str, opts: &CreateOptions) -> Result<(), Error>;
}

#[allow(dead_code)]
pub enum CreateOptions {
    RocksDB(rocksdb::RocksDBOptions),
}

impl CreateOptions {
    pub fn new_rocksdb_options() -> Self {
        Self::RocksDB(Default::default())
    }
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self::new_rocksdb_options()
    }
}

/// Reader defines all the read operations for a give driver.
#[async_trait]
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
    ) -> Box<dyn Iterator<Item = Result<KVBytes, Error>> + '_>;
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
    cursor: Option<Vec<u8>>,
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
    RocksDB(
        (
            rocksdb::ReadOptions,
            (Option<Vec<u8>>, Option<rocksdb::Direction>),
        ),
    ),
}

impl Default for IterOptions {
    fn default() -> Self {
        IterOptions::new_rocksdb_options()
    }
}

impl IterOptions {
    /// 4MB
    pub const DEFAULT_BLOCK_SIZE: usize = 4_194_304;
    /// 10MB
    pub const LARGE_BLOCK_SIZE: usize = 10_194_304;

    pub fn new_rocksdb_options() -> Self {
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.set_readahead_size(Self::DEFAULT_BLOCK_SIZE);

        IterOptions::RocksDB((read_options, (None, None)))
    }

    pub fn with_block_size(self, block_size: usize) -> Self {
        let Self::RocksDB(this) = self;

        let mut read_options = this.0;
        read_options.set_readahead_size(block_size);

        IterOptions::RocksDB((read_options, this.1))
    }

    pub fn starting_at(self, token: Vec<u8>) -> Self {
        let mode = (Some(token), Some(Direction::Forward));
        let Self::RocksDB(this) = self;
        IterOptions::RocksDB((this.0, mode))
    }

    #[cfg(test)]
    pub fn scan_fully(self) -> Self {
        let Self::RocksDB(this) = self;

        let mut read_options = this.0;
        read_options.set_total_order_seek(true);

        IterOptions::RocksDB((read_options, this.1))
    }
}

/// It combines Writer + Reader to make implementing a driver more ergonomic.
#[allow(dead_code)]
pub trait Driver: Writer + Reader {}

/// Multiple options to configure different database drivers.
///
/// The only option at the moment is RocksDB
#[non_exhaustive]
pub enum ConnectionOptions {
    RocksDB(rocksdb::Options),
}

/// Open a connection to a database.
///
/// This is the main entry point to connect Indexify server with a database to
/// keep the state.
///
/// It returns a `RocksDBDriver` at the moment because there is no other option
/// supported. This helps keep the code backward compatible.
pub fn open_database(
    options: ConnectionOptions,
    metrics: Arc<StateStoreMetrics>,
) -> Result<RocksDBDriver, Error> {
    match options {
        ConnectionOptions::RocksDB(options) => {
            rocksdb::RocksDBDriver::open(options, metrics).map_err(Into::into)
        }
    }
}

#[async_trait]
pub trait InnerTransaction: Send + Sync {
    async fn commit(&self) -> Result<(), Error>;

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), Error>;
    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), Error>;
    async fn delete_range(&self, cf: &str, start_key: &[u8], end_key: &[u8]) -> Result<(), Error>;

    async fn iter(&self, cf: &str, prefix: Vec<u8>) -> Vec<Result<KVBytes, Error>>;
}

pub type Transaction = std::sync::Arc<dyn InnerTransaction>;
