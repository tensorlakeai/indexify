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
use serde::{de::DeserializeOwned, Serialize};

use crate::{data_model::clocks::Linearizable, state_store::scanner::CursorDirection};

pub mod rocksdb;
use rocksdb::*;

pub type KVBytes = (Box<[u8]>, Box<[u8]>);

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
}

/// Writer defines all the write operations for a given driver.
pub trait Writer {
    /// Start a new Transaction in the database.
    fn transaction(&self) -> Transaction;

    fn put<N, K, V>(&self, cf: N, key: K, value: V) -> Result<(), Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>;

    fn drop<N>(&mut self, cf: N) -> Result<(), Error>
    where
        N: AsRef<str>;

    fn create<N>(&mut self, cf: N, opts: &CreateOptions) -> Result<(), Error>
    where
        N: AsRef<str>;
}

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
pub trait Reader {
    // Get an item from the database.
    fn get<N, K>(&self, cf: N, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>;

    /// Return the items in the database that match the keys passed as
    /// arguments. If the key does not exist in the database, the value is
    /// not returned, it's just ignored.
    fn list_existent_items<N>(&self, cf: N, keys: Vec<&[u8]>) -> Result<Vec<Bytes>, Error>
    where
        N: AsRef<str>;

    /// Return a list of keys from a given range in an iterator.
    fn get_key_range<N>(&self, cf: N, options: RangeOptions) -> Result<Range, Error>
    where
        N: AsRef<str>;

    /// Iterate over a list of Key/Value pairs.
    fn iter<N>(&self, cf: N, options: IterOptions) -> impl Iterator<Item = Result<KVBytes, Error>>
    where
        N: AsRef<str>;
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
pub struct RangeOptions<'db> {
    #[builder(default)]
    cursor: Option<&'db [u8]>,
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
pub enum IterOptions<'db> {
    RocksDB((rocksdb::ReadOptions, Option<rocksdb::IteratorMode<'db>>)),
}

impl<'db> IterOptions<'db> {
    /// 4MB
    pub const DEFAULT_BLOCK_SIZE: usize = 4_194_304;
    /// 10MB
    pub const LARGE_BLOCK_SIZE: usize = 10_194_304;

    pub fn new_rocksdb_options() -> Self {
        let mut read_options = rocksdb::ReadOptions::default();
        read_options.set_readahead_size(Self::DEFAULT_BLOCK_SIZE);

        IterOptions::RocksDB((read_options, None))
    }

    pub fn with_block_size(self, block_size: usize) -> Self {
        let Self::RocksDB(this) = self;

        let mut read_options = this.0;
        read_options.set_readahead_size(block_size);

        IterOptions::RocksDB((read_options, this.1))
    }

    pub fn starting_at(self, token: &'db [u8]) -> Self {
        let mode = IteratorMode::From(token, Direction::Forward);
        self.with_mode(mode)
    }

    fn with_mode(self, mode: rocksdb::IteratorMode<'db>) -> Self {
        let Self::RocksDB(this) = self;
        IterOptions::RocksDB((this.0, Some(mode)))
    }

    #[cfg(test)]
    pub fn scan_fully(self) -> Self {
        let Self::RocksDB(this) = self;

        let mut read_options = this.0;
        read_options.set_total_order_seek(true);

        IterOptions::RocksDB((read_options, this.1))
    }
}

impl<'db> Default for IterOptions<'db> {
    fn default() -> Self {
        Self::new_rocksdb_options()
    }
}

/// AtomicComparator defines atomic functions that compare
/// incoming records with existent records in the database.
///
/// These comparisons often use `crate::data_model::clocks::VectorClock`
/// and `crate::data_model::clocks::Linearizable` structs to check
/// the "happens-before" relationship between two records.
pub trait AtomicComparator {
    /// Compare a persisted record's vector clock against
    /// the vector clock from an incoming new record, and commit
    /// the new record if the clocks match.
    ///
    /// This function returns an error if the clock in the persisted
    /// record is higher than the incoming one. This means that
    /// a different event updated the record before, and the client
    /// needs to reconcile the changes before trying to write its
    /// changes.
    #[allow(dead_code)]
    fn compare_and_swap<N, K, R>(
        &self,
        tx: Arc<Transaction>,
        cf: N,
        key: K,
        record: R,
    ) -> Result<(), Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        R: Linearizable + Serialize + DeserializeOwned + fmt::Debug;
}

/// Driver defines all the operations a database driver needs to support.
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
pub fn open_database(options: ConnectionOptions) -> Result<RocksDBDriver, Error> {
    match options {
        ConnectionOptions::RocksDB(options) => {
            rocksdb::RocksDBDriver::open(options).map_err(Into::into)
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
pub enum Transaction<'db> {
    RocksDB(RocksDBTransaction<'db>),
}

impl fmt::Debug for Transaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transaction::RocksDB(_) => write!(f, "Transaction::RocksDB"),
        }
    }
}

impl<'db> Transaction<'db> {
    pub fn commit(self) -> Result<(), Error> {
        let Self::RocksDB(tx) = self;
        tx.commit()
    }

    pub fn get<N, K>(&self, cf: N, key: K) -> Result<Option<Vec<u8>>, Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let Self::RocksDB(tx) = self;
        tx.get(cf, key)
    }

    pub fn put<N, K, V>(&self, cf: N, key: K, value: V) -> Result<(), Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let Self::RocksDB(tx) = self;
        tx.put(cf, key, value)
    }

    pub fn delete<N, K>(&self, cf: N, key: K) -> Result<(), Error>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let Self::RocksDB(tx) = self;
        tx.delete(cf, key)
    }

    pub fn iter<N>(
        &'db self,
        cf: N,
        prefix: &'db [u8],
        options: IterOptions,
    ) -> impl Iterator<Item = Result<KVBytes, Error>> + 'db
    where
        N: AsRef<str>,
    {
        let Self::RocksDB(tx) = self;
        tx.iter(cf, prefix, options)
    }
}
