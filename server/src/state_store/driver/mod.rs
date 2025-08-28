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

use serde::{de::DeserializeOwned, Serialize};

use crate::data_model::clocks::Linearizable;

pub mod rocksdb;
use rocksdb::RocksDBDriver;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "Failed to store record, the records didn't match the vector clock. table: {}, field: {}",
        table,
        field
    )]
    MismatchedClock { table: String, field: String },

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
    #[allow(dead_code)]
    fn start_transaction(&self) -> Result<Arc<dyn Transaction + '_>, Error>;
}

/// Reader defines all the read operations for a give driver.
pub trait Reader {}

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
    fn compare_and_swap<R>(
        &self,
        tx: Arc<dyn Transaction>,
        table: &str,
        key: &str,
        record: R,
    ) -> Result<(), Error>
    where
        R: Linearizable + Serialize + DeserializeOwned + fmt::Debug;
}

/// Transaction is a wrapper around specific database transactions.
/// Since different databases have different transaction semantics,
/// this trait allow us to hide those semantics from the caller's
/// point of view.
pub trait Transaction {
    #[allow(dead_code)]
    fn commit(self) -> Result<(), Error>;
    #[allow(dead_code)]
    fn rollback(&self) -> Result<(), Error>;

    fn get_for_update(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, Error>;
    fn put(&self, table: &str, key: &str, record: &[u8]) -> Result<(), Error>;
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
pub fn open_database(options: ConnectionOptions) -> Result<Arc<RocksDBDriver>, Error> {
    match options {
        ConnectionOptions::RocksDB(options) => {
            rocksdb::RocksDBDriver::open(options).map_err(Into::into)
        }
    }
}
