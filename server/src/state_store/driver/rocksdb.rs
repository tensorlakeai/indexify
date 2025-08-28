use std::{fmt, path::PathBuf, sync::Arc};

use rocksdb::{
    ColumnFamily,
    ColumnFamilyDescriptor,
    Error as RocksDBError,
    Options as RocksDBOptions,
    Transaction,
    TransactionDB,
    TransactionDBOptions,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::error;

use crate::{
    data_model::clocks::Linearizable,
    state_store::{
        driver::{AtomicComparator, Driver, Error as DriverError, Reader, Writer},
        serializer::{JsonEncode, JsonEncoder},
    },
    utils::OptionInspectNone,
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to open RocksDB database. error: {}", source)]
    OpenDatabaseFailed { source: RocksDBError },

    #[error("Failed to load column handle. table: {}", name)]
    ColumnFamilyLocationFailed { name: String },

    #[error(transparent)]
    GenericRocksDBFailure { source: RocksDBError },
}

/// Options to start a connection with RocksDB.
pub(crate) struct Options {
    pub path: PathBuf,
    pub column_families: Vec<ColumnFamilyDescriptor>,
}

/// Driver to connect with a RocksDB database.
pub(crate) struct RocksDBDriver {
    // This field is public, so Indexify server can use keep using
    // RocksDB's connection directly until all the operations can be
    // put behind an interface. That will keep the codebase
    // backwards compatible with what we have now.
    pub db: Arc<TransactionDB>,
}

impl RocksDBDriver {
    /// Open a new connection with a RocksDB database.
    pub(crate) fn open(driver_options: Options) -> Result<Arc<RocksDBDriver>, Error> {
        let mut db_opts = RocksDBOptions::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            &driver_options.path,
            driver_options.column_families,
        )
        .map_err(|source| Error::OpenDatabaseFailed { source })?;

        Ok(Arc::new(RocksDBDriver { db: Arc::new(db) }))
    }
}

impl Writer for RocksDBDriver {
    #[allow(clippy::arc_with_non_send_sync)]
    fn start_transaction(&self) -> Result<Arc<dyn super::Transaction + '_>, DriverError> {
        let tx = self.db.transaction();

        Ok(Arc::new(RocksDBTransaction {
            db: self.db.clone(),
            tx,
        }))
    }
}

impl AtomicComparator for RocksDBDriver {
    fn compare_and_swap<R>(
        &self,
        tx: Arc<dyn super::Transaction>,
        table: &str,
        key: &str,
        new_record: R,
    ) -> Result<(), DriverError>
    where
        R: Linearizable + Serialize + DeserializeOwned + fmt::Debug,
    {
        let existing_record = tx.get_for_update(table, key)?;

        if let Some(record) = existing_record {
            let old_record: R = JsonEncoder::decode(&record)
                .map_err(|source| DriverError::JsonDecoderFailed { source })?;

            if old_record.vector_clock() > new_record.vector_clock() {
                return Err(DriverError::MismatchedClock {
                    table: table.to_string(),
                    field: key.to_string(),
                });
            }
        }

        let new_record = JsonEncoder::encode(&new_record)
            .map_err(|source| DriverError::JsonEncoderFailed { source })?;

        tx.put(table, key, &new_record)
    }
}

impl Reader for RocksDBDriver {}
impl Driver for RocksDBDriver {}

#[allow(dead_code)]
pub(crate) struct RocksDBTransaction<'a> {
    db: Arc<TransactionDB>,
    tx: Transaction<'a, TransactionDB>,
}

impl<'a> super::Transaction for RocksDBTransaction<'a> {
    fn commit(self) -> Result<(), DriverError> {
        self.tx
            .commit()
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    fn rollback(&self) -> Result<(), DriverError> {
        self.tx
            .rollback()
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    fn get_for_update(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, DriverError> {
        let cf = self.column_family(table)?;
        self.tx
            .get_for_update_cf(cf, key, true)
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    fn put(&self, table: &str, key: &str, value: &[u8]) -> Result<(), DriverError> {
        let cf = self.column_family(table)?;
        self.tx
            .put_cf(cf, key, value)
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }
}

impl<'a> RocksDBTransaction<'a> {
    /// Get the column family for a key from RocksDB.
    ///
    /// TODO(David): Figure out how expensive is to call this operation.
    /// If it's too expensive, we should probably have column families
    /// pre-loaded, or cached in memory.
    #[allow(dead_code)]
    fn column_family(&self, name: &str) -> Result<&ColumnFamily, DriverError> {
        self.db
            .cf_handle(name)
            .inspect_none(|| {
                error!("failed to get column family handle for {}", name);
            })
            .ok_or_else(|| {
                Error::ColumnFamilyLocationFailed {
                    name: name.to_string(),
                }
                .into()
            })
    }
}
