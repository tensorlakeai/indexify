use std::{fmt, path::PathBuf, sync::Arc};

use bytes::Bytes;
use rocksdb::{
    AsColumnFamilyRef,
    ColumnFamily,
    ColumnFamilyDescriptor,
    Error as RocksDBError,
    Options as RocksDBOptions,
    Transaction,
    TransactionDB,
    TransactionDBOptions,
};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{error, warn};

use crate::{
    data_model::clocks::Linearizable,
    state_store::{
        driver::{AtomicComparator, Driver, Error as DriverError, Reader, Writer},
        serializer::{JsonEncode, JsonEncoder},
    },
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
    // the RocksDB's connection directly until all the operations can be
    // put behind an interface. That will keep the codebase
    // backwards compatible with what we have now.
    pub db: TransactionDB,
}

impl RocksDBDriver {
    /// Open a new connection with a RocksDB database.
    pub(crate) fn open(driver_options: Options) -> Result<RocksDBDriver, Error> {
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

        Ok(RocksDBDriver { db })
    }
}

impl RocksDBDriver {
    /// Fetch a column family from RocksDB.
    ///
    /// If the name does not exist in the database,
    /// this function panics because it's an irrecoverable error.
    pub fn column_family(&self, name: &str) -> &ColumnFamily {
        let Some(handle) = self.db.cf_handle(name) else {
            panic!("failed to get column family handle for {name}");
        };

        handle
    }

    pub fn get_cf<K>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
    ) -> Result<Option<Vec<u8>>, RocksDBError>
    where
        K: AsRef<[u8]>,
    {
        self.db.get_cf(cf, key)
    }

    pub fn put_cf<K, V>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        value: V,
    ) -> Result<(), RocksDBError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.put_cf(cf, key, value)
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), RocksDBError> {
        self.db.drop_cf(name)
    }

    pub fn create_cf<N>(&mut self, name: N, opts: &RocksDBOptions) -> Result<(), RocksDBError>
    where
        N: AsRef<str>,
    {
        self.db.create_cf(name, opts)
    }
}

impl Writer for RocksDBDriver {
    fn start_transaction(&self) -> Result<super::Transaction, DriverError> {
        let tx = self.db.transaction();

        Ok(super::Transaction::RocksDB(RocksDBTransaction {
            db: self,
            tx,
        }))
    }
}

impl AtomicComparator for RocksDBDriver {
    fn compare_and_swap<R>(
        &self,
        tx: Arc<super::Transaction>,
        table: &str,
        key: &str,
        new_record: R,
    ) -> Result<(), DriverError>
    where
        R: Linearizable + Serialize + DeserializeOwned + fmt::Debug,
    {
        let tx = unwrap_rocksdb_transaction(&tx);
        let cf = tx.column_family(table);

        let existing_record = tx.get_for_update_cf(cf, key)?;

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

        tx.put_cf(cf, key, &new_record)
    }
}

impl Reader for RocksDBDriver {
    fn list_existent_items(
        &self,
        column: &str,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<Bytes>, DriverError> {
        let cf_handle =
            self.db
                .cf_handle(column)
                .ok_or_else(|| Error::ColumnFamilyLocationFailed {
                    name: column.to_string(),
                })?;

        let mut items = Vec::with_capacity(keys.len());
        let multi_get_keys: Vec<_> = keys.iter().map(|k| (&cf_handle, k.to_vec())).collect();
        let values = self.db.multi_get_cf(multi_get_keys);

        for (key, value) in keys.into_iter().zip(values.into_iter()) {
            match value.map_err(|source| Error::GenericRocksDBFailure { source })? {
                Some(v) => items.push(v.into()),
                None => {
                    warn!(
                        "Key not found: {}",
                        String::from_utf8(key.to_vec()).unwrap_or_default()
                    );
                }
            }
        }
        Ok(items)
    }
}

impl Driver for RocksDBDriver {}

#[allow(irrefutable_let_patterns)]
/// Ensure that the transaction we're using has been generated by the RocksDB
/// driver. Using a transaction from another driver is an irrecoverable failure
/// and we should crash the server.
fn unwrap_rocksdb_transaction<'db>(tx: &'db super::Transaction) -> &'db RocksDBTransaction<'db> {
    let super::Transaction::RocksDB(tx) = tx else {
        panic!("tried to unwrap a RocksDBTransaction from a Transaction that was not created by the RocksDB driver: {tx:?}");
    };
    tx
}

#[allow(dead_code)]
pub(crate) struct RocksDBTransaction<'a> {
    db: &'a RocksDBDriver,
    tx: Transaction<'a, TransactionDB>,
}

impl<'a> RocksDBTransaction<'a> {
    /// Get the column family for a key from RocksDB.
    ///
    /// TODO(David): Figure out how expensive is to call this operation.
    /// If it's too expensive, we should probably have column families
    /// pre-loaded, or cached in memory.
    #[allow(dead_code)]
    fn column_family(&self, name: &str) -> &ColumnFamily {
        self.db.column_family(name)
    }

    #[allow(dead_code)]
    fn commit(self) -> Result<(), DriverError> {
        self.tx
            .commit()
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    #[allow(dead_code)]
    fn rollback(&self) -> Result<(), DriverError> {
        self.tx
            .rollback()
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    #[allow(dead_code)]
    fn get_for_update(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, DriverError> {
        let cf = self.column_family(table);
        self.get_for_update_cf(cf, key)
    }

    fn get_for_update_cf(
        &self,
        cf: &ColumnFamily,
        key: &str,
    ) -> Result<Option<Vec<u8>>, DriverError> {
        self.tx
            .get_for_update_cf(cf, key, true)
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }

    #[allow(dead_code)]
    fn put(&self, table: &str, key: &str, value: &[u8]) -> Result<(), DriverError> {
        let cf = self.column_family(table);
        self.put_cf(cf, key, value)
    }

    fn put_cf(&self, cf: &ColumnFamily, key: &str, value: &[u8]) -> Result<(), DriverError> {
        self.tx
            .put_cf(cf, key, value)
            .map_err(|source| Error::GenericRocksDBFailure { source }.into())
    }
}
