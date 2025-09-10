use std::{fmt, path::PathBuf, sync::Arc};

use bytes::Bytes;
use opentelemetry::KeyValue;
use rocksdb::{
    ColumnFamily,
    ColumnFamilyDescriptor,
    Error as RocksDBError,
    Transaction,
    TransactionDB,
    TransactionDBOptions,
};
pub use rocksdb::{Direction, IteratorMode, Options as RocksDBOptions, ReadOptions};
use serde::{de::DeserializeOwned, Serialize};
use tracing::{error, warn};

use crate::{
    data_model::clocks::Linearizable,
    metrics::{Increment, StateStoreMetrics},
    state_store::{
        driver::{
            AtomicComparator,
            Driver,
            Error as DriverError,
            IterOptions,
            Range,
            RangeOptions,
            Reader,
            Writer,
        },
        scanner::CursorDirection,
        serializer::{JsonEncode, JsonEncoder},
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to open RocksDB database. error: {}", source)]
    OpenDatabaseFailed { source: RocksDBError },

    #[error(transparent)]
    GenericRocksDBFailure { source: RocksDBError },
}

impl Error {
    fn into_generic(source: RocksDBError) -> DriverError {
        Self::GenericRocksDBFailure { source }.into()
    }
}

/// Options to start a connection with RocksDB.
pub(crate) struct Options {
    pub path: PathBuf,
    pub column_families: Vec<ColumnFamilyDescriptor>,
}

/// Driver to connect with a RocksDB database.
pub(crate) struct RocksDBDriver {
    db: TransactionDB,
    metrics: Arc<StateStoreMetrics>,
}

impl RocksDBDriver {
    /// Open a new connection with a RocksDB database.
    pub(crate) fn open(
        driver_options: Options,
        metrics: Arc<StateStoreMetrics>,
    ) -> Result<RocksDBDriver, Error> {
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

        Ok(RocksDBDriver { db, metrics })
    }
}

impl RocksDBDriver {
    fn column_family<N>(&self, name: N) -> &ColumnFamily
    where
        N: AsRef<str>,
    {
        let Some(handle) = self.db.cf_handle(name.as_ref()) else {
            panic!("failed to get column family handle for {}", name.as_ref());
        };

        handle
    }
}

impl Writer for RocksDBDriver {
    fn transaction(&self) -> super::Transaction<'_> {
        let tx = self.db.transaction();

        super::Transaction::RocksDB(RocksDBTransaction { db: self, tx })
    }

    fn put<N, K, V>(&self, cf: N, key: K, value: V) -> Result<(), DriverError>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_writes, attrs);

        let cf = self.column_family(cf);
        self.db.put_cf(cf, key, value).map_err(Error::into_generic)
    }

    fn drop<N>(&mut self, cf: N) -> Result<(), DriverError>
    where
        N: AsRef<str>,
    {
        self.db.drop_cf(cf.as_ref()).map_err(Error::into_generic)
    }

    fn create<N>(&mut self, name: N, opts: &super::CreateOptions) -> Result<(), DriverError>
    where
        N: AsRef<str>,
    {
        let super::CreateOptions::RocksDB(opts) = opts;
        self.db.create_cf(name, opts).map_err(Error::into_generic)
    }
}

impl AtomicComparator for RocksDBDriver {
    fn compare_and_swap<N, K, R>(
        &self,
        tx: Arc<super::Transaction>,
        cf: N,
        key: K,
        new_record: R,
    ) -> Result<(), DriverError>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        R: Linearizable + Serialize + DeserializeOwned + fmt::Debug,
    {
        let name = cf.as_ref();
        let key = key.as_ref();

        let tx = unwrap_rocksdb_transaction(&tx);
        let cf = tx.column_family(name);

        let existing_record = tx.get_for_update_cf(cf, key)?;

        if let Some(record) = existing_record {
            let old_record: R = JsonEncoder::decode(&record)
                .map_err(|source| DriverError::JsonDecoderFailed { source })?;

            if old_record.vector_clock() > new_record.vector_clock() {
                return Err(DriverError::MismatchedClock {
                    table: name.to_string(),
                    key: String::from_utf8_lossy(key).to_string(),
                });
            }
        }

        let new_record = JsonEncoder::encode(&new_record)
            .map_err(|source| DriverError::JsonEncoderFailed { source })?;

        tx.put_cf(cf, key, &new_record)
    }
}

impl Reader for RocksDBDriver {
    fn get<N, K>(&self, cf: N, key: K) -> Result<Option<Vec<u8>>, DriverError>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_reads, attrs);

        let cf = self.column_family(cf);
        self.db.get_cf(cf, key).map_err(Error::into_generic)
    }

    fn list_existent_items<N>(&self, cf: N, keys: Vec<&[u8]>) -> Result<Vec<Bytes>, DriverError>
    where
        N: AsRef<str>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let cf_handle = self.column_family(cf.as_ref());

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

    fn get_key_range<N>(&self, cf: N, options: RangeOptions) -> Result<Range, DriverError>
    where
        N: AsRef<str>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let direction = options.direction.unwrap_or_default();

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(10_194_304);
        if let Some(upper_bound) = options.upper_bound {
            read_options.set_iterate_upper_bound(upper_bound);
        }
        if let Some(lower_bound) = options.lower_bound {
            read_options.set_iterate_lower_bound(lower_bound);
        }

        let cf = self.column_family(cf.as_ref());
        let mut iter = self.db.raw_iterator_cf_opt(cf, read_options);

        match options.cursor {
            Some(cursor) => {
                match direction {
                    CursorDirection::Backward => iter.seek(cursor),
                    CursorDirection::Forward => iter.seek_for_prev(cursor),
                }
                // Skip the first item (cursor position)
                if iter.valid() {
                    match direction {
                        CursorDirection::Forward => iter.prev(),
                        CursorDirection::Backward => iter.next(),
                    }
                }
            }
            None => match direction {
                CursorDirection::Backward => iter.seek_to_first(), // Start at beginning of range
                CursorDirection::Forward => iter.seek_to_last(),   // Start at end of range
            },
        }

        let mut items = Vec::new();
        let mut next_cursor = None;
        let mut prev_cursor = None;

        // Collect results
        while iter.valid() && items.len() < options.limit {
            if let Some((key, _v)) = iter.item() {
                items.push(key.to_vec().into());
            } else {
                break; // No valid item found
            }

            // Move the iterator after capturing the current item
            match direction {
                CursorDirection::Forward => iter.prev(),
                CursorDirection::Backward => iter.next(),
            }
        }

        // Check if there are more items after our limit
        if iter.valid() {
            let key = items.last().cloned();
            match direction {
                CursorDirection::Forward => {
                    next_cursor = key;
                }
                CursorDirection::Backward => {
                    prev_cursor = key;
                }
            }
        }

        // Set the previous cursor if we have a valid item
        if options.cursor.is_some() {
            let key = items.first().cloned();
            match direction {
                CursorDirection::Forward => {
                    prev_cursor = key;
                }
                CursorDirection::Backward => {
                    next_cursor = key;
                }
            }
        }

        Ok(Range {
            items,
            direction,
            prev_cursor,
            next_cursor,
        })
    }

    fn iter<N>(
        &self,
        cf: N,
        options: super::IterOptions,
    ) -> impl Iterator<Item = Result<super::KVBytes, DriverError>>
    where
        N: AsRef<str>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let super::IterOptions::RocksDB((opts, mode)) = options;
        let mode = mode.unwrap_or(IteratorMode::Start);

        let iter = self
            .db
            .iterator_cf_opt(self.column_family(cf.as_ref()), opts, mode);

        iter.map(|item| item.map_err(Error::into_generic))
    }
}

impl Driver for RocksDBDriver {}

#[allow(irrefutable_let_patterns)]
#[allow(dead_code)]
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
    fn column_family<N>(&self, cf: N) -> &ColumnFamily
    where
        N: AsRef<str>,
    {
        self.db.column_family(cf)
    }

    pub fn commit(self) -> Result<(), DriverError> {
        self.tx.commit().map_err(Error::into_generic)
    }

    pub fn get<N, K: AsRef<[u8]>>(&self, table: N, key: K) -> Result<Option<Vec<u8>>, DriverError>
    where
        N: AsRef<str>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_reads, attrs);

        let cf = self.column_family(table);
        self.get_for_update_cf(cf, key)
    }

    fn get_for_update_cf<K: AsRef<[u8]>>(
        &self,
        cf: &ColumnFamily,
        key: K,
    ) -> Result<Option<Vec<u8>>, DriverError> {
        self.tx
            .get_for_update_cf(cf, key, true)
            .map_err(Error::into_generic)
    }

    pub fn put<N, K, V>(&self, cf: N, key: K, value: V) -> Result<(), DriverError>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_writes, attrs);

        let cf = self.column_family(cf);
        self.put_cf(cf, key, value)
    }

    fn put_cf<K, V>(&self, cf: &ColumnFamily, key: K, value: V) -> Result<(), DriverError>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.tx.put_cf(cf, key, value).map_err(Error::into_generic)
    }

    pub fn delete<N, K>(&self, cf: N, key: K) -> Result<(), DriverError>
    where
        N: AsRef<str>,
        K: AsRef<[u8]>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_deletes, attrs);

        let cf = self.column_family(cf.as_ref());
        self.delete_cf(cf, key)
    }

    fn delete_cf<K: AsRef<[u8]>>(&self, cf: &ColumnFamily, key: K) -> Result<(), DriverError> {
        self.tx.delete_cf(cf, key).map_err(Error::into_generic)
    }

    pub fn iter<N>(
        &'a self,
        cf: N,
        prefix: &'a [u8],
        options: IterOptions,
    ) -> impl Iterator<Item = Result<super::KVBytes, DriverError>> + 'a
    where
        N: AsRef<str>,
    {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_scans, attrs);

        let IterOptions::RocksDB((read_options, mode)) = options;

        let cf = self.column_family(cf.as_ref());
        let mode = mode.unwrap_or(IteratorMode::From(prefix, Direction::Forward));

        let iter = self.tx.iterator_cf_opt(cf, read_options, mode);

        iter.map(|item| item.map_err(Error::into_generic))
            .take_while(move |item| match item {
                Ok((key, _)) => key.starts_with(prefix),
                Err(_) => true,
            })
    }
}
