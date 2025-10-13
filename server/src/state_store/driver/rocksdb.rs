use std::{
    fmt::{self, Display},
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use opentelemetry::KeyValue;
use rocksdb::{
    ColumnFamily,
    ColumnFamilyDescriptor,
    DBCompactionStyle,
    DBCompressionType,
    Error as RocksDBError,
    LogLevel,
    Transaction,
    TransactionDB,
    TransactionDBOptions,
};
pub use rocksdb::{Direction, IteratorMode, Options as RocksDBOptions, ReadOptions};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
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

    #[error("Invalid RocksDB configuration")]
    InvalidConfiguration { option: String, message: String },

    #[error(transparent)]
    GenericRocksDBFailure { source: RocksDBError },
}

impl Error {
    fn into_generic(source: RocksDBError) -> DriverError {
        Self::GenericRocksDBFailure { source }.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBConfig {
    /// Create if missing
    pub create_if_missing: bool,

    /// Create missing column families
    pub create_missing_column_families: bool,

    /// The number of threads to start for flushing and compaction
    pub thread_count: i32,

    /// The maximum number of threads to use for flushing and compaction
    pub jobs_count: i32,

    /// The maximum number of write buffers which can be used
    pub max_write_buffer_number: i32,

    /// The amount of data each write buffer can build up in memory (in bytes)
    pub write_buffer_size: usize,

    /// The write-ahead-log size threshold to trigger archived WAL deletion (in
    /// bytes)
    pub wal_size_limit: u64,

    /// The total max write-ahead-log size before column family flushes (in
    /// bytes)
    pub max_total_wal_size: u64,

    /// The target file size for compaction (in bytes)
    pub target_file_size_base: u64,

    /// The target file size multiplier for each compaction level
    pub target_file_size_multiplier: i32,

    /// The number of files needed to trigger level 0 compaction
    pub level_zero_file_compaction_trigger: i32,

    /// The maximum number threads which will perform compactions
    pub max_concurrent_subcompactions: u32,

    /// Whether to use separate queues for WAL writes and memtable writes
    pub enable_pipelined_writes: bool,

    /// The maximum number of information log files to keep
    pub keep_log_file_num: usize,

    /// The information log level of the RocksDB library
    pub log_level: String,

    /// Database compaction style
    pub compaction_style: String,
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        RocksDBConfig {
            create_if_missing: true,
            create_missing_column_families: true,
            thread_count: 1,
            jobs_count: 2,
            max_write_buffer_number: 2,
            write_buffer_size: 32 * 1024 * 1024, // 32 MiB
            wal_size_limit: 0,
            max_total_wal_size: 1024 * 1024 * 1024,  // 1 GiB
            target_file_size_base: 64 * 1024 * 1024, // 64 MiB
            target_file_size_multiplier: 2,
            level_zero_file_compaction_trigger: 4,
            max_concurrent_subcompactions: 4,
            enable_pipelined_writes: true,
            keep_log_file_num: 10,
            log_level: "warn".to_string(),
            compaction_style: "level".to_string(),
        }
    }
}

impl Display for RocksDBConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_if_missing: {}, create_missing_column_families: {}, thread_count: {}, jobs_count: {}, max_write_buffer_number: {}, write_buffer_size: {}, wal_size_limit: {}, max_total_wal_size: {}, target_file_size_base: {}, target_file_size_multiplier: {}, level_zero_file_compaction_trigger: {}, max_concurrent_subcompactions: {}, enable_pipelined_writes: {}, keep_log_file_num: {}, log_level: {}, compaction_style: {}",
            self.create_if_missing,
            self.create_missing_column_families,
            self.thread_count,
            self.jobs_count,
            self.max_write_buffer_number,
            self.write_buffer_size,
            self.wal_size_limit,
            self.max_total_wal_size,
            self.target_file_size_base,
            self.target_file_size_multiplier,
            self.level_zero_file_compaction_trigger,
            self.max_concurrent_subcompactions,
            self.enable_pipelined_writes,
            self.keep_log_file_num,
            self.log_level,
            self.compaction_style
        )
    }
}

/// Options to start a connection with RocksDB.
pub(crate) struct Options {
    pub path: PathBuf,
    pub column_families: Vec<ColumnFamilyDescriptor>,
    pub config: RocksDBConfig,
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
        db_opts.create_if_missing(driver_options.config.create_if_missing);
        db_opts
            .create_missing_column_families(driver_options.config.create_missing_column_families);
        db_opts.increase_parallelism(driver_options.config.thread_count);
        db_opts.set_max_background_jobs(driver_options.config.jobs_count);
        db_opts.set_target_file_size_base(driver_options.config.target_file_size_base);
        db_opts.set_target_file_size_multiplier(driver_options.config.target_file_size_multiplier);
        db_opts.set_wal_size_limit_mb(driver_options.config.wal_size_limit);
        db_opts.set_max_total_wal_size(driver_options.config.max_total_wal_size);
        db_opts.set_write_buffer_size(driver_options.config.write_buffer_size);
        db_opts.set_max_write_buffer_number(driver_options.config.max_write_buffer_number);
        db_opts.set_level_zero_file_num_compaction_trigger(
            driver_options.config.level_zero_file_compaction_trigger,
        );
        db_opts.set_max_subcompactions(driver_options.config.max_concurrent_subcompactions);
        db_opts.set_enable_pipelined_write(driver_options.config.enable_pipelined_writes);
        db_opts.set_keep_log_file_num(driver_options.config.keep_log_file_num);
        db_opts.set_log_level(
            match driver_options
                .config
                .log_level
                .to_ascii_lowercase()
                .as_str()
            {
                "debug" => LogLevel::Debug,
                "info" => LogLevel::Info,
                "warn" => LogLevel::Warn,
                "error" => LogLevel::Error,
                "fatal" => LogLevel::Fatal,
                l => {
                    return Err(Error::InvalidConfiguration {
                        message: format!("Invalid log level: {l}"),
                        option: "log_level".to_string(),
                    });
                }
            },
        );
        db_opts.set_compaction_style(
            match driver_options
                .config
                .compaction_style
                .to_ascii_lowercase()
                .as_str()
            {
                "universal" => DBCompactionStyle::Universal,
                _ => DBCompactionStyle::Level,
            },
        );

        db_opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Snappy,
            DBCompressionType::Snappy,
            DBCompressionType::Snappy,
        ]);

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
        panic!(
            "tried to unwrap a RocksDBTransaction from a Transaction that was not created by the RocksDB driver: {tx:?}"
        );
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
        let result = self.tx.commit();

        // Count commits and errors
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_commits, attrs);

        // Count errors
        if let Err(err) = &result {
            let attrs = &[
                KeyValue::new("driver", "rocksdb"),
                KeyValue::new("driver.error_kind", format!("{:?}", err.kind())),
            ];
            let _inc_errors = Increment::inc(&self.db.metrics.driver_commits_errors, attrs);
        }
        result.map_err(Error::into_generic)
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
