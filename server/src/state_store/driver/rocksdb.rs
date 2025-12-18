use std::{
    env,
    fmt::{self, Display},
    fs,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::Mutex;
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
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use strum::IntoEnumIterator;
use tracing::{error, warn};

use crate::{
    metrics::{Increment, StateStoreMetrics},
    state_store::{
        driver::{
            Driver,
            Error as DriverError,
            IterOptions,
            KVBytes,
            Range,
            RangeOptions,
            Reader,
            Writer,
        },
        scanner::CursorDirection,
        state_machine::IndexifyObjectsColumns,
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

    #[error("Failed to create state store dir: {source}")]
    CreateDirFailed { source: std::io::Error },

    #[allow(dead_code)]
    #[error("Failed to make the connection mutable")]
    MakeConnectionMutableFailed,
}

impl Error {
    fn into_generic(source: RocksDBError) -> DriverError {
        Self::GenericRocksDBFailure { source }.into()
    }
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBConfig {
    /// Create if missing
    #[serde_inline_default(true)]
    pub create_if_missing: bool,

    /// Create missing column families
    #[serde_inline_default(true)]
    pub create_missing_column_families: bool,

    /// The number of threads to start for flushing and compaction
    #[serde_inline_default(1)]
    pub thread_count: i32,

    /// The maximum number of threads to use for flushing and compaction
    #[serde_inline_default(2)]
    pub jobs_count: i32,

    /// The maximum number of write buffers which can be used
    #[serde_inline_default(2)]
    pub max_write_buffer_number: i32,

    /// The amount of data each write buffer can build up in memory (in bytes)
    #[serde_inline_default(32 * 1024 * 1024)] // 32 MiB
    pub write_buffer_size: usize,

    /// The write-ahead-log size threshold to trigger archived WAL deletion (in
    /// bytes)
    #[serde_inline_default(0)]
    pub wal_size_limit: u64,

    /// The total max write-ahead-log size before column family flushes (in
    /// bytes)
    #[serde_inline_default(1024 * 1024 * 1024)] // 1 GiB
    pub max_total_wal_size: u64,

    /// The target file size for compaction (in bytes)
    #[serde_inline_default(64 * 1024 * 1024)] // 64 MiB
    pub target_file_size_base: u64,

    /// The target file size multiplier for each compaction level
    #[serde_inline_default(2)]
    pub target_file_size_multiplier: i32,

    /// The number of files needed to trigger level 0 compaction
    #[serde_inline_default(4)]
    pub level_zero_file_compaction_trigger: i32,

    /// The maximum number threads which will perform compactions
    #[serde_inline_default(4)]
    pub max_concurrent_subcompactions: u32,

    /// Whether to use separate queues for WAL writes and memtable writes
    #[serde_inline_default(true)]
    pub enable_pipelined_writes: bool,

    /// The maximum number of information log files to keep
    #[serde_inline_default(10)]
    pub keep_log_file_num: usize,

    /// The information log level of the RocksDB library
    #[serde_inline_default("warn".to_string())]
    pub log_level: String,

    /// Database compaction style
    #[serde_inline_default("level".to_string())]
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
#[derive(Serialize)]
pub(crate) struct Options {
    pub path: PathBuf,
    #[serde(skip_serializing)]
    pub column_families: Vec<ColumnFamilyDescriptor>,
    pub config: RocksDBConfig,
}

impl Clone for Options {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            column_families: self
                .column_families
                .iter()
                .map(|cf| {
                    ColumnFamilyDescriptor::new_with_ttl(cf.name(), Default::default(), cf.ttl())
                })
                .collect::<Vec<_>>(),
            config: self.config.clone(),
        }
    }
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("path", &self.path)
            .field(
                "column_families",
                &self
                    .column_families
                    .iter()
                    .map(|cf| cf.name())
                    .collect::<Vec<_>>(),
            )
            .field("config", &self.config)
            .finish()
    }
}

impl<'de> Deserialize<'de> for Options {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct OptionsHelper {
            #[serde(default = "default_indexify_storage_path")]
            path: PathBuf,
            #[serde(default)]
            config: RocksDBConfig,
        }

        let helper = OptionsHelper::deserialize(deserializer)?;

        Ok(Options {
            path: helper.path,
            config: helper.config,
            column_families: all_column_families(),
        })
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            path: default_indexify_storage_path(),
            config: RocksDBConfig::default(),
            column_families: all_column_families(),
        }
    }
}

fn default_indexify_storage_path() -> PathBuf {
    env::current_dir()
        .unwrap()
        .join("indexify_storage")
        .join("state")
}

fn all_column_families() -> Vec<ColumnFamilyDescriptor> {
    IndexifyObjectsColumns::iter()
        .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Default::default()))
        .collect::<Vec<_>>()
}

/// Driver to connect with a RocksDB database.
#[derive(Clone)]
pub(crate) struct RocksDBDriver {
    db: Arc<TransactionDB>,
    metrics: Arc<StateStoreMetrics>,
}

impl RocksDBDriver {
    /// Open a new connection with a RocksDB database.
    pub(crate) fn open(
        driver_options: Options,
        metrics: Arc<StateStoreMetrics>,
    ) -> Result<RocksDBDriver, Error> {
        fs::create_dir_all(driver_options.path.clone())
            .map_err(|source| Error::CreateDirFailed { source })?;

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

        Ok(RocksDBDriver {
            db: Arc::new(db),
            metrics,
        })
    }
}

impl RocksDBDriver {
    /// Returns a reference to the column family with the given name.
    ///
    /// This function doesn't need to be async, it just gets the value from an
    /// internal map.
    fn column_family(&self, name: &str) -> &ColumnFamily {
        let Some(handle) = self.db.cf_handle(name) else {
            panic!("failed to get column family handle for {name}");
        };

        handle
    }
}

#[async_trait]
impl Writer for RocksDBDriver {
    fn transaction(&self) -> Result<super::Transaction, DriverError> {
        let tx = self.db.transaction();

        // The database reference must always outlive
        // the transaction. If it doesn't then this
        // is undefined behaviour. This unsafe block
        // ensures that the transaction reference is
        // static, but will cause a crash if the
        // database is dropped prematurely.
        let inner = unsafe {
            std::mem::transmute::<
                rocksdb::Transaction<'_, TransactionDB>,
                rocksdb::Transaction<'static, TransactionDB>,
            >(tx)
        };

        Ok(Arc::new(RocksDBTransaction {
            db: self.clone(),
            tx: Mutex::new(Some(inner)),
        }))
    }

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_writes, attrs);

        let cf = self.column_family(cf);
        self.db.put_cf(cf, key, value).map_err(Error::into_generic)
    }

    #[cfg(feature = "migrations")]
    async fn drop(&mut self, cf: &str) -> Result<(), DriverError> {
        // `drop` is used in migrations to remove old column families.
        // Migrations run serially, so only one reference to the database should
        // be held at a time.
        // If that premise changes, we'll raise an error during migration.
        let mut_db = Arc::get_mut(&mut self.db).ok_or(Error::MakeConnectionMutableFailed)?;
        mut_db.drop_cf(cf).map_err(Error::into_generic)
    }

    #[cfg(feature = "migrations")]
    async fn create(&mut self, cf: &str, opts: &super::CreateOptions) -> Result<(), DriverError> {
        // `create` is used in migrations to remove old column families.
        // Migrations run serially, so only one reference to the database should
        // be held at a time.
        // If that premise changes, we'll raise an error during migration.
        let super::CreateOptions::RocksDB(opts) = opts;
        let mut_db = Arc::get_mut(&mut self.db).ok_or(Error::MakeConnectionMutableFailed)?;
        mut_db.create_cf(cf, opts).map_err(Error::into_generic)
    }
}

#[async_trait]
impl Reader for RocksDBDriver {
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_reads, attrs);

        let cf = self.column_family(cf);
        self.db.get_cf(cf, key).map_err(Error::into_generic)
    }

    async fn list_existent_items(
        &self,
        cf: &str,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<Bytes>, DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let cf_handle = self.column_family(cf);

        let mut items = Vec::with_capacity(keys.len());
        let multi_get_keys: Vec<_> = keys.iter().map(|k| (&cf_handle, k)).collect();
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

    async fn get_key_range(&self, cf: &str, options: RangeOptions) -> Result<Range, DriverError> {
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

        match &options.cursor {
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

    async fn iter(
        &self,
        cf: &str,
        options: IterOptions,
    ) -> Box<dyn Iterator<Item = Result<super::KVBytes, DriverError>> + '_> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let super::IterOptions::RocksDB((opts, (token, direction))) = options;

        let mode = match (&token, direction) {
            (Some(t), Some(d)) => rocksdb::IteratorMode::From(t, d),
            (Some(t), None) => rocksdb::IteratorMode::From(t, rocksdb::Direction::Forward),
            (None, _) => rocksdb::IteratorMode::Start,
        };

        Box::new(
            self.db
                .iterator_cf_opt(self.column_family(cf), opts, mode)
                .map(|item| item.map_err(Error::into_generic)),
        )
    }
}

impl Driver for RocksDBDriver {}

pub(crate) struct RocksDBTransaction {
    db: RocksDBDriver,
    tx: Mutex<Option<Transaction<'static, TransactionDB>>>,
}

#[async_trait]
impl super::InnerTransaction for RocksDBTransaction {
    async fn commit(&self) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_commits, attrs);

        let mut guard = self.tx.lock().await;
        let tx = guard.take().expect("Transaction not initialized");
        let result = tx.commit();

        // Count errors
        if let Err(err) = &result {
            let attrs = &[
                KeyValue::new("driver", "rocksdb"),
                KeyValue::new("driver.error_kind", format!("{:?}", err.kind())),
            ];
            Increment::inc(&self.db.metrics.driver_commits_errors, attrs);
        }
        result.map_err(Error::into_generic)
    }

    async fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_reads, attrs);

        let cf = self.db.column_family(table);

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        tx.get_for_update_cf(cf, key, true)
            .map_err(Error::into_generic)
    }

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_writes, attrs);

        let cf = self.db.column_family(cf);

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        tx.put_cf(cf, key, value).map_err(Error::into_generic)
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_deletes, attrs);

        let cf = self.db.column_family(cf);

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        tx.delete_cf(cf, key).map_err(Error::into_generic)
    }

    async fn iter(
        &self,
        cf: &str,
        prefix: Vec<u8>,
    ) -> Result<Vec<Result<KVBytes, DriverError>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "rocksdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_scans, attrs);

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        let cf = self.db.column_family(cf);

        let col = tx
            .iterator_cf(cf, IteratorMode::Start)
            .map(|item| item.map_err(Error::into_generic))
            .take_while(move |item| {
                let Ok((key, _)) = item else {
                    return false;
                };
                key.starts_with(prefix.as_ref())
            })
            .collect();

        Ok(col)
    }
}
