use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use foundationdb::{
    Database,
    FdbError,
    KeySelector,
    RangeOption,
    Transaction,
    TransactionCommitError,
    api::NetworkAutoStop,
    options::DatabaseOption,
    tuple::Subspace,
};
use futures::lock::Mutex;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use tracing::{debug, error};

use crate::{
    metrics::{Increment, StateStoreMetrics},
    state_store::{
        driver::{
            Driver,
            Error as DriverError,
            InnerTransaction,
            IterOptions,
            KVBytes,
            Range,
            RangeOptions,
            Reader,
            Writer,
        },
        scanner::CursorDirection,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to open FoundationDB database. error: {}", source)]
    OpenDatabaseFailed { source: FdbError },

    #[error(transparent)]
    GenericFoundationDBFailure { source: FdbError },

    #[error(transparent)]
    TransactionFailed { source: TransactionCommitError },
}

/// Options to start a connection with FoundationDB.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Options {
    pub cluster_file: Option<String>,
    pub transaction_timeout: Option<i32>,
    pub retry_limit: Option<i32>,
    pub max_retry_delay: Option<i32>,
}

/// Driver to connect with a FoundationDB database.
pub(crate) struct FoundationDBDriver {
    database: Database,
    metrics: Arc<StateStoreMetrics>,
    _fdb_network: Arc<NetworkAutoStop>,
}

impl FoundationDBDriver {
    /// Open a new connection with a FoundationDB database.
    #[tracing::instrument(skip(metrics))]
    pub(crate) fn open(
        options: Options,
        metrics: Arc<StateStoreMetrics>,
    ) -> Result<FoundationDBDriver, Error> {
        debug!("Opening FoundationDB connection");
        // Initialize the FoundationDB Client API
        static NETWORK: LazyLock<Arc<foundationdb::api::NetworkAutoStop>> =
            LazyLock::new(|| Arc::new(unsafe { foundationdb::boot() }));
        // Store the network cancellation handle
        let _fdb_network = (*NETWORK).clone();

        let database = Database::new(options.cluster_file.as_deref())
            .map_err(|source| Error::OpenDatabaseFailed { source })?;
        if let Some(transaction_timeout) = options.transaction_timeout {
            database
                .set_option(DatabaseOption::TransactionTimeout(transaction_timeout))
                .map_err(|source| Error::OpenDatabaseFailed { source })?;
        }
        if let Some(retry_limit) = options.retry_limit {
            database
                .set_option(DatabaseOption::TransactionRetryLimit(retry_limit))
                .map_err(|source| Error::OpenDatabaseFailed { source })?;
        }
        if let Some(max_retry_delay) = options.max_retry_delay {
            database
                .set_option(DatabaseOption::TransactionMaxRetryDelay(max_retry_delay))
                .map_err(|source| Error::OpenDatabaseFailed { source })?;
        }

        Ok(FoundationDBDriver {
            database,
            metrics,
            _fdb_network,
        })
    }
}

#[async_trait::async_trait]
impl Writer for FoundationDBDriver {
    fn transaction(&self) -> Result<super::Transaction, DriverError> {
        let tx = self
            .database
            .create_trx()
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        Ok(Arc::new(FoundationDBTransaction {
            metrics: self.metrics.clone(),
            tx: Mutex::new(Some(tx)),
        }))
    }

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_writes, attrs);

        let tx = self.database.create_trx().unwrap();
        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));
        tx.set(&key, value);
        tx.commit()
            .await
            .map_err(|source| Error::TransactionFailed { source })?;
        Ok(())
    }

    #[cfg(feature = "migrations")]
    async fn drop(&mut self, _cf: &str) -> Result<(), DriverError> {
        // FoundationDB doesn't have column families, so drop is a no-op
        Ok(())
    }

    #[cfg(feature = "migrations")]
    async fn create(
        &mut self,
        _name: &str,
        _opts: &super::CreateOptions,
    ) -> Result<(), DriverError> {
        // FoundationDB doesn't have column families, so create is a no-op
        Ok(())
    }
}

#[async_trait::async_trait]
impl Reader for FoundationDBDriver {
    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_reads, attrs);

        let tx = self.database.create_trx().unwrap();
        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));
        let value = tx
            .get(&key, false)
            .await
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        Ok(value.map(|v| v.to_vec()))
    }

    async fn list_existent_items(
        &self,
        cf: &str,
        keys: Vec<&[u8]>,
    ) -> Result<Vec<Bytes>, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let tx = self.database.create_trx().unwrap();

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let queries = keys
            .into_iter()
            .map(|k| subspace.pack(&k))
            .map(|k| tx.get(&k, false))
            .collect::<Vec<_>>();

        let items = futures::future::join_all(queries).await;
        let mut flatten = Vec::with_capacity(items.len());
        for item in items {
            let item = item.map_err(|source| Error::GenericFoundationDBFailure { source })?;
            if let Some(value) = item {
                flatten.push(value.to_vec().into());
            }
        }

        Ok(flatten)
    }

    async fn get_key_range(&self, cf: &str, options: RangeOptions) -> Result<Range, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let tx = self.database.create_trx().unwrap();
        let subspace = Subspace::from_bytes(cf.as_bytes());

        let begin = if let Some(cursor) = &options.cursor {
            let slice: &[u8] = cursor.as_ref();
            KeySelector::first_greater_or_equal(subspace.pack(&slice))
        } else if let Some(lower) = &options.lower_bound {
            let slice: &[u8] = lower.as_ref();
            KeySelector::first_greater_or_equal(subspace.pack(&slice))
        } else {
            let empty: &[u8] = &[];
            KeySelector::first_greater_or_equal(subspace.pack(&empty))
        };

        let end = if let Some(upper) = &options.upper_bound {
            let slice: &[u8] = upper.as_ref();
            KeySelector::first_greater_or_equal(subspace.pack(&slice))
        } else {
            let large: &[u8] = &[255u8];
            KeySelector::first_greater_or_equal(subspace.pack(&large)) // Some large key
        };

        let reverse = matches!(options.direction, Some(CursorDirection::Backward));

        let range_option = RangeOption {
            begin,
            end,
            reverse,
            ..Default::default()
        };

        let range_result = tx
            .get_range(&range_option, options.limit, false)
            .await
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        let items: Vec<Bytes> = range_result
            .into_iter()
            .map(|kv| kv.value().to_vec().into())
            .collect();

        let direction = options.direction.unwrap_or(CursorDirection::Forward);

        // For simplicity, no pagination cursors for now
        let prev_cursor = None;
        let next_cursor = None;

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
        _options: IterOptions,
    ) -> Box<dyn Iterator<Item = Result<KVBytes, DriverError>> + '_> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let tx = self.database.create_trx().unwrap();
        let subspace = Subspace::from_bytes(cf.as_bytes());

        // Simple implementation: get all keys in the subspace
        let empty: &[u8] = &[];
        let begin = KeySelector::first_greater_or_equal(subspace.pack(&empty));
        let large: &[u8] = &[255u8];
        let end = KeySelector::first_greater_or_equal(subspace.pack(&large));

        let range_option = RangeOption {
            begin,
            end,
            reverse: false,
            ..Default::default()
        };

        let range_result = tx.get_range(&range_option, 1000, false).await.unwrap();

        let iter = range_result.into_iter().map(|kv| {
            Ok((
                kv.key().to_vec().into_boxed_slice(),
                kv.value().to_vec().into_boxed_slice(),
            ))
        });

        Box::new(iter)
    }
}

impl Driver for FoundationDBDriver {}

#[allow(dead_code)]
pub(crate) struct FoundationDBTransaction {
    metrics: Arc<StateStoreMetrics>,
    tx: Mutex<Option<Transaction>>,
}

#[async_trait::async_trait]
impl InnerTransaction for FoundationDBTransaction {
    async fn commit(&self) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        Increment::inc(&self.metrics.driver_commits, attrs);

        let mut guard = self.tx.lock().await;
        let tx = guard.take().expect("Transaction not initialized");
        tx.commit()
            .await
            .map_err(|source| Error::TransactionFailed { source })?;
        Ok(())
    }

    async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_reads, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        let value = tx
            .get(&key, false)
            .await
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        Ok(value.map(|v| v.to_vec()))
    }

    async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_writes, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        tx.set(&key, value);
        Ok(())
    }

    async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_deletes, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");
        tx.clear(&key);
        Ok(())
    }

    async fn iter(
        &self,
        cf: &str,
        prefix: Vec<u8>,
    ) -> Result<Vec<Result<KVBytes, DriverError>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());

        // // For prefix, set begin to prefix, end to prefix + 1
        let begin = KeySelector::first_greater_or_equal(subspace.pack(&prefix));
        let end_prefix = if prefix.is_empty() {
            vec![255u8]
        } else {
            let mut end = prefix.to_vec();
            // Increment the last byte to get the next prefix
            for i in (0..end.len()).rev() {
                if end[i] < 255 {
                    end[i] += 1;
                    end.truncate(i + 1);
                    break;
                }
            }
            end
        };
        let end = KeySelector::first_greater_or_equal(subspace.pack(&end_prefix));

        let range_option = RangeOption {
            begin,
            end,
            ..Default::default()
        };

        let guard = self.tx.lock().await;
        let tx = guard.as_ref().expect("Transaction not initialized");

        let range_result = tx
            .get_range(&range_option, 1000, false) // Some large limit
            .await
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        let col = range_result
            .into_iter()
            .map(|kv| {
                Ok((
                    kv.key().to_vec().into_boxed_slice(),
                    kv.value().to_vec().into_boxed_slice(),
                ))
            })
            .collect::<Vec<_>>();

        Ok(col)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::metrics::StateStoreMetrics;

    #[tokio::test]
    #[ignore]
    async fn test_foundationdb_driver_basic_operations() {
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver = FoundationDBDriver::open(Default::default(), metrics)
            .expect("Failed to open FoundationDB driver");

        // Test basic put/get operations
        let cf = "test_cf";
        let key = b"test_key";
        let value = b"test_value";

        driver
            .put(cf, key, value)
            .await
            .expect("Failed to put value");

        let retrieved = driver.get(cf, key).await.expect("Failed to get value");
        assert_eq!(retrieved, Some(value.to_vec()));
    }

    #[tokio::test]
    #[ignore]
    async fn test_foundationdb_driver_transaction() {
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver = FoundationDBDriver::open(Default::default(), metrics)
            .expect("Failed to open FoundationDB driver");

        // Test transaction operations
        let cf = "test_cf";
        let key1 = b"key1";
        let value1 = b"value1";
        let key2 = b"key2";
        let value2 = b"value2";

        let tx = driver.transaction().unwrap();
        tx.put(cf, key1, value1)
            .await
            .expect("Failed to put in transaction");
        tx.put(cf, key2, value2)
            .await
            .expect("Failed to put in transaction");
        tx.commit().await.expect("Failed to commit transaction");

        // Verify the values were committed
        let retrieved1 = driver.get(cf, key1).await.expect("Failed to get value1");
        let retrieved2 = driver.get(cf, key2).await.expect("Failed to get value2");

        assert_eq!(retrieved1, Some(value1.to_vec()));
        assert_eq!(retrieved2, Some(value2.to_vec()));
    }

    // #[test]
    // fn test_foundationdb_driver_range_scan() {
    //     let options = Options::default();
    //     let metrics = Arc::new(StateStoreMetrics::new());

    //     let driver =
    //         FoundationDBDriver::open(options, metrics).expect("Failed to open
    // FoundationDB driver");

    //     // Insert multiple values
    //     let cf = "test_cf";
    //     let values = vec![
    //         (b"key1", b"value1"),
    //         (b"key2", b"value2"),
    //         (b"key3", b"value3"),
    //     ];

    //     for (key, value) in &values {
    //         driver.put(cf, key, value).expect("Failed to put value");
    //     }

    //     // Test range scan
    //     let range_options = RangeOptionsBuilder::default()
    //         .limit(100)
    //         .build()
    //         .expect("Failed to build range options");
    //     let range = driver
    //         .get_key_range(cf, range_options)
    //         .expect("Failed to get key range");

    //     assert_eq!(range.items.len(), 3);
    // }

    #[tokio::test]
    #[ignore]
    async fn test_foundationdb_driver_list_existent_items() {
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver = FoundationDBDriver::open(Default::default(), metrics)
            .expect("Failed to open FoundationDB driver");

        // Insert values
        let cf = "test_cf";
        let key1 = b"key1";
        let value1 = b"value1";
        let key2 = b"key2";
        let value2 = b"value2";
        let key3 = b"key3"; // This won't be inserted

        driver
            .put(cf, key1, value1)
            .await
            .expect("Failed to put value1");
        driver
            .put(cf, key2, value2)
            .await
            .expect("Failed to put value2");

        // Test list_existent_items
        let keys = vec![key1.as_ref(), key2.as_ref(), key3.as_ref()];
        let items = driver
            .list_existent_items(cf, keys)
            .await
            .expect("Failed to list items");

        assert_eq!(items.len(), 2); // Only key1 and key2 should be found
    }
}
