use std::{collections::HashMap, fmt, sync::Arc};

use bytes::Bytes;
use derive_builder::UninitializedFieldError;
use foundationdb::{
    Database, FdbError, Transaction, TransactionCommitError, api::NetworkAutoStop, tuple::Subspace,
};
use opentelemetry::KeyValue;
use serde::{Serialize, de::DeserializeOwned};
use tracing::{error, warn};

use crate::{
    data_model::clocks::Linearizable,
    metrics::{Increment, StateStoreMetrics},
    state_store::{
        driver::{
            Driver, Error as DriverError, IterOptions, Range, RangeOptions, RangeOptionsBuilder,
            Reader, Writer,
        },
        serializer::{JsonEncode, JsonEncoder},
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
#[derive(Clone, Debug, Default)]
pub(crate) struct Options {
    pub cluster_file: Option<String>,
    pub database_name: Option<String>,
}

/// Driver to connect with a FoundationDB database.
pub(crate) struct FoundationDBDriver {
    network: NetworkAutoStop,
    database: Database,
    metrics: Arc<StateStoreMetrics>,
}

impl FoundationDBDriver {
    /// Open a new connection with a FoundationDB database.
    ///
    /// NOTE: This is a mock implementation. For production use, install
    /// FoundationDB client libraries and replace this with real implementation.
    pub(crate) fn open(
        _driver_options: Options,
        metrics: Arc<StateStoreMetrics>,
    ) -> Result<FoundationDBDriver, Error> {
        let network = unsafe { foundationdb::boot() };
        let database =
            Database::default().map_err(|source| Error::OpenDatabaseFailed { source })?;

        Ok(FoundationDBDriver {
            network,
            database,
            metrics,
        })
    }
}

#[async_trait::async_trait]
impl Writer for FoundationDBDriver {
    fn transaction(&self) -> super::Transaction<'_> {
        super::Transaction::FoundationDB(FoundationDBTransaction {
            db: self,
            tx: self.database.create_trx().unwrap(),
        })
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

    fn drop(&mut self, _cf: &str) -> Result<(), DriverError> {
        unimplemented!(
            "Drop is only used in migrations. We don't support migrations for the FoundationDB driver"
        );
    }

    fn create(&mut self, _name: &str, _opts: &super::CreateOptions) -> Result<(), DriverError> {
        unimplemented!(
            "Create is only used in migrations. We don't support migrations for the FoundationDB driver"
        );
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

    async fn get_key_range(&self, _cf: &str, _options: RangeOptions) -> Result<Range, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.metrics.driver_scans, attrs);

        unimplemented!()
    }

    fn iter(
        &self,
        _cf: &str,
        _options: IterOptions<'_>,
    ) -> Box<dyn Iterator<Item = Result<super::KVBytes, DriverError>> + '_> {
        unimplemented!()
    }
}

impl Driver for FoundationDBDriver {}

#[allow(irrefutable_let_patterns)]
#[allow(dead_code)]
/// Ensure that the transaction we're using has been generated by the
/// FoundationDB driver. Using a transaction from another driver is an
/// irrecoverable failure and we should crash the server.
fn unwrap_foundationdb_transaction<'db>(
    tx: &'db super::Transaction,
) -> &'db FoundationDBTransaction<'db> {
    let super::Transaction::FoundationDB(tx) = tx else {
        panic!(
            "tried to unwrap a FoundationDBTransaction from a Transaction that was not created by the FoundationDB driver: {tx:?}"
        );
    };
    tx
}

#[allow(dead_code)]
pub(crate) struct FoundationDBTransaction<'a> {
    db: &'a FoundationDBDriver,
    tx: Transaction,
}

impl<'a> FoundationDBTransaction<'a> {
    pub async fn commit(self) -> Result<(), DriverError> {
        self.tx
            .commit()
            .await
            .map_err(|source| Error::TransactionFailed { source })?;
        Ok(())
    }

    pub async fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>, DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_reads, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));
        let value = self
            .tx
            .get(&key, false)
            .await
            .map_err(|source| Error::GenericFoundationDBFailure { source })?;

        Ok(value.map(|v| v.to_vec()))
    }

    pub async fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_writes, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));
        self.tx.set(&key, value);
        Ok(())
    }

    pub async fn delete(&self, cf: &str, key: &[u8]) -> Result<(), DriverError> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_deletes, attrs);

        let subspace = Subspace::from_bytes(cf.as_bytes());
        let key = subspace.pack(&(key));
        self.tx.clear(&key);
        Ok(())
    }

    pub fn iter(
        &self,
        _cf: &str,
        _prefix: &[u8],
        _options: IterOptions<'_>,
    ) -> Box<dyn Iterator<Item = Result<super::KVBytes, DriverError>> + 'a> {
        let attrs = &[KeyValue::new("driver", "foundationdb")];
        let _inc = Increment::inc(&self.db.metrics.driver_scans, attrs);

        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::metrics::StateStoreMetrics;

    #[tokio::test]
    async fn test_foundationdb_driver_basic_operations() {
        let options = Options::default();
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver =
            FoundationDBDriver::open(options, metrics).expect("Failed to open FoundationDB driver");

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
    async fn test_foundationdb_driver_transaction() {
        let options = Options::default();
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver =
            FoundationDBDriver::open(options, metrics).expect("Failed to open FoundationDB driver");

        // Test transaction operations
        let cf = "test_cf";
        let key1 = b"key1";
        let value1 = b"value1";
        let key2 = b"key2";
        let value2 = b"value2";

        let tx = driver.transaction();
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
    //         FoundationDBDriver::open(options, metrics).expect("Failed to open FoundationDB driver");

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
    async fn test_foundationdb_driver_list_existent_items() {
        let options = Options::default();
        let metrics = Arc::new(StateStoreMetrics::new());

        let driver =
            FoundationDBDriver::open(options, metrics).expect("Failed to open FoundationDB driver");

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
