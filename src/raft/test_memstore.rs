use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use openraft::StorageError;

use super::memstore::Config;
use super::memstore::MemNodeId;
use super::memstore::MemStore;

struct MemBuilder {}
#[async_trait]
impl StoreBuilder<Config, Arc<MemStore>> for MemBuilder {
    async fn run_test<Fun, Ret, Res>(&self, t: Fun) -> Result<Ret, StorageError<MemNodeId>>
    where
        Res: Future<Output = Result<Ret, StorageError<MemNodeId>>> + Send,
        Fun: Fn(Arc<MemStore>) -> Res + Sync + Send,
    {
        let coordinator_config = crate::server_config::CoordinatorConfig::default();
        let store = MemStore::new(Arc::new(coordinator_config)).await?;
        t(Arc::new(store)).await
    }
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<MemNodeId>> {
    Suite::test_all(MemBuilder {})?;
    Ok(())
}