use std::{pin::Pin, sync::Arc};

use anyhow::Result;
use rocksdb::ColumnFamilyDescriptor;
use tempfile::TempDir;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::{
    metrics::StateStoreMetrics,
    state_store::{
        self,
        driver::{
            Writer,
            rocksdb::{RocksDBConfig, RocksDBDriver},
        },
    },
};

/// A more complete test utility that handles custom column families
pub struct MigrationTestBuilder {
    column_families: Vec<String>,
}

impl MigrationTestBuilder {
    pub fn new() -> Self {
        Self {
            column_families: vec!["default".to_string()],
        }
    }

    /// Add column families to create initially
    pub fn with_column_family(mut self, cf_name: &str) -> Self {
        self.column_families.push(cf_name.to_string());
        self
    }

    /// Run the test with the given migration and setup/verify functions
    pub async fn run_test<M, S, V>(self, migration: &M, setup: S, verify: V) -> Result<()>
    where
        M: Migration,
        S: for<'a> FnOnce(
            &'a RocksDBDriver,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>,
        V: for<'a> FnOnce(
            &'a RocksDBDriver,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>>,
    {
        // Create temporary database directory
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        let metrics = Arc::new(StateStoreMetrics::new());
        // Create database with specified column families
        let db = state_store::open_database(
            path.to_path_buf(),
            RocksDBConfig::default(),
            self.column_families
                .into_iter()
                .map(|s| ColumnFamilyDescriptor::new(s, Default::default())),
            metrics,
        )?;

        // Run setup function to populate test data
        setup(&db).await?;

        // Close database
        drop(db);

        // Prepare the database for migration
        let prepare_ctx = PrepareContext::new(path.to_path_buf(), RocksDBConfig::default());
        let db = migration.prepare(&prepare_ctx).await?;

        // Apply the migration
        let txn = db.transaction();
        let mut migration_ctx = MigrationContext::new(db.clone(), txn);

        migration.apply(&migration_ctx).await?;
        migration_ctx.commit().await?;

        // Run verification
        verify(&db).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::state_store::{
        driver::{Reader, Writer},
        state_machine::IndexifyObjectsColumns,
    };

    #[derive(Clone)]
    struct MockMigration {
        version_num: u64,
    }

    #[async_trait]
    impl Migration for MockMigration {
        fn version(&self) -> u64 {
            self.version_num
        }

        fn name(&self) -> &'static str {
            "Mock Migration"
        }

        async fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
            ctx.open_db()
        }

        async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
            // Simple mock implementation that just puts a marker
            ctx.txn
                .put(
                    IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
                    b"migration_test",
                    format!("v{}", self.version_num).as_bytes(),
                )
                .await?;
            Ok(())
        }

        fn box_clone(&self) -> Box<dyn Migration> {
            Box::new(self.clone())
        }
    }

    #[tokio::test]
    async fn test_migration_test_builder() -> Result<()> {
        let migration = MockMigration { version_num: 43 };

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::StateMachineMetadata.as_ref())
            .run_test(
                &migration,
                |_db| Box::pin(async { Ok(()) }),
                |db| {
                    Box::pin(async move {
                        // Verify migration was applied
                        let result = db
                            .get(
                                IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
                                b"migration_test",
                            )
                            .await?;

                        assert_eq!(result, Some(b"v43".to_vec()));
                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
