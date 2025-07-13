use anyhow::Result;
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};

#[derive(Clone)]
/// Migration to remove the deprecated Executors column family
pub struct V4DropExecutorsMigration {}

impl Migration for V4DropExecutorsMigration {
    fn version(&self) -> u64 {
        4
    }

    fn name(&self) -> &'static str {
        "Drop Executors column family"
    }

    fn prepare(&self, ctx: &PrepareContext) -> Result<rocksdb::TransactionDB> {
        // Check if the Executors CF exists and drop it if needed
        let existing_cfs = ctx.list_cfs()?;

        ctx.reopen_with_cf_operations(|db| {
            // Drop Executors CF if it exists
            if existing_cfs.contains(&"Executors".to_string()) {
                info!("Dropping Executors column family");
                db.drop_cf("Executors")?;
            } else {
                info!("Executors column family doesn't exist, no action needed");
            }

            Ok(())
        })
    }

    fn apply(&self, _ctx: &MigrationContext) -> Result<()> {
        // No data migration needed, just log completion
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};

    use super::*;
    use crate::state_store::migrations::testing::MigrationTestBuilder;

    #[test]
    fn test_v4_migration_with_executors_cf() -> Result<()> {
        let migration = V4DropExecutorsMigration {};

        // Create DB with custom setup that includes Executors CF
        let temp_dir = tempfile::TempDir::new()?;
        let path = temp_dir.path();

        // First create a DB with Executors CF
        {
            let mut db_opts = Options::default();
            db_opts.create_missing_column_families(true);
            db_opts.create_if_missing(true);

            let cf_descriptors = vec![
                ColumnFamilyDescriptor::new("default", Options::default()),
                ColumnFamilyDescriptor::new("Executors", Options::default()),
            ];

            let _db: TransactionDB = TransactionDB::open_cf_descriptors(
                &db_opts,
                &TransactionDBOptions::default(),
                path,
                cf_descriptors,
            )?;
            // DB is dropped here when it goes out of scope
        }

        // Now run migration
        let prepare_ctx = PrepareContext::new(path.to_path_buf());
        let db = migration.prepare(&prepare_ctx)?;

        // Verify Executors CF was dropped
        let cfs = prepare_ctx.list_cfs()?;
        assert!(!cfs.contains(&"Executors".to_string()));

        // Run apply phase (no-op in this case)
        let txn = db.transaction();
        let migration_ctx = MigrationContext::new(&db, &txn);
        migration.apply(&migration_ctx)?;
        txn.commit()?;

        Ok(())
    }

    #[test]
    fn test_v4_migration_without_executors_cf() -> Result<()> {
        let migration = V4DropExecutorsMigration {};

        MigrationTestBuilder::new().run_test(
            &migration,
            |_db| {
                // No setup needed - DB doesn't have Executors CF
                Ok(())
            },
            |db| {
                // Verify migration completes without error
                let txn = db.transaction();
                txn.commit()?;
                Ok(())
            },
        )?;

        Ok(())
    }
}
