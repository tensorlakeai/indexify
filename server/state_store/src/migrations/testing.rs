use anyhow::Result;
use rocksdb::{Options, TransactionDB, TransactionDBOptions};
use tempfile::TempDir;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};

/// A more complete test utility that handles custom column families
pub struct MigrationTestBuilder {
    column_families: Vec<String>,
    db_opts: Options,
}

impl MigrationTestBuilder {
    pub fn new() -> Self {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        Self {
            column_families: vec!["default".to_string()],
            db_opts,
        }
    }

    /// Add column families to create initially
    pub fn with_column_family(mut self, cf_name: &str) -> Self {
        self.column_families.push(cf_name.to_string());
        self
    }

    /// Run the test with the given migration and setup/verify functions
    pub fn run_test<M, S, V>(self, migration: &M, setup: S, verify: V) -> Result<()>
    where
        M: Migration,
        S: FnOnce(&TransactionDB) -> Result<()>,
        V: FnOnce(&TransactionDB) -> Result<()>,
    {
        // Create temporary database directory
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        // Create database with specified column families
        let db = TransactionDB::open_cf(
            &self.db_opts,
            &TransactionDBOptions::default(),
            path,
            &self.column_families,
        )?;

        // Run setup function to populate test data
        setup(&db)?;

        // Close database
        drop(db);

        // Prepare the database for migration
        let prepare_ctx = PrepareContext::new(path.to_path_buf());
        let db = migration.prepare(&prepare_ctx)?;

        // Apply the migration
        let txn = db.transaction();
        let migration_ctx = MigrationContext::new(&db, &txn);

        migration.apply(&migration_ctx)?;
        txn.commit()?;

        // Run verification
        verify(&db)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::IndexifyObjectsColumns;

    #[derive(Clone)]
    struct MockMigration {
        version_num: u64,
    }

    impl Migration for MockMigration {
        fn version(&self) -> u64 {
            self.version_num
        }

        fn name(&self) -> &'static str {
            "Mock Migration"
        }

        fn prepare(&self, ctx: &PrepareContext) -> Result<TransactionDB> {
            ctx.open_db()
        }

        fn apply(&self, ctx: &MigrationContext) -> Result<()> {
            // Simple mock implementation that just puts a marker
            ctx.txn.put_cf(
                ctx.cf(&IndexifyObjectsColumns::StateMachineMetadata),
                b"migration_test",
                format!("v{}", self.version_num).as_bytes(),
            )?;
            Ok(())
        }

        fn box_clone(&self) -> Box<dyn Migration> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_migration_test_builder() -> Result<()> {
        let migration = MockMigration { version_num: 43 };

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::StateMachineMetadata.as_ref())
            .run_test(
                &migration,
                |_db| {
                    // Setup - nothing needed
                    Ok(())
                },
                |db| {
                    // Verify migration was applied
                    let result = db.get_cf(
                        IndexifyObjectsColumns::StateMachineMetadata.cf_db(db),
                        b"migration_test",
                    )?;

                    assert_eq!(result, Some(b"v43".to_vec()));
                    Ok(())
                },
            )?;

        Ok(())
    }
}
