use anyhow::Result;
use rocksdb::TransactionDB;

use super::contexts::{MigrationContext, PrepareContext};

/// Trait defining a database migration
pub trait Migration {
    /// The version this migration upgrades TO
    fn version(&self) -> u64;

    /// Name for logging purposes
    fn name(&self) -> &'static str;

    /// DB preparation - column family operations before transaction
    /// Default implementation simply opens the DB with existing column families
    fn prepare(&self, ctx: &PrepareContext) -> Result<TransactionDB> {
        ctx.open_db()
    }

    /// Apply migration using provided context
    fn apply(&self, ctx: &MigrationContext) -> Result<()>;

    fn box_clone(&self) -> Box<dyn Migration>;
}

#[cfg(test)]
mod tests {
    use rocksdb::Options;
    use tempfile::TempDir;

    use super::*;

    #[derive(Clone)]
    struct TestMigration {
        version_num: u64,
    }

    impl Migration for TestMigration {
        fn version(&self) -> u64 {
            self.version_num
        }

        fn name(&self) -> &'static str {
            "Test Migration"
        }

        fn prepare(&self, ctx: &PrepareContext) -> Result<TransactionDB> {
            ctx.open_db()
        }

        fn apply(&self, _ctx: &MigrationContext) -> Result<()> {
            Ok(())
        }

        fn box_clone(&self) -> Box<dyn Migration> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_migration_trait() -> Result<()> {
        let migration = TestMigration { version_num: 42 };
        assert_eq!(migration.version(), 42);
        assert_eq!(migration.name(), "Test Migration");

        // Test prepare function
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();

        // Create a test database first
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        let prep_ctx = PrepareContext {
            path: path.clone(),
            db_opts: db_opts.clone(),
        };

        let db = migration.prepare(&prep_ctx)?;

        // Make sure we can open a transaction
        let txn = db.transaction();

        // Should be able to commit without error
        txn.commit()?;

        Ok(())
    }
}
