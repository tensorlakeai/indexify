use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use crate::{
    data_model::StateMachineMetadata,
    state_store::{
        driver::{Reader, Transaction, Writer, rocksdb::RocksDBDriver},
        migrations::{
            contexts::{MigrationContext, PrepareContext},
            registry::MigrationRegistry,
        },
        serializer::{JsonEncode, JsonEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Main function to run all necessary migrations on a database at the given
/// path
pub fn run(path: &Path) -> Result<StateMachineMetadata> {
    // Initialize prepare context
    let prepare_ctx = PrepareContext::new(path.to_path_buf());

    // Initialize registry
    let registry = MigrationRegistry::new()?;
    let latest_version = registry.latest_version();

    // Check if DB exists
    let db = match prepare_ctx.open_db() {
        Ok(db) => db,
        Err(e) if e.to_string().contains("No such file or directory") => {
            // New DB, return default metadata
            info!(
                "No database found. Initializing at version {}",
                latest_version
            );
            return Ok(StateMachineMetadata {
                db_version: latest_version,
                last_change_idx: 0,
            });
        }
        Err(e) => return Err(anyhow::anyhow!("Error opening database: {e:?}")),
    };

    // Read current metadata
    let mut sm_meta = read_sm_meta(&db)?;
    drop(db); // Close DB before migrations

    // Find applicable migrations
    let migrations = registry.find_migrations(sm_meta.db_version);

    // No migrations needed
    if migrations.is_empty() {
        info!(
            "Database already at version {}. No migrations needed.",
            sm_meta.db_version
        );
        return Ok(sm_meta);
    }

    info!(
        "Starting migrations from v{} to v{}",
        sm_meta.db_version, latest_version
    );

    // Execute each migration in sequence
    for migration in migrations {
        let from_version = sm_meta.db_version;
        let to_version = migration.version();

        info!(
            "Running migration {}: v{} → v{}",
            migration.name(),
            from_version,
            to_version
        );

        // Each migration prepares the DB as needed
        let db = migration
            .prepare(&prepare_ctx)
            .with_context(|| format!("Preparing DB for migration to v{to_version}"))?;

        // Apply migration in a transaction
        let txn = db.transaction();

        // Create migration context
        let migration_ctx = MigrationContext::new(&db, &txn);

        // Apply the migration
        migration
            .apply(&migration_ctx)
            .with_context(|| format!("Applying migration to v{to_version}"))?;

        // Update metadata in the same transaction
        sm_meta.db_version = to_version;
        write_sm_meta(&txn, &sm_meta)?;

        info!("Committing migration to v{}", to_version);
        txn.commit()
            .with_context(|| format!("Committing migration to v{to_version}"))?;

        // Close DB after each migration to ensure clean state
        drop(db);
    }

    info!(
        "Completed all migrations. DB now at version {}",
        sm_meta.db_version
    );
    Ok(sm_meta)
}

/// Read state machine metadata from the database
pub fn read_sm_meta(db: &RocksDBDriver) -> Result<StateMachineMetadata> {
    let meta = db.get(
        IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
        b"sm_meta",
    )?;
    match meta {
        Some(meta) => Ok(JsonEncoder::decode(&meta)?),
        None => Ok(StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
        }),
    }
}

/// Write state machine metadata to the database
pub fn write_sm_meta(txn: &Transaction, sm_meta: &StateMachineMetadata) -> Result<()> {
    let serialized_meta = JsonEncoder::encode(sm_meta)?;
    txn.put(
        IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
        b"sm_meta",
        &serialized_meta,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyDescriptor, Options};
    use strum::IntoEnumIterator;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        metrics::StateStoreMetrics,
        state_store::{self, migrations::migration_trait::Migration},
    };

    #[derive(Clone)]
    struct MockMigration {
        version: u64,
        name: &'static str,
    }

    impl Migration for MockMigration {
        fn version(&self) -> u64 {
            self.version
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
            // Simple mock - just open DB
            ctx.open_db()
        }

        fn apply(&self, _ctx: &MigrationContext) -> Result<()> {
            // No-op for test
            Ok(())
        }

        fn box_clone(&self) -> Box<dyn Migration> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_migration_new_db() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        // Create a mock migration
        let mock_migration = MockMigration {
            version: 1,
            name: "MockMigration",
        };

        // Use the mock migration (e.g., log its name)
        info!("Testing with migration: {}", mock_migration.name());

        // Run migrations on non-existent DB
        let sm_meta = run(path)?;

        // Check migration resulted in latest version
        assert_eq!(
            sm_meta.db_version,
            MigrationRegistry::new()?.latest_version()
        );

        Ok(())
    }

    #[test]
    fn test_migration_existing_db() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        // Create DB with initial metadata
        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        let metrics = Arc::new(StateStoreMetrics::new());
        let db = state_store::open_database(path.to_path_buf(), sm_column_families, metrics)?;

        // Set initial version to 1
        let txn = db.transaction();
        let initial_meta = StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
        };
        write_sm_meta(&txn, &initial_meta)?;
        txn.commit()?;
        drop(db);

        // Run migrations
        let sm_meta = run(path)?;

        // Check migration resulted in latest version
        assert_eq!(
            sm_meta.db_version,
            MigrationRegistry::new()?.latest_version()
        );

        Ok(())
    }
}
