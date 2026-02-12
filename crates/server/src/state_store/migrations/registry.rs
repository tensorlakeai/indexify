use anyhow::{Result, anyhow};

use super::migration_trait::Migration;
use crate::state_store::migrations::{
    v1_fake_migration::V1FakeMigration,
    v9_separate_executor_and_app_state_changes::V9SeparateExecutorAndAppStateChanges,
    v10_allocation_output_event_format::V10AllocationOutputEventFormat,
    v11_sandbox_data_model_changes::V11SandboxDataModelChanges,
    v12_slim_allocation_output_event::V12SlimAllocationOutputEvent,
    v13_reencode_json_as_bincode::V13ReencodeJsonAsBincode,
    v14_normalize_request_ctx::V14NormalizeRequestCtx,
};
// Import all migration implementations

/// Registry for all available migrations
pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    /// Create a new registry with all registered migrations
    pub fn new() -> Result<Self> {
        let mut registry = Self {
            migrations: Vec::new(),
        };

        // Register all migrations
        // Removing the migrations since we assume new state store is being used
        // Add new migrations here
        registry.register(Box::new(V1FakeMigration {}));
        registry.register(Box::new(V9SeparateExecutorAndAppStateChanges));
        registry.register(Box::new(V10AllocationOutputEventFormat));
        registry.register(Box::new(V11SandboxDataModelChanges));
        registry.register(Box::new(V12SlimAllocationOutputEvent));
        registry.register(Box::new(V13ReencodeJsonAsBincode));
        registry.register(Box::new(V14NormalizeRequestCtx));

        // Sort and validate migrations
        registry.sort_and_validate()?;

        Ok(registry)
    }

    /// Register a new migration
    fn register(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
    }

    /// Sort migrations by version and validate no duplicates
    fn sort_and_validate(&mut self) -> Result<()> {
        // Sort migrations by version
        self.migrations.sort_by_key(|m| m.version());

        // Validate no duplicate versions
        for i in 1..self.migrations.len() {
            if self.migrations[i].version() == self.migrations[i - 1].version() {
                return Err(anyhow!(
                    "Duplicate migration version {} found: {} and {}",
                    self.migrations[i].version(),
                    self.migrations[i - 1].name(),
                    self.migrations[i].name()
                ));
            }
        }
        Ok(())
    }

    /// Find migrations that should be applied from the current version
    pub fn find_migrations(&self, from_version: u64) -> Vec<Box<dyn Migration>> {
        self.migrations
            .iter()
            .map(|m| m.box_clone())
            .filter(|m| m.version() > from_version)
            .collect()
    }

    /// Get the latest migration version
    pub fn latest_version(&self) -> u64 {
        self.migrations
            .iter()
            .map(|m| m.version())
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{super::contexts::MigrationContext, *};

    #[derive(Clone)]
    struct TestMigration {
        version_num: u64,
        name_str: &'static str,
    }

    impl Migration for TestMigration {
        fn version(&self) -> u64 {
            self.version_num
        }

        fn name(&self) -> &'static str {
            self.name_str
        }

        fn apply(&self, _ctx: &MigrationContext) -> Result<()> {
            Ok(())
        }

        fn box_clone(&self) -> Box<dyn Migration> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_registry_sorts_migrations() -> Result<()> {
        let mut registry = MigrationRegistry {
            migrations: Vec::new(),
        };

        // Add migrations in random order
        registry.register(Box::new(TestMigration {
            version_num: 3,
            name_str: "Migration 3",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 1,
            name_str: "Migration 1",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 2,
            name_str: "Migration 2",
        }));

        registry.sort_and_validate()?;

        // Check migrations are sorted
        assert_eq!(registry.migrations[0].version(), 1);
        assert_eq!(registry.migrations[1].version(), 2);
        assert_eq!(registry.migrations[2].version(), 3);

        Ok(())
    }

    #[test]
    fn test_registry_detects_duplicates() {
        let mut registry = MigrationRegistry {
            migrations: Vec::new(),
        };

        // Add migrations with duplicate versions
        registry.register(Box::new(TestMigration {
            version_num: 1,
            name_str: "Migration A",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 1,
            name_str: "Migration B",
        }));

        let result = registry.sort_and_validate();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                e.to_string(),
                "Duplicate migration version 1 found: Migration A and Migration B"
            );
        }
    }

    #[test]
    fn test_find_migrations() -> Result<()> {
        let mut registry = MigrationRegistry {
            migrations: Vec::new(),
        };

        registry.register(Box::new(TestMigration {
            version_num: 1,
            name_str: "Migration 1",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 2,
            name_str: "Migration 2",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 3,
            name_str: "Migration 3",
        }));

        registry.sort_and_validate()?;

        // Find migrations from version 0
        let migrations = registry.find_migrations(0);
        assert_eq!(migrations.len(), 3);

        // Find migrations from version 1
        let migrations = registry.find_migrations(1);
        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].version(), 2);
        assert_eq!(migrations[1].version(), 3);

        // Find migrations from version 3
        let migrations = registry.find_migrations(3);
        assert_eq!(migrations.len(), 0);

        Ok(())
    }

    #[test]
    fn test_latest_version() {
        let mut registry = MigrationRegistry {
            migrations: Vec::new(),
        };

        // Empty registry
        assert_eq!(registry.latest_version(), 0);

        // Add migrations
        registry.register(Box::new(TestMigration {
            version_num: 1,
            name_str: "Migration 1",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 5,
            name_str: "Migration 5",
        }));
        registry.register(Box::new(TestMigration {
            version_num: 3,
            name_str: "Migration 3",
        }));

        assert_eq!(registry.latest_version(), 5);
    }
}
