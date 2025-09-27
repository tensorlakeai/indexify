use anyhow::Result;

use super::{contexts::MigrationContext, migration_trait::Migration};

#[derive(Clone)]
pub struct V1FakeMigration {}

impl Migration for V1FakeMigration {
    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> &'static str {
        "Add status field to tasks"
    }

    fn apply(&self, _ctx: &MigrationContext) -> Result<()> {
        // Do nothing
        _ctx.iterate(
            &crate::state_store::state_machine::IndexifyObjectsColumns::Namespaces,
            |_key, value| {
                let namespace: serde_json::Value = serde_json::from_slice(value)?;
                println!("Namespace: {:?}", namespace);
                Ok(())
            },
        )?;

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::{
        migrations::testing::MigrationTestBuilder,
        state_machine::IndexifyObjectsColumns,
    };

    #[test]
    fn test_v1_migration() -> Result<()> {
        let migration = V1FakeMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Namespaces.as_ref())
            .run_test(
                &migration,
                |_db| {
                    // Setup: Insert test tasks with different outcomes and no status field

                    Ok(())
                },
                |_db| {
                    // Verify: Check that status fields were added properly
                    Ok(())
                },
            )?;

        Ok(())
    }
}
