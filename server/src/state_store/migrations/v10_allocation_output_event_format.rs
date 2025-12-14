use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

/// Migration to handle the AllocationOutputIngestedEvent format change.
///
/// The AllocationOutputIngestedEvent struct was updated to:
/// - Remove `allocation_key` field
/// - Add `allocation` field with the full Allocation object
///
/// This migration deletes unprocessed AllocationOutputsIngested state changes
/// that have the old format. These will be recreated with the new format when
/// executors report their state again.
#[derive(Clone)]
pub struct V10AllocationOutputEventFormat;

impl Migration for V10AllocationOutputEventFormat {
    fn version(&self) -> u64 {
        10
    }

    fn name(&self) -> &'static str {
        "Update AllocationOutputIngestedEvent format"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_state_changes: usize = 0;
        let mut num_deleted_state_changes: usize = 0;
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

        // Iterate through ApplicationStateChanges to find old format
        // AllocationOutputsIngested events
        ctx.iterate(
            &IndexifyObjectsColumns::ApplicationStateChanges,
            |key, value| {
                num_total_state_changes += 1;

                // Parse as raw JSON to check the format
                let raw_json = String::from_utf8_lossy(value);

                // Check if this is an AllocationOutputsIngested event with old format
                // Old format has "allocation_key" but not "allocation" field
                if raw_json.contains("AllocationOutputsIngested") &&
                    raw_json.contains("allocation_key") &&
                    !raw_json.contains(r#""allocation":"#) &&
                    !raw_json.contains(r#""allocation" :"#)
                {
                    info!("Marking old format AllocationOutputsIngested state change for deletion");
                    keys_to_delete.push(key.to_vec());
                }

                Ok(())
            },
        )?;

        // Delete the old format state changes using the transaction
        for key in &keys_to_delete {
            ctx.txn.delete(
                IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                key,
            )?;
            num_deleted_state_changes += 1;
        }

        info!(
            "AllocationOutputEvent migration: deleted {} old format state changes out of {} total",
            num_deleted_state_changes, num_total_state_changes
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::migrations::testing::MigrationTestBuilder;

    #[test]
    fn test_v10_migration_deletes_old_format() -> anyhow::Result<()> {
        let migration = V10AllocationOutputEventFormat;

        // Create an old format state change (simulated as raw JSON)
        let old_format_json = r#"{
            "id": 1,
            "namespace": "test_ns",
            "application": "app_1",
            "object_id": "test_obj",
            "created_at": 1234567890,
            "processed_at": null,
            "change_type": {
                "AllocationOutputsIngested": {
                    "namespace": "test_ns",
                    "application": "app_1",
                    "function": "fn_1",
                    "request_id": "req_1",
                    "function_call_id": "call_1",
                    "data_payload": null,
                    "graph_updates": null,
                    "allocation_key": "old_key",
                    "request_exception": null
                }
            }
        }"#;

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::ApplicationStateChanges.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Insert old format state change
                    db.put(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        1_u64.to_be_bytes(),
                        old_format_json.as_bytes(),
                    )?;
                    Ok(())
                },
                |db| {
                    // Verify the old format state change was deleted
                    let result = db.get(
                        IndexifyObjectsColumns::ApplicationStateChanges,
                        1_u64.to_be_bytes(),
                    )?;
                    assert!(
                        result.is_none(),
                        "Old format AllocationOutputsIngested should be deleted"
                    );
                    Ok(())
                },
            )?;

        Ok(())
    }
}
