use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

/// Migration to handle the AllocationOutputIngestedEvent format change.
///
/// The AllocationOutputIngestedEvent struct was updated to:
/// - Remove the full `allocation: Allocation` field
/// - Add slim fields: `allocation_id`, `allocation_target`,
///   `allocation_outcome`, `execution_duration_ms`
///
/// This migration deletes unprocessed AllocationOutputsIngested state changes
/// that have the old format. These will be recreated with the new format when
/// executors report their state again.
#[derive(Clone)]
pub struct V12SlimAllocationOutputEvent;

impl Migration for V12SlimAllocationOutputEvent {
    fn version(&self) -> u64 {
        12
    }

    fn name(&self) -> &'static str {
        "Slim AllocationOutputIngestedEvent format"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_state_changes: usize = 0;
        let mut num_deleted_state_changes: usize = 0;
        let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

        // Iterate through ApplicationStateChanges to find old format
        // AllocationOutputsIngested events that have the full "allocation" field
        ctx.iterate(
            &IndexifyObjectsColumns::ApplicationStateChanges,
            |key, value| {
                num_total_state_changes += 1;

                let raw_json = String::from_utf8_lossy(value);

                // Old format has "allocation" field (full Allocation object)
                // but not "allocation_id" (the new slim field).
                if raw_json.contains("AllocationOutputsIngested") &&
                    !raw_json.contains("allocation_id")
                {
                    keys_to_delete.push(key.to_vec());
                }

                Ok(())
            },
        )?;

        for key in &keys_to_delete {
            ctx.txn.delete(
                IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                key,
            )?;
            num_deleted_state_changes += 1;
        }

        info!(
            "SlimAllocationOutputEvent migration: deleted {} old format state changes out of {} total",
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
    fn test_v12_migration_deletes_old_format() -> anyhow::Result<()> {
        let migration = V12SlimAllocationOutputEvent;

        // Old format: has "allocation" field with full Allocation object
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
                    "request_exception": null,
                    "allocation": {"id": "alloc_1", "outcome": "Unknown"}
                }
            }
        }"#;

        // New format: has "allocation_id" slim field
        let new_format_json = r#"{
            "id": 2,
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
                    "request_exception": null,
                    "allocation_id": "alloc_1",
                    "allocation_target": {"executor_id": "ex_1", "function_executor_id": "fe_1"},
                    "allocation_outcome": "Success",
                    "execution_duration_ms": 100
                }
            }
        }"#;

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::ApplicationStateChanges.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Insert old format state change
                    db.put_sync(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        &1_u64.to_be_bytes(),
                        old_format_json.as_bytes(),
                    )?;
                    // Insert new format state change
                    db.put_sync(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        &2_u64.to_be_bytes(),
                        new_format_json.as_bytes(),
                    )?;
                    Ok(())
                },
                |db| {
                    // Old format should be deleted
                    let result = db.get_sync(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        &1_u64.to_be_bytes(),
                    )?;
                    assert!(
                        result.is_none(),
                        "Old format AllocationOutputsIngested should be deleted"
                    );

                    // New format should be preserved
                    let result = db.get_sync(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        &2_u64.to_be_bytes(),
                    )?;
                    assert!(
                        result.is_some(),
                        "New format AllocationOutputsIngested should be preserved"
                    );
                    Ok(())
                },
            )?;

        Ok(())
    }
}
