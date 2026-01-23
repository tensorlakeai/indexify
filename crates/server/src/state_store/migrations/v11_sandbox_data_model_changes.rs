use anyhow::Result;
use serde_json::Value;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

/// Migration for sandbox feature data model changes.
///
/// This migration handles the following schema changes:
///
/// 1. **Function.fn_name removal**: The `fn_name` field has been removed from the Function struct.
///    Old data may contain this field which needs to be stripped to avoid deserialization issues
///    if strict mode is ever enabled.
///
/// 2. **Application/ApplicationVersion.code and entrypoint**: Changed from required to Optional.
///    These are backward compatible with `#[serde(default)]` - existing data deserializes correctly.
///
/// 3. **Function.min_containers and max_containers**: New optional fields with defaults.
///    Backward compatible - old data will have these as None.
///
/// 4. **Type renames** (FunctionExecutorId → ContainerId, etc.): These don't affect serialized
///    JSON format as they're newtype wrappers around String.
///
/// 5. **New Sandboxes column family**: Automatically created by RocksDB, no migration needed.
#[derive(Clone)]
pub struct V11SandboxDataModelChanges;

impl Migration for V11SandboxDataModelChanges {
    fn version(&self) -> u64 {
        11
    }

    fn name(&self) -> &'static str {
        "Sandbox data model changes - remove Function.fn_name field"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut apps_processed = 0;
        let mut apps_updated = 0;
        let mut versions_processed = 0;
        let mut versions_updated = 0;

        // Process Applications column family
        let mut app_updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        ctx.iterate(&IndexifyObjectsColumns::Applications, |key, value| {
            apps_processed += 1;

            if let Ok(mut json) = serde_json::from_slice::<Value>(value) {
                if remove_fn_name_from_functions(&mut json) {
                    let updated = serde_json::to_vec(&json)?;
                    app_updates.push((key.to_vec(), updated));
                }
            }
            Ok(())
        })?;

        for (key, value) in &app_updates {
            ctx.txn
                .put(IndexifyObjectsColumns::Applications.as_ref(), key, value)?;
            apps_updated += 1;
        }

        // Process ApplicationVersions column family
        let mut version_updates: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        ctx.iterate(&IndexifyObjectsColumns::ApplicationVersions, |key, value| {
            versions_processed += 1;

            if let Ok(mut json) = serde_json::from_slice::<Value>(value) {
                if remove_fn_name_from_functions(&mut json) {
                    let updated = serde_json::to_vec(&json)?;
                    version_updates.push((key.to_vec(), updated));
                }
            }
            Ok(())
        })?;

        for (key, value) in &version_updates {
            ctx.txn.put(
                IndexifyObjectsColumns::ApplicationVersions.as_ref(),
                key,
                value,
            )?;
            versions_updated += 1;
        }

        info!(
            "V11 migration complete: Applications {}/{} updated, ApplicationVersions {}/{} updated",
            apps_updated, apps_processed, versions_updated, versions_processed
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

/// Remove the deprecated `fn_name` field from all functions in an Application or ApplicationVersion JSON.
/// Returns true if any modifications were made.
fn remove_fn_name_from_functions(json: &mut Value) -> bool {
    let mut modified = false;

    if let Some(functions) = json.get_mut("functions") {
        if let Some(functions_map) = functions.as_object_mut() {
            for (_fn_key, fn_value) in functions_map.iter_mut() {
                if let Some(fn_obj) = fn_value.as_object_mut() {
                    if fn_obj.remove("fn_name").is_some() {
                        modified = true;
                    }
                }
            }
        }
    }

    modified
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::state_store::migrations::testing::MigrationTestBuilder;

    #[test]
    fn test_remove_fn_name_from_functions() {
        let mut json = json!({
            "namespace": "test",
            "name": "app1",
            "functions": {
                "fn_a": {
                    "name": "fn_a",
                    "fn_name": "deprecated_field",
                    "description": "test function"
                },
                "fn_b": {
                    "name": "fn_b",
                    "fn_name": "another_deprecated",
                    "description": "another function"
                }
            }
        });

        let modified = remove_fn_name_from_functions(&mut json);
        assert!(modified, "Should report modification");

        // Verify fn_name was removed
        let functions = json.get("functions").unwrap().as_object().unwrap();
        for (_key, fn_val) in functions.iter() {
            assert!(
                fn_val.get("fn_name").is_none(),
                "fn_name should be removed"
            );
            // Other fields should be preserved
            assert!(fn_val.get("name").is_some());
            assert!(fn_val.get("description").is_some());
        }
    }

    #[test]
    fn test_no_modification_when_no_fn_name() {
        let mut json = json!({
            "namespace": "test",
            "name": "app1",
            "functions": {
                "fn_a": {
                    "name": "fn_a",
                    "description": "test function"
                }
            }
        });

        let modified = remove_fn_name_from_functions(&mut json);
        assert!(!modified, "Should not report modification when no fn_name exists");
    }

    #[test]
    fn test_v11_migration_removes_fn_name() -> Result<()> {
        let migration = V11SandboxDataModelChanges;

        let old_app_json = json!({
            "namespace": "test_ns",
            "name": "app_1",
            "version": "1",
            "functions": {
                "fn_a": {
                    "name": "fn_a",
                    "fn_name": "old_fn_name_value",
                    "description": "test"
                }
            },
            "code": {
                "id": "code_id",
                "path": "/path/to/code",
                "size": 100,
                "sha256_hash": "hash123",
                "encoding": "application/octet-stream",
                "metadata_size": 0,
                "offset": 0
            },
            "entrypoint": {
                "function_name": "fn_a",
                "input_serializer": "json",
                "output_serializer": "json",
                "output_type_hints_base64": ""
            },
            "created_at": 12345
        });

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Applications.as_ref())
            .with_column_family(IndexifyObjectsColumns::ApplicationVersions.as_ref())
            .run_test(
                &migration,
                |db| {
                    db.put(
                        IndexifyObjectsColumns::Applications.as_ref(),
                        b"test_ns|app_1",
                        serde_json::to_vec(&old_app_json)?,
                    )?;
                    Ok(())
                },
                |db| {
                    let result = db
                        .get(IndexifyObjectsColumns::Applications, b"test_ns|app_1")?
                        .expect("Application should exist");

                    let json: Value = serde_json::from_slice(&result)?;
                    let functions = json.get("functions").unwrap().as_object().unwrap();
                    let fn_a = functions.get("fn_a").unwrap();

                    // fn_name should be removed
                    assert!(fn_a.get("fn_name").is_none(), "fn_name should be removed");
                    // Other fields preserved
                    assert_eq!(fn_a.get("name").unwrap(), "fn_a");
                    assert_eq!(fn_a.get("description").unwrap(), "test");

                    Ok(())
                },
            )?;

        Ok(())
    }
}
