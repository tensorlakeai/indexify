use std::io::{BufRead, Cursor};

use anyhow::Result;
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::state_store::{
    driver::{Writer, rocksdb::RocksDBDriver},
    state_machine::IndexifyObjectsColumns,
};

/// Migration to rebuild the invocation context secondary indexes by dropping
/// and recreating the column family
#[derive(Clone)]
pub struct V9SeparateExecutorAndAppStateChanges;

impl Migration for V9SeparateExecutorAndAppStateChanges {
    fn version(&self) -> u64 {
        9
    }

    fn name(&self) -> &'static str {
        "Separate executor and app state changes"
    }

    fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        let existing_cfs = ctx.list_cfs()?;
        ctx.reopen_with_cf_operations(|db| {
            // Create fresh
            if !existing_cfs.contains(&IndexifyObjectsColumns::ExecutorStateChanges.to_string()) {
                info!("Creating executor state changes column family");
                db.create(
                    IndexifyObjectsColumns::ExecutorStateChanges,
                    &Default::default(),
                )?;
            }

            if !existing_cfs.contains(&IndexifyObjectsColumns::ApplicationStateChanges.to_string())
            {
                info!("Creating app state changes column family");
                db.create(
                    IndexifyObjectsColumns::ApplicationStateChanges,
                    &Default::default(),
                )?;
            }

            Ok(())
        })
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_state_changes: usize = 0;
        let mut num_migrated_state_changes: usize = 0;

        ctx.iterate(
            &IndexifyObjectsColumns::UnprocessedStateChanges,
            |key, value| {
                num_total_state_changes += 1;

                let parts = Cursor::new(key)
                    .split(b'|')
                    .map(|l| l.unwrap())
                    .collect::<Vec<_>>();
                if parts.len() != 2 {
                    anyhow::bail!("unexpected key format: {}", str::from_utf8(key)?);
                }

                let mut iter = parts.iter();
                let (Some(prefix), Some(id)) = (iter.next(), iter.next()) else {
                    anyhow::bail!("unexpected key format: {}", str::from_utf8(key)?);
                };

                if prefix == b"global" {
                    ctx.txn.put(
                        IndexifyObjectsColumns::ExecutorStateChanges.as_ref(),
                        id,
                        value,
                    )?;
                } else if prefix.starts_with(b"ns_") {
                    ctx.txn.put(
                        IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
                        id,
                        value,
                    )?;
                } else {
                    anyhow::bail!("unexpected key format: {}", str::from_utf8(key)?);
                }

                num_migrated_state_changes += 1;
                Ok(())
            },
        )?;

        info!(
            "Migrate state changes to new tables: {} migrated/{} total",
            num_migrated_state_changes, num_total_state_changes
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
    use crate::{
        data_model::{
            ChangeType,
            ExecutorUpsertedEvent,
            InvokeApplicationEvent,
            StateChange,
            StateChangeBuilder,
            StateChangeId,
        },
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            serializer::{StateStoreEncode, StateStoreEncoder},
        },
        utils::get_epoch_time_in_ms,
    };

    #[test]
    fn test_v9_migration() -> Result<()> {
        let migration = V9SeparateExecutorAndAppStateChanges;

        // Setup: Create several state changes
        let app_state_change = StateChangeBuilder::default()
            .namespace(Some("test_ns".to_string()))
            .application(Some("app_1".to_string()))
            .change_type(ChangeType::InvokeApplication(InvokeApplicationEvent {
                namespace: "test_ns".to_string(),
                request_id: "request_id".to_string(),
                application: "app_1".to_string(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id("request_id".to_string())
            .id(StateChangeId::new(1))
            .processed_at(None)
            .build()?;

        let executor_state_change = StateChangeBuilder::default()
            .change_type(ChangeType::ExecutorUpserted(ExecutorUpsertedEvent {
                executor_id: "executor_id".into(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id("executor_id".to_string())
            .id(StateChangeId::new(1))
            .processed_at(None)
            .namespace(None)
            .application(None)
            .build()?;

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::UnprocessedStateChanges.as_ref())
            .run_test(
                &migration,
                |db| {
                    let mut app_key = Vec::new();
                    app_key.extend(
                        format!(
                            "ns_{}|",
                            app_state_change.namespace.clone().unwrap_or_default()
                        )
                        .as_bytes(),
                    );
                    app_key.extend(app_state_change.id.as_ref().to_be_bytes());
                    db.put(
                        IndexifyObjectsColumns::UnprocessedStateChanges.as_ref(),
                        app_key,
                        StateStoreEncoder::encode(&app_state_change)?,
                    )?;

                    let mut global_key = Vec::new();
                    global_key.extend("global|".as_bytes());
                    global_key.extend(executor_state_change.id.as_ref().to_be_bytes());
                    db.put(
                        IndexifyObjectsColumns::UnprocessedStateChanges.as_ref(),
                        global_key,
                        StateStoreEncoder::encode(&executor_state_change)?,
                    )?;

                    Ok(())
                },
                |db| {
                    // Verify: State changes are stored in the new tables

                    let change = db
                        .get(
                            IndexifyObjectsColumns::ApplicationStateChanges,
                            1_u64.to_be_bytes(),
                        )
                        .unwrap()
                        .expect("Failed to get application state change");
                    let app_state_change: StateChange = StateStoreEncoder::decode(&change)?;
                    assert_eq!(Some("test_ns"), app_state_change.namespace.as_deref());
                    assert_eq!(Some("app_1"), app_state_change.application.as_deref());
                    assert_eq!("request_id", app_state_change.object_id);

                    let change = db
                        .get(
                            IndexifyObjectsColumns::ExecutorStateChanges,
                            1_u64.to_be_bytes(),
                        )
                        .unwrap()
                        .expect("Failed to get executor state change");
                    let executor_state_change: StateChange = StateStoreEncoder::decode(&change)?;
                    assert_eq!(None, executor_state_change.namespace);
                    assert_eq!(None, executor_state_change.application);
                    assert_eq!("executor_id", executor_state_change.object_id);

                    Ok(())
                },
            )?;

        Ok(())
    }
}
