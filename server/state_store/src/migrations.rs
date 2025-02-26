use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use data_model::StateMachineMetadata;
use rocksdb::{IteratorMode, ReadOptions, Transaction, TransactionDB};
use tracing::info;

use crate::{
    serializer::{JsonEncode, JsonEncoder},
    state_machine::IndexifyObjectsColumns,
};

// Note: should never be used with data model types to guarantee it works with
// different versions.
pub fn migrate(db: Arc<TransactionDB>) -> Result<StateMachineMetadata> {
    let mut sm_meta = read_sm_meta(&db).context("reading current state machine metadata")?;

    let txn = db.transaction();

    if sm_meta.db_version < 2 {
        migrate_v1_to_v2(db.clone(), &txn, &mut sm_meta).context("migrating from v1 to v2")?;
    }

    txn.commit().context("committing migrations")?;
    Ok(sm_meta)
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v1_to_v2(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    sm_meta: &mut StateMachineMetadata,
) -> Result<()> {
    let mut num_migrated_tasks: usize = 0;
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304);

    // Migrate tasks statuses
    // If the status is not set,
    //    set it to "Pending" if the outcome is not terminal.
    //    set it to "Completed" if the outcome is terminal.
    {
        let iter = db.iterator_cf_opt(
            IndexifyObjectsColumns::Tasks.cf_db(&db),
            read_options,
            IteratorMode::Start,
        );

        for kv in iter {
            let (key, val_bytes) = kv?;

            let mut task_value: serde_json::Value = serde_json::from_slice(&val_bytes)
                .map_err(|e| anyhow::anyhow!("error deserializing Tasks json bytes, {}", e))?;

            let task_obj = task_value.as_object_mut().ok_or(anyhow::anyhow!(
                "unexpected task JSON value: {:?}",
                String::from_utf8(val_bytes.to_vec()),
            ))?;

            let outcome =
                task_obj
                    .get("outcome")
                    .and_then(|v| v.as_str())
                    .ok_or(anyhow::anyhow!(
                        "unexpected task outcome JSON value: {:?}",
                        task_obj.get("outcome")
                    ))?;

            let status_undefined = match task_obj.get("status") {
                Some(serde_json::Value::String(status)) => status.is_empty(),
                Some(serde_json::Value::Null) => true,
                None => true,
                val @ _ => {
                    return Err(anyhow::anyhow!(
                        "unexpected task status JSON value: {:?}",
                        val
                    ));
                }
            };
            if status_undefined {
                num_migrated_tasks += 1;
                if outcome == "Success" || outcome == "Failure" {
                    task_obj.insert(
                        "status".to_string(),
                        serde_json::Value::String("Completed".to_string()),
                    );
                } else {
                    task_obj.insert(
                        "status".to_string(),
                        serde_json::Value::String("Pending".to_string()),
                    );
                }
            }

            let task_bytes = serde_json::to_vec(&task_value).map_err(|e| {
                anyhow::anyhow!(
                    "error serializing into json: {}, value: {:?}",
                    e,
                    task_value.clone()
                )
            })?;

            txn.put_cf(IndexifyObjectsColumns::Tasks.cf_db(&db), &key, &task_bytes)?;
        }
    }

    info!("Migrated {} tasks from v1 to v2", num_migrated_tasks);

    sm_meta.db_version = 2;
    write_sm_meta(db.clone(), &txn, &sm_meta)?;
    Ok(())
}

pub fn write_sm_meta(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    sm_meta: &StateMachineMetadata,
) -> Result<()> {
    let serialized_meta = JsonEncoder::encode(&sm_meta)?;
    txn.put_cf(
        &IndexifyObjectsColumns::StateMachineMetadata.cf_db(&db),
        b"sm_meta",
        &serialized_meta,
    )?;
    Ok(())
}

pub fn read_sm_meta(db: &TransactionDB) -> Result<StateMachineMetadata> {
    let meta = db.get_cf(
        &IndexifyObjectsColumns::StateMachineMetadata.cf_db(&db),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{Options, TransactionDBOptions};
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_migrate_v1_to_v2() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().unwrap();

        let sm_column_families = vec![
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::Tasks.as_ref(),
                Options::default(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
                Options::default(),
            ),
        ];

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = Arc::new(
            TransactionDB::open_cf_descriptors(
                &db_opts,
                &TransactionDBOptions::default(),
                path,
                sm_column_families,
            )
            .map_err(|e| anyhow!("failed to open db: {}", e))?,
        );

        // Create tasks with different outcomes and no status
        let tasks = vec![
            json!({
                "id": "task1",
                "namespace": "test_ns",
                "compute_fn_name": "test_fn",
                "compute_graph_name": "test_graph",
                "invocation_id": "test_invocation",
                "input_node_output_key": "test_input",
                "graph_version": "1",
                "outcome": "Success",
                "creation_time_ns": 0,
            }),
            json!({
                "id": "task2",
                "namespace": "test_ns",
                "compute_fn_name": "test_fn",
                "compute_graph_name": "test_graph",
                "invocation_id": "test_invocation",
                "input_node_output_key": "test_input",
                "graph_version": "1",
                "outcome": "Failure",
                "creation_time_ns": 0,
            }),
            json!({
                "id": "task3",
                "namespace": "test_ns",
                "compute_fn_name": "test_fn",
                "compute_graph_name": "test_graph",
                "invocation_id": "test_invocation",
                "input_node_output_key": "test_input",
                "graph_version": "1",
                "outcome": "Unknown",
                "creation_time_ns": 0,
            }),
        ];

        for task in tasks {
            let task_key = format!(
                "{}|{}|{}|{}|{}",
                task["namespace"].as_str().unwrap(),
                task["compute_graph_name"].as_str().unwrap(),
                task["invocation_id"].as_str().unwrap(),
                task["compute_fn_name"].as_str().unwrap(),
                task["id"].as_str().unwrap()
            );
            let task_bytes = serde_json::to_vec(&task)?;
            db.put_cf(
                &IndexifyObjectsColumns::Tasks.cf_db(&db),
                &task_key,
                &task_bytes,
            )?;
        }

        // Perform migration
        let mut sm_meta = StateMachineMetadata {
            db_version: 1,
            last_change_idx: 0,
        };
        let txn = db.transaction();
        migrate_v1_to_v2(db.clone(), &txn, &mut sm_meta)?;
        txn.commit()?;

        // Verify migration
        let task1_key = "test_ns|test_graph|test_invocation|test_fn|task1";
        let task1: serde_json::Value = serde_json::from_slice(
            &db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task1_key)?
                .unwrap(),
        )?;
        assert_eq!(task1["status"], "Completed", "task1 {}", task1);

        let task2_key = "test_ns|test_graph|test_invocation|test_fn|task2";
        let task2: serde_json::Value = serde_json::from_slice(
            &db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task2_key)?
                .unwrap(),
        )?;
        assert_eq!(task2["status"], "Completed");

        let task3_key = "test_ns|test_graph|test_invocation|test_fn|task3";
        let task3: serde_json::Value = serde_json::from_slice(
            &db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task3_key)?
                .unwrap(),
        )?;
        assert_eq!(task3["status"], "Pending");

        Ok(())
    }
}
