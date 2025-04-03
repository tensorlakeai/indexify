use std::path::Path;

use anyhow::{Context, Result};
use data_model::{GraphInvocationCtx, StateMachineMetadata};
use rocksdb::{
    IteratorMode,
    Options,
    ReadOptions,
    Transaction,
    TransactionDB,
    TransactionDBOptions,
    DB,
};
use tracing::info;

use crate::{
    serializer::{JsonEncode, JsonEncoder},
    state_machine::IndexifyObjectsColumns,
};

const SERVER_DB_VERSION: u64 = 7;

// Note: should never be used with data model types to guarantee it works with
// different versions.
pub fn migrate(path: &Path) -> Result<StateMachineMetadata> {
    let mut db_opts = Options::default();
    // don't create missing column families during migration
    db_opts.create_missing_column_families(false);
    db_opts.create_if_missing(true);

    // Fetch existing column families
    let existing_cfs = match DB::list_cf(&db_opts, &path) {
        Ok(cfs) => cfs,
        Err(e) if e.kind() == rocksdb::ErrorKind::IOError => {
            // No migration needed, just return the default metadata.
            info!(
                "no state store migration needed, new state at version {}",
                SERVER_DB_VERSION
            );
            return Ok(StateMachineMetadata {
                db_version: SERVER_DB_VERSION,
                last_change_idx: 0,
            });
        }
        Err(e) => return Err(anyhow::anyhow!("listing column families: {}", e)),
    };

    // Open the database with the existing column families
    let mut db = TransactionDB::open_cf(
        &db_opts,
        &TransactionDBOptions::default(),
        path,
        &existing_cfs,
    )
    .map_err(|e| anyhow::anyhow!("failed to open db for migration: {}", e))?;

    let mut sm_meta = read_sm_meta(&db).context("reading current state machine metadata")?;
    let current_db_version = sm_meta.db_version;

    if current_db_version == SERVER_DB_VERSION {
        info!(
            "no state store migration needed, already at version {}",
            SERVER_DB_VERSION
        );
        return Ok(sm_meta);
    }

    // Drop column families before starting migrations transaction
    // Dropping column families cannot be done in a transaction and requires
    // borrowing with mut.
    drop_unused_cfs(&existing_cfs, &mut db).context("dropping column families before migration")?;

    info!(
        "starting state store migration from version {} to {}",
        current_db_version, SERVER_DB_VERSION
    );

    let txn = db.transaction();

    // handle empty DB
    if sm_meta.db_version == 0 {
        sm_meta.db_version = SERVER_DB_VERSION;
    }

    // migrations
    {
        if sm_meta.db_version == 1 {
            sm_meta.db_version += 1;
            migrate_v1_to_v2(&db, &txn).context("migrating from v1 to v2")?;
        }

        if sm_meta.db_version == 2 {
            sm_meta.db_version += 1;
            migrate_v2_to_v3(&db, &txn).context("migrating from v2 to v3")?;
        }

        if sm_meta.db_version == 3 {
            sm_meta.db_version += 1;
            migrate_v3_to_v4(&db, &txn).context("migrating from v3 to v4")?;
        }

        if sm_meta.db_version == 4 {
            sm_meta.db_version += 1;
            // Bumping for new cf to drop: Executors
        }

        if sm_meta.db_version == 5 {
            sm_meta.db_version += 1;
            migrate_v5_to_v6_migrate_allocations(&db, &txn).context("migrating from v5 to v6")?;
            migrate_v5_to_v6_clean_orphaned_tasks(&db, &txn).context("migrating from v5 to v6")?;
        }

        if sm_meta.db_version == 6 {
            sm_meta.db_version += 1;
            migrate_v6_to_v7_reallocate_allocated_tasks(&db, &txn)
                .context("migrating from v6 to v7")?;
        }

        // add new migrations before this line and increment SERVER_DB_VERSION
    }

    // assert we migrated all the way to the expected server version
    if sm_meta.db_version != SERVER_DB_VERSION {
        return Err(anyhow::anyhow!(
            "migration did not migrate to the expected server version: {} != {}",
            sm_meta.db_version,
            SERVER_DB_VERSION
        ));
    }

    // saving db version
    write_sm_meta(&db, &txn, &sm_meta)?;

    info!("committing migration");
    txn.commit().context("committing migration")?;
    info!("completed state store migration");

    Ok(sm_meta)
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v1_to_v2(db: &TransactionDB, txn: &Transaction<TransactionDB>) -> Result<()> {
    let mut num_total_tasks: usize = 0;
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
            num_total_tasks += 1;
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
    }

    info!(
        "Migrated {}/{} tasks from v1 to v2",
        num_migrated_tasks, num_total_tasks
    );

    Ok(())
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v2_to_v3(db: &TransactionDB, txn: &Transaction<TransactionDB>) -> Result<()> {
    let mut num_total_invocation_ctx: usize = 0;
    let mut num_migrated_invocation_ctx: usize = 0;
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304);

    // Migrate graph invocation ctx date from invocation payload data
    // by using the payload created_at
    {
        let iter = db.iterator_cf_opt(
            IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            read_options,
            IteratorMode::Start,
        );

        for kv in iter {
            num_total_invocation_ctx += 1;
            let (key, val_bytes) = kv?;
            let key_str = String::from_utf8_lossy(&key);

            let mut invocation_ctx: serde_json::Value = serde_json::from_slice(&val_bytes)
                .map_err(|e| {
                    anyhow::anyhow!("error deserializing InvocationCtx json bytes, {}", e)
                })?;

            let new_invocation_ctx = invocation_ctx.as_object_mut().ok_or_else(|| {
                anyhow::anyhow!("unexpected invocation ctx JSON value {}", key_str)
            })?;

            let invocation_bytes = db
                .get_cf(&IndexifyObjectsColumns::GraphInvocations.cf_db(&db), &key)?
                .ok_or_else(|| {
                    anyhow::anyhow!("invocation not found for invocation ctx: {}", key_str)
                })?;

            let invocation: serde_json::Value = serde_json::from_slice(&invocation_bytes)?;

            let created_at = invocation
                .get("created_at")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| {
                    anyhow::anyhow!("created_at not found in invocation: {}", key_str)
                })?;

            new_invocation_ctx.insert(
                "created_at".to_string(),
                serde_json::Value::from(created_at),
            );

            let new_invocation_ctx_bytes = serde_json::to_vec(&new_invocation_ctx)?;

            txn.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &key,
                &new_invocation_ctx_bytes,
            )?;

            num_migrated_invocation_ctx += 1;
        }
    }

    info!(
        "Migrated {}/{} invocation context from v2 to v3",
        num_migrated_invocation_ctx, num_total_invocation_ctx
    );

    Ok(())
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v3_to_v4(db: &TransactionDB, txn: &Transaction<TransactionDB>) -> Result<()> {
    let mut num_total_invocation_ctx: usize = 0;
    let mut num_migrated_invocation_ctx: usize = 0;

    {
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);

        let iter = db.iterator_cf_opt(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            read_options,
            IteratorMode::Start,
        );

        for kv in iter {
            num_total_invocation_ctx += 1;
            let (_key, value) = kv?;

            let graph_invocation_ctx = JsonEncoder::decode::<GraphInvocationCtx>(&value)?;

            let secondary_index_key =
                GraphInvocationCtx::secondary_index_key(&graph_invocation_ctx);

            txn.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.cf_db(&db),
                &secondary_index_key,
                &[],
            )?;

            num_migrated_invocation_ctx += 1;
        }
    }

    info!(
        "Migrated {}/{} invocation context secondary indexes from v3 to v4",
        num_migrated_invocation_ctx, num_total_invocation_ctx
    );
    Ok(())
}

fn get_string_val(val: &serde_json::Value, key: &str) -> Result<String> {
    val.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or(anyhow::anyhow!("missing {} in json value", key))
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v5_to_v6_migrate_allocations(
    db: &TransactionDB,
    txn: &Transaction<TransactionDB>,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304); // 10MB

    let iter = db.iterator_cf_opt(
        IndexifyObjectsColumns::Allocations.cf_db(&db),
        read_options,
        IteratorMode::Start,
    );

    let mut num_migrated_allocations = 0;
    let mut num_deleted_allocations = 0;
    let mut num_total_allocations = 0;

    for kv in iter {
        num_total_allocations += 1;
        let (key, val_bytes) = kv?;
        let allocation: serde_json::Value = serde_json::from_slice(&val_bytes)
            .map_err(|e| anyhow::anyhow!("error deserializing Allocations json bytes, {:#?}", e))?;

        let namespace = get_string_val(&allocation, "namespace")?;
        let compute_graph = get_string_val(&allocation, "compute_graph")?;
        let invocation_id = get_string_val(&allocation, "invocation_id")?;
        let new_allocation_key = format!(
            "{}|{}|{}|{}|{}|{}",
            namespace,
            compute_graph,
            invocation_id,
            get_string_val(&allocation, "compute_fn")?,
            get_string_val(&allocation, "task_id")?,
            get_string_val(&allocation, "executor_id")?
        );

        // Delete the old allocation using id as key
        txn.delete_cf(IndexifyObjectsColumns::Allocations.cf_db(&db), &key)?;

        // Check if the allocation is orphaned by ensuring it has a graph invocation and
        // ctx
        if db
            .get_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                format!("{}|{}|{}", namespace, compute_graph, invocation_id),
            )?
            .is_some()
        {
            // Re-put the allocation with the new key
            txn.put_cf(
                IndexifyObjectsColumns::Allocations.cf_db(&db),
                new_allocation_key,
                &val_bytes,
            )?;
            num_migrated_allocations += 1;
        } else {
            num_deleted_allocations += 1;
        }
    }

    info!(
        "Migrated {} allocations and deleted {} orphaned allocations from {} total allocations",
        num_migrated_allocations, num_deleted_allocations, num_total_allocations
    );

    Ok(())
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v5_to_v6_clean_orphaned_tasks(
    db: &TransactionDB,
    txn: &Transaction<TransactionDB>,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304); // 10MB

    let iter = db.iterator_cf_opt(
        IndexifyObjectsColumns::Tasks.cf_db(&db),
        read_options,
        IteratorMode::Start,
    );

    let mut num_deleted_tasks = 0;
    let mut num_total_tasks = 0;

    for kv in iter {
        num_total_tasks += 1;
        let (key, val_bytes) = kv?;
        let task: serde_json::Value = serde_json::from_slice(&val_bytes)
            .map_err(|e| anyhow::anyhow!("error deserializing Tasks json bytes, {:#?}", e))?;

        let namespace = get_string_val(&task, "namespace")?;
        let compute_graph = get_string_val(&task, "compute_graph_name")?;
        let invocation_id = get_string_val(&task, "invocation_id")?;

        // Check if the task is orphaned by ensuring it has a graph invocation
        if db
            .get_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                format!("{}|{}|{}", namespace, compute_graph, invocation_id),
            )?
            .is_none()
        {
            // Delete the orphaned task
            txn.delete_cf(IndexifyObjectsColumns::Tasks.cf_db(&db), &key)?;
            num_deleted_tasks += 1;
        }
    }

    info!(
        "Deleted {} orphaned tasks out of {}",
        num_deleted_tasks, num_total_tasks
    );

    Ok(())
}

#[tracing::instrument(skip(db, txn))]
pub fn migrate_v6_to_v7_reallocate_allocated_tasks(
    db: &TransactionDB,
    txn: &Transaction<TransactionDB>,
) -> Result<()> {
    // Set up read options with reasonable readahead size
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304); // 10MB

    // Iterate through all Allocations
    let iter = db.iterator_cf_opt(
        IndexifyObjectsColumns::Allocations.cf_db(&db),
        read_options,
        IteratorMode::Start,
    );

    let mut num_total_allocations = 0;
    let mut num_deleted_allocations = 0;
    let mut num_updated_tasks = 0;

    for kv in iter {
        num_total_allocations += 1;
        let (key, val_bytes) = kv?;
        let allocation: serde_json::Value = serde_json::from_slice(&val_bytes)
            .map_err(|e| anyhow::anyhow!("error deserializing Allocations json bytes, {:#?}", e))?;

        // Extract task information from the allocation
        let namespace = get_string_val(&allocation, "namespace")?;
        let compute_graph = get_string_val(&allocation, "compute_graph")?;
        let invocation_id = get_string_val(&allocation, "invocation_id")?;
        let compute_fn = get_string_val(&allocation, "compute_fn")?;
        let task_id = get_string_val(&allocation, "task_id")?;

        // Construct the task key
        let task_key = format!(
            "{}|{}|{}|{}|{}",
            namespace, compute_graph, invocation_id, compute_fn, task_id
        );

        // Get the task
        if let Some(task_bytes) = db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task_key)? {
            let mut task: serde_json::Value = serde_json::from_slice(&task_bytes)
                .map_err(|e| anyhow::anyhow!("error deserializing Task json bytes, {:#?}", e))?;

            // Update task status to Pending
            if let Some(task_obj) = task.as_object_mut() {
                task_obj.insert(
                    "status".to_string(),
                    serde_json::Value::String("Pending".to_string()),
                );

                // Update the task in the database
                let updated_task_bytes = serde_json::to_vec(&task)
                    .map_err(|e| anyhow::anyhow!("error serializing task: {:#?}", e))?;
                txn.put_cf(
                    &IndexifyObjectsColumns::Tasks.cf_db(&db),
                    &task_key,
                    &updated_task_bytes,
                )?;
                num_updated_tasks += 1;
            }
        }

        // Delete the allocation
        txn.delete_cf(IndexifyObjectsColumns::Allocations.cf_db(&db), &key)?;
        num_deleted_allocations += 1;
    }

    info!(
        "Dropped {} allocations and updated {} tasks out of {} total allocations",
        num_deleted_allocations, num_updated_tasks, num_total_allocations
    );

    Ok(())
}

pub fn write_sm_meta(
    db: &TransactionDB,
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

#[tracing::instrument(skip(db, existing_cfs))]
pub fn drop_unused_cfs(existing_cfs: &Vec<String>, db: &mut TransactionDB) -> Result<()> {
    let cfs_to_drop = vec!["Executors", "UnallocatedTasks", "TaskAllocations"];

    for cf_name in cfs_to_drop {
        if existing_cfs.contains(&cf_name.to_string()) {
            info!("Dropping unused {} column family", cf_name);
            db.drop_cf(cf_name)?;
        }
    }

    Ok(())
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
        let db = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            path,
            sm_column_families,
        )
        .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?;

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
        let txn = db.transaction();
        migrate_v1_to_v2(&db, &txn)?;
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

    #[tokio::test]
    async fn test_migrate_v2_to_v3() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().unwrap();

        let sm_column_families = vec![
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
                Options::default(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::GraphInvocations.as_ref(),
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
        let db = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            path,
            sm_column_families,
        )
        .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?;

        // Create invocation payloads and invocation contexts without created_at
        let invocations = vec![
            json!({
                "id": "invocation1",
                "namespace": "test_ns",
                "compute_graph_name": "test_graph",
                "payload": {
                    "path": "path1",
                    "size": 123,
                    "sha256_hash": "hash1"
                },
                "created_at": 1000,
                "encoding": "application/json"
            }),
            json!({
                "id": "invocation2",
                "namespace": "test_ns",
                "compute_graph_name": "test_graph",
                "payload": {
                    "path": "path2",
                    "size": 456,
                    "sha256_hash": "hash2"
                },
                "created_at": 2000,
                "encoding": "application/json"
            }),
        ];

        let invocation_ctxs = vec![
            json!({
                "namespace": "test_ns",
                "compute_graph_name": "test_graph",
                "graph_version": "1",
                "invocation_id": "invocation1",
                "completed": false,
                "outcome": "Undefined",
                "outstanding_tasks": 0,
                "fn_task_analytics": {}
            }),
            json!({
                "namespace": "test_ns",
                "compute_graph_name": "test_graph",
                "graph_version": "1",
                "invocation_id": "invocation2",
                "completed": false,
                "outcome": "Undefined",
                "outstanding_tasks": 0,
                "fn_task_analytics": {}
            }),
        ];

        for invocation in invocations {
            let key = format!(
                "{}|{}|{}",
                invocation["namespace"].as_str().unwrap(),
                invocation["compute_graph_name"].as_str().unwrap(),
                invocation["id"].as_str().unwrap()
            );
            let bytes = serde_json::to_vec(&invocation)?;
            db.put_cf(
                &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
                &key,
                &bytes,
            )?;
        }

        for ctx in invocation_ctxs {
            let key = format!(
                "{}|{}|{}",
                ctx["namespace"].as_str().unwrap(),
                ctx["compute_graph_name"].as_str().unwrap(),
                ctx["invocation_id"].as_str().unwrap()
            );
            let bytes = serde_json::to_vec(&ctx)?;
            db.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &key,
                &bytes,
            )?;
        }

        // Perform migration
        let txn = db.transaction();
        migrate_v2_to_v3(&db, &txn)?;
        txn.commit()?;

        // Verify migration
        let ctx1_key = "test_ns|test_graph|invocation1";
        let ctx1: serde_json::Value = serde_json::from_slice(
            &db.get_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &ctx1_key,
            )?
            .unwrap(),
        )?;
        assert_eq!(ctx1["created_at"], 1000);

        let ctx2_key = "test_ns|test_graph|invocation2";
        let ctx2: serde_json::Value = serde_json::from_slice(
            &db.get_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &ctx2_key,
            )?
            .unwrap(),
        )?;
        assert_eq!(ctx2["created_at"], 2000);

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_logic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path();

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = Arc::new(
            TransactionDB::open_cf_descriptors(
                &db_opts,
                &TransactionDBOptions::default(),
                path,
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
                    Options::default(),
                )],
            )
            .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?,
        );

        // Test case where the database is already at the latest version
        let sm_meta = StateMachineMetadata {
            db_version: SERVER_DB_VERSION,
            last_change_idx: 0,
        };
        let txn = db.transaction();
        write_sm_meta(&db, &txn, &sm_meta)?;
        txn.commit()?;

        drop(db);

        let sm_meta = migrate(path)?;
        assert_eq!(sm_meta.db_version, SERVER_DB_VERSION);

        // Test case where the database is empty
        let sm_meta = StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
        };

        let db = Arc::new(
            TransactionDB::open_cf_descriptors(
                &db_opts,
                &TransactionDBOptions::default(),
                path,
                vec![rocksdb::ColumnFamilyDescriptor::new(
                    IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
                    Options::default(),
                )],
            )
            .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?,
        );
        let txn = db.transaction();
        write_sm_meta(&db, &txn, &sm_meta)?;
        txn.commit()?;
        drop(db);

        let sm_meta = migrate(path)?;
        assert_eq!(sm_meta.db_version, SERVER_DB_VERSION);

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_v5_to_v6() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().unwrap();

        let sm_column_families = vec![
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::Allocations.as_ref(),
                Options::default(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
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
        let db = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            path,
            sm_column_families,
        )
        .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?;

        // Create allocations with different invocation statuses
        let allocations = vec![
            json!({
                "id": "allocation1",
                "namespace": "test_ns",
                "compute_graph": "test_graph",
                "invocation_id": "invocation1",
                "compute_fn": "test_fn",
                "task_id": "task1",
                "executor_id": "executor1",
            }),
            json!({
                "id": "allocation2",
                "namespace": "test_ns",
                "compute_graph": "test_graph",
                "invocation_id": "invocation2",
                "compute_fn": "test_fn",
                "task_id": "task2",
                "executor_id": "executor2",
            }),
        ];

        for allocation in allocations {
            let allocation_key = allocation["id"].as_str().unwrap();
            let allocation_bytes = serde_json::to_vec(&allocation)?;
            db.put_cf(
                &IndexifyObjectsColumns::Allocations.cf_db(&db),
                allocation_key,
                &allocation_bytes,
            )?;
        }

        // Create invocation contexts
        let invocation_ctxs = vec![json!({
            "namespace": "test_ns",
            "compute_graph_name": "test_graph",
            "graph_version": "1",
            "invocation_id": "invocation1",
            "completed": false,
            "outcome": "Undefined",
            "outstanding_tasks": 0,
            "fn_task_analytics": {}
        })];

        for ctx in invocation_ctxs {
            let key = format!(
                "{}|{}|{}",
                ctx["namespace"].as_str().unwrap(),
                ctx["compute_graph_name"].as_str().unwrap(),
                ctx["invocation_id"].as_str().unwrap()
            );
            let bytes = serde_json::to_vec(&ctx)?;
            db.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &key,
                &bytes,
            )?;
        }

        // Perform migration
        let txn = db.transaction();
        migrate_v5_to_v6_migrate_allocations(&db, &txn)?;
        txn.commit()?;

        let all_allocations = &db
            .full_iterator_cf(
                &IndexifyObjectsColumns::Allocations.cf_db(&db),
                IteratorMode::Start,
            )
            .collect::<Vec<_>>();
        assert_eq!(
            all_allocations.len(),
            1,
            "allocations: {:#?}",
            all_allocations
        );

        // Verify migration
        let allocation1_key = "test_ns|test_graph|invocation1|test_fn|task1|executor1";
        let allocation1: serde_json::Value = serde_json::from_slice(
            &db.get_cf(
                &IndexifyObjectsColumns::Allocations.cf_db(&db),
                allocation1_key,
            )?
            .unwrap(),
        )?;
        assert_eq!(allocation1["invocation_id"], "invocation1");

        Ok(())
    }

    #[tokio::test]
    async fn test_migrate_v5_to_v6_clean_orphaned_tasks() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().unwrap();

        let sm_column_families = vec![
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::Tasks.as_ref(),
                Options::default(),
            ),
            rocksdb::ColumnFamilyDescriptor::new(
                IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
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
        let db = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            path,
            sm_column_families,
        )
        .map_err(|e| anyhow::anyhow!("failed to open db: {}", e))?;

        // Create tasks with different invocation statuses
        let tasks = vec![
            json!({
                "id": "task1",
                "namespace": "test_ns",
                "compute_fn_name": "test_fn",
                "compute_graph_name": "test_graph",
                "invocation_id": "invocation1",
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
                "invocation_id": "invocation2",
                "input_node_output_key": "test_input",
                "graph_version": "1",
                "outcome": "Failure",
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

        // Create invocation contexts
        let invocation_ctxs = vec![json!({
            "namespace": "test_ns",
            "compute_graph_name": "test_graph",
            "graph_version": "1",
            "invocation_id": "invocation1",
            "completed": false,
            "outcome": "Undefined",
            "outstanding_tasks": 0,
            "fn_task_analytics": {}
        })];

        for ctx in invocation_ctxs {
            let key = format!(
                "{}|{}|{}",
                ctx["namespace"].as_str().unwrap(),
                ctx["compute_graph_name"].as_str().unwrap(),
                ctx["invocation_id"].as_str().unwrap()
            );
            let bytes = serde_json::to_vec(&ctx)?;
            db.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                &key,
                &bytes,
            )?;
        }

        // Perform migration
        let txn = db.transaction();
        migrate_v5_to_v6_clean_orphaned_tasks(&db, &txn)?;
        txn.commit()?;

        // Verify migration

        let all_tasks = &db
            .full_iterator_cf(
                &IndexifyObjectsColumns::Tasks.cf_db(&db),
                IteratorMode::Start,
            )
            .collect::<Vec<_>>();
        assert_eq!(all_tasks.len(), 1, "tasks: {:#?}", all_tasks);

        let task1_key = "test_ns|test_graph|invocation1|test_fn|task1";
        let task1: serde_json::Value = serde_json::from_slice(
            &db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task1_key)?
                .unwrap(),
        )?;
        assert_eq!(task1["invocation_id"], "invocation1");

        let task2_key = "test_ns|test_graph|invocation2|test_fn|task2";
        let task2 = db.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task2_key)?;
        assert!(task2.is_none(), "task2 should be deleted");

        Ok(())
    }
}
