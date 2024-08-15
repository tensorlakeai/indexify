use anyhow::anyhow;
use indexify_internal_api::{v3, ExtractionGraph};
use openraft::{StorageError, StorageIOError};
use rocksdb::OptimisticTransactionDB;
use tracing::info;

use super::{
    convert_column,
    convert_column_value,
    convert_v2_task,
    init_task_analytics,
    v3 as req_v3,
};
use crate::state::{
    store::{StateMachineColumns, CURRENT_STORE_VERSION, LOG_STORE_LOGS_COLUMN},
    NodeId,
};

pub fn convert_v3(
    db: &OptimisticTransactionDB,
    log_db: &OptimisticTransactionDB,
) -> Result<(), StorageError<NodeId>> {
    info!("Converting store to v{} from v3", CURRENT_STORE_VERSION);

    convert_column(
        db,
        StateMachineColumns::ExtractionGraphs.cf(db),
        |graph: ExtractionGraph| -> Result<ExtractionGraph, _> { Ok(graph) },
        |_key: &[u8], value: &ExtractionGraph| -> Result<Vec<u8>, _> { Ok(value.key()) },
    )?;

    convert_column_value(db, StateMachineColumns::Tasks.cf(db), |task: v3::Task| {
        Ok(convert_v2_task(task, db).map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(e),
        })?)
    })?;

    let cf = log_db
        .cf_handle(LOG_STORE_LOGS_COLUMN)
        .ok_or_else(|| StorageIOError::read_state_machine(anyhow!("log_db logs cf not found")))?;

    convert_column_value(log_db, cf, |e| {
        req_v3::convert_log_entry(e, db).map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(e),
        })
    })?;

    init_task_analytics(db)?;

    Ok(())
}
