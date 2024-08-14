use anyhow::anyhow;
use indexify_internal_api::{v2, ExtractionGraph, ExtractionPolicy};
use openraft::{StorageError, StorageIOError};
use rocksdb::OptimisticTransactionDB;
use tracing::info;

use super::{convert_column, v2 as req_v2};
use crate::state::{
    store::{
        compat::init_task_analytics,
        StateMachineColumns,
        CURRENT_STORE_VERSION,
        LOG_STORE_LOGS_COLUMN,
        STORE_VERSION,
    },
    NodeId,
};

// TODO: handle crashes during conversion
pub fn convert_v2(
    db: &OptimisticTransactionDB,
    log_db: &OptimisticTransactionDB,
) -> Result<(), StorageError<NodeId>> {
    info!("Converting store to v{} from v2", CURRENT_STORE_VERSION);
    convert_column(
        db,
        StateMachineColumns::ExtractionGraphs.cf(db),
        |graph: v2::ExtractionGraph| -> Result<ExtractionGraph, _> { Ok(graph.into()) },
    )?;
    convert_column(
        db,
        StateMachineColumns::ExtractionPolicies.cf(db),
        |policy: v2::ExtractionPolicy| -> Result<ExtractionPolicy, _> { Ok(policy.into()) },
    )?;
    convert_column(
        db,
        StateMachineColumns::Tasks.cf(db),
        |task: v2::Task| -> Result<_, _> {
            req_v2::convert_v2_task(task, db).map_err(|e| StorageError::IO {
                source: StorageIOError::read_state_machine(e),
            })
        },
    )?;

    let cf = log_db
        .cf_handle(LOG_STORE_LOGS_COLUMN)
        .ok_or_else(|| StorageIOError::read_state_machine(anyhow!("log_db logs cf not found")))?;

    convert_column(log_db, cf, |e| {
        req_v2::convert_log_entry(e, db).map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(e),
        })
    })?;

    db.put_cf(
        StateMachineColumns::RaftState.cf(db),
        STORE_VERSION,
        CURRENT_STORE_VERSION.to_be_bytes(),
    )
    .map_err(|e| StorageIOError::read_state_machine(&e))?;

    init_task_analytics(db)?;

    Ok(())
}
