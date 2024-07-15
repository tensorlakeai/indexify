use std::fmt::Debug;

use anyhow::anyhow;
use indexify_internal_api::{v2, ExtractionGraph, ExtractionPolicy};
use openraft::{StorageError, StorageIOError};
use rocksdb::{ColumnFamily, IteratorMode, OptimisticTransactionDB};
use serde::de::DeserializeOwned;
use tracing::info;

use super::v2 as req_v2;
use crate::state::{
    store::{
        serializer::{JsonEncode, JsonEncoder},
        StateMachineColumns,
        CURRENT_STORE_VERSION,
        LOG_STORE_LOGS_COLUMN,
        STORE_VERSION,
    },
    NodeId,
};

// This assumes that key value does not change with conversion.
fn convert_column<T, U>(
    db: &OptimisticTransactionDB,
    cf: &ColumnFamily,
    convert: impl Fn(T) -> U,
) -> Result<(), StorageError<NodeId>>
where
    T: DeserializeOwned,
    U: serde::Serialize + Debug,
{
    for val in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = val.map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: T =
            JsonEncoder::decode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: U = convert(value);
        let value =
            &JsonEncoder::encode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        db.put_cf(cf, &key, &value)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
    }
    Ok(())
}

// TODO: handle crashes during conversion
pub fn convert_v2(
    db: &OptimisticTransactionDB,
    log_db: &OptimisticTransactionDB,
) -> Result<(), StorageError<NodeId>> {
    info!("Converting store to v{} from v2", CURRENT_STORE_VERSION);
    convert_column(
        db,
        StateMachineColumns::ExtractionGraphs.cf(db),
        |graph: v2::ExtractionGraph| -> ExtractionGraph { graph.into() },
    )?;
    convert_column(
        db,
        StateMachineColumns::ExtractionPolicies.cf(db),
        |policy: v2::ExtractionPolicy| -> ExtractionPolicy { policy.into() },
    )?;

    let cf = log_db
        .cf_handle(LOG_STORE_LOGS_COLUMN)
        .ok_or_else(|| StorageIOError::read_state_machine(anyhow!("log_db logs cf not found")))?;

    convert_column(log_db, cf, req_v2::convert_log_entry)?;

    db.put_cf(
        StateMachineColumns::RaftState.cf(db),
        STORE_VERSION,
        CURRENT_STORE_VERSION.to_be_bytes(),
    )
    .map_err(|e| StorageIOError::read_state_machine(&e))?;

    Ok(())
}
