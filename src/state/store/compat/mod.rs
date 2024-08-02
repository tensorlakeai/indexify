use std::fmt::Debug;

use openraft::{StorageError, StorageIOError};
use rocksdb::{ColumnFamily, IteratorMode, OptimisticTransactionDB};
use serde::de::DeserializeOwned;

use super::{JsonEncode, JsonEncoder};
use crate::state::{NodeId, StateMachineColumns};

mod convert_v1;
mod convert_v2;
mod convert_v3;
mod v2;
mod v3;

pub use convert_v1::convert_v1_task;
pub use convert_v2::convert_v2;
pub use convert_v3::convert_v3;
use indexify_internal_api::{Task, TaskAnalytics, TaskOutcome};
pub(crate) use v2::convert_v2_task;

// This assumes that key value does not change with conversion.
fn convert_column<T, U>(
    db: &OptimisticTransactionDB,
    cf: &ColumnFamily,
    convert: impl Fn(T) -> Result<U, StorageError<NodeId>>,
) -> Result<(), StorageError<NodeId>>
where
    T: DeserializeOwned,
    U: serde::Serialize + Debug,
{
    for val in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = val.map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: T =
            JsonEncoder::decode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: U = convert(value)?;
        let value =
            &JsonEncoder::encode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        db.put_cf(cf, &key, value)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
    }
    Ok(())
}

pub fn init_task_analytics(db: &OptimisticTransactionDB) -> Result<(), StorageError<NodeId>> {
    let iter = db.iterator_cf(StateMachineColumns::Tasks.cf(db), IteratorMode::Start);
    for val in iter {
        let (_, value) = val.map_err(|e| StorageIOError::read_state_machine(&e))?;
        let task: Task =
            JsonEncoder::decode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let key = format!(
            "{}_{}_{}",
            task.namespace, task.extraction_graph_name, task.extraction_policy_name
        );
        let task_analytics = db
            .get_cf(StateMachineColumns::TaskAnalytics.cf(db), key.clone())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        let mut task_analytics: TaskAnalytics = task_analytics
            .map(|db_vec| {
                JsonEncoder::decode(&db_vec).map_err(|e| StorageIOError::read_state_machine(&e))
            })
            .unwrap_or_else(|| Ok(TaskAnalytics::default()))?;
        match task.outcome {
            TaskOutcome::Success => task_analytics.success(),
            TaskOutcome::Failed => task_analytics.fail(),
            TaskOutcome::Unknown => task_analytics.pending(),
        }
        let serialized_task_analytics = JsonEncoder::encode(&task_analytics)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        db.put_cf(
            StateMachineColumns::TaskAnalytics.cf(db),
            key,
            &serialized_task_analytics,
        )
        .map_err(|e| StorageIOError::write_state_machine(&e))?;
    }

    Ok(())
}
