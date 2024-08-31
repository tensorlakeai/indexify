use std::fmt::Debug;

use openraft::{StorageError, StorageIOError};
use rocksdb::{ColumnFamily, IteratorMode, OptimisticTransactionDB, WriteBatchWithTransaction};
use serde::de::DeserializeOwned;

use super::{JsonEncode, JsonEncoder};
use crate::state::{NodeId, StateMachineColumns};

mod convert_v4;
mod v4;

pub use convert_v4::convert_v4;
use indexify_internal_api::ContentMetadata;

fn convert_column<T, U>(
    db: &OptimisticTransactionDB,
    cf: &ColumnFamily,
    convert_value: impl Fn(T) -> Result<U, StorageError<NodeId>>,
    convert_key: impl Fn(&[u8], &U) -> Result<Vec<u8>, StorageError<NodeId>>,
) -> Result<(), StorageError<NodeId>>
where
    T: DeserializeOwned,
    U: serde::Serialize + Debug,
{
    let mut batch = WriteBatchWithTransaction::<true>::default();
    for val in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) = val.map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: T =
            JsonEncoder::decode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let value: U = convert_value(value)?;
        let new_key = convert_key(&key, &value)?;
        let value =
            &JsonEncoder::encode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        if new_key != *key {
            batch.delete_cf(cf, &key);
        }
        batch.put_cf(cf, &new_key, value);
    }
    db.write(batch)
        .map_err(|e| StorageIOError::read_state_machine(&e))?;
    Ok(())
}

pub fn init_graph_index(db: &OptimisticTransactionDB) -> Result<(), StorageError<NodeId>> {
    let iter = db.iterator_cf(
        StateMachineColumns::ContentTable.cf(db),
        IteratorMode::Start,
    );
    for item in iter {
        let (_, value) = item.map_err(|e| StorageIOError::read_state_machine(&e))?;
        let content: ContentMetadata =
            JsonEncoder::decode(&value).map_err(|e| StorageIOError::read_state_machine(&e))?;
        for graph in content.extraction_graph_names.iter() {
            let key = content.graph_key(&graph);
            db.put_cf(StateMachineColumns::GraphContentIndex.cf(db), key, &[])
                .map_err(|e| StorageIOError::write_state_machine(&e))?;
        }
    }
    Ok(())
}
