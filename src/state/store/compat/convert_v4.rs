use indexify_internal_api::ExtractionGraph;
use openraft::{StorageError, StorageIOError};
use rocksdb::OptimisticTransactionDB;
use tracing::info;

use super::convert_column;
use crate::state::{
    store::{StateMachineColumns, CURRENT_STORE_VERSION, STORE_VERSION},
    NodeId,
};

fn convert_graph_key(
    _key: &[u8],
    value: &ExtractionGraph,
) -> Result<Vec<u8>, StorageError<NodeId>> {
    Ok(value.key())
}

pub fn convert_v4(db: &OptimisticTransactionDB) -> Result<(), StorageError<NodeId>> {
    info!("Converting store to v{} from v4", CURRENT_STORE_VERSION);

    convert_column(
        db,
        StateMachineColumns::ExtractionGraphs.cf(db),
        |graph: ExtractionGraph| Ok(graph),
        convert_graph_key,
    )?;

    db.put_cf(
        StateMachineColumns::RaftState.cf(db),
        STORE_VERSION,
        CURRENT_STORE_VERSION.to_be_bytes(),
    )
    .map_err(|e| StorageIOError::read_state_machine(&e))?;

    Ok(())
}
