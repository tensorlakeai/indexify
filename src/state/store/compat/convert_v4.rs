use anyhow::anyhow;
use openraft::{StorageError, StorageIOError};
use rocksdb::OptimisticTransactionDB;
use tracing::info;

use super::{convert_column, v4 as req_v4};
use crate::state::{
    store::{CURRENT_STORE_VERSION, LOG_STORE_LOGS_COLUMN},
    NodeId,
};

pub fn convert_v4(
    db: &OptimisticTransactionDB,
    log_db: &OptimisticTransactionDB,
) -> Result<(), StorageError<NodeId>> {
    info!("Converting store to v{} from v4", CURRENT_STORE_VERSION);

    let cf = log_db
        .cf_handle(LOG_STORE_LOGS_COLUMN)
        .ok_or_else(|| StorageIOError::read_state_machine(anyhow!("log_db logs cf not found")))?;

    convert_column(log_db, cf, |e| {
        req_v4::convert_log_entry(e, db).map_err(|e| StorageError::IO {
            source: StorageIOError::read_state_machine(e),
        })
    })?;

    Ok(())
}
