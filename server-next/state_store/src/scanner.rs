use std::mem;

use anyhow::Result;
use data_model::StateChange;
use rocksdb::{Direction, IteratorMode, OptimisticTransactionDB, ReadOptions};
use serde::de::DeserializeOwned;

use super::state_machine::IndexifyObjectsColumns;
use crate::serializer::{JsonEncode, JsonEncoder};

pub fn filter_join_cf<T, F, K>(
    db: &OptimisticTransactionDB,
    index_column: IndexifyObjectsColumns,
    data_column: IndexifyObjectsColumns,
    filter: F,
    key_prefix: &[u8],
    key_reference: K,
    restart_key: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<FilterResponse<T>, anyhow::Error>
where
    T: DeserializeOwned,
    F: Fn(&T) -> bool,
    K: Fn(&[u8]) -> Result<Vec<u8>, anyhow::Error>,
{
    let index_cf = index_column.cf(db);
    let data_cf = data_column.cf(db);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let mode = match restart_key {
        Some(restart_key) => IteratorMode::From(restart_key, Direction::Forward),
        None => {
            if key_prefix.is_empty() {
                IteratorMode::Start
            } else {
                IteratorMode::From(key_prefix, Direction::Forward)
            }
        }
    };
    let iter = db.iterator_cf_opt(index_cf, read_options, mode);
    let mut items = Vec::new();
    let mut total = 0;
    let limit = limit.unwrap_or(usize::MAX);
    let mut restart_key = Vec::new();
    let mut lookup_keys = Vec::new();
    let mut keys = Vec::<Box<[u8]>>::new();

    let mut get_entries = |lookup_keys, keys: Vec<Box<[u8]>>| -> Result<bool> {
        let res = db.multi_get_cf(lookup_keys);
        for (index, value) in res.into_iter().enumerate() {
            if let Ok(Some(value)) = value {
                let item = JsonEncoder::decode::<T>(&value)?;
                if filter(&item) {
                    if items.len() < limit {
                        total += 1;
                        items.push(item);
                    } else {
                        restart_key = keys[index].clone().into();
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    };

    for kv in iter {
        if let Ok((key, _)) = kv {
            if !key.starts_with(key_prefix) {
                break;
            }
            lookup_keys.push((data_cf, key_reference(&key)?));
            keys.push(key);
            if lookup_keys.len() >= limit {
                if get_entries(mem::take(&mut lookup_keys), mem::take(&mut keys))? {
                    break;
                }
            }
        } else {
            return Err(anyhow::anyhow!("error reading db"));
        }
    }
    get_entries(mem::take(&mut lookup_keys), mem::take(&mut keys))?;
    Ok(FilterResponse {
        items,
        total,
        cursor: restart_key,
    })
}

pub fn filter_cf<T, F>(
    db: &OptimisticTransactionDB,
    column: IndexifyObjectsColumns,
    filter: F,
    start: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<FilterResponse<T>, anyhow::Error>
where
    T: DeserializeOwned,
    F: Fn(&T) -> bool,
{
    let cf = column.cf(db);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let mode = match start {
        Some(start) => IteratorMode::From(start, Direction::Forward),
        None => IteratorMode::Start,
    };
    let iter = db.iterator_cf_opt(cf, read_options, mode);
    let mut items = Vec::new();
    let mut total = 0;
    let limit = limit.unwrap_or(usize::MAX);
    let mut restart_key = Vec::new();
    for kv in iter {
        if let Ok((key, value)) = kv {
            let item = JsonEncoder::decode::<T>(&value)?;
            if !filter(&item) {
                break;
            }
            if filter(&item) {
                if items.len() < limit {
                    total += 1;
                    items.push(item);
                } else {
                    restart_key = key.into();
                    break;
                }
            }
        } else {
            return Err(anyhow::anyhow!("error reading db"));
        }
    }
    Ok(FilterResponse {
        items,
        total,
        cursor: restart_key,
    })
}

/// This method fetches a key from a specific column family
pub fn get_from_cf<T, K>(
    db: &OptimisticTransactionDB,
    column: IndexifyObjectsColumns,
    key: K,
) -> Result<Option<T>, anyhow::Error>
where
    T: DeserializeOwned,
    K: AsRef<[u8]>,
{
    let result_bytes = match db.get_cf(column.cf(db), key)? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let result = JsonEncoder::decode::<T>(&result_bytes)
        .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

    Ok(Some(result))
}

#[derive(Debug)]
pub struct FilterResponse<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub cursor: Vec<u8>,
}

pub fn get_unprocessed_state_changes(db: &OptimisticTransactionDB) -> Result<Vec<StateChange>> {
    let cf = IndexifyObjectsColumns::UnprocessedStateChanges.cf(db);
    let iter = db.iterator_cf(cf, IteratorMode::Start);
    let mut state_changes = Vec::new();
    let mut count = 0;
    for kv in iter {
        if let Ok((key, _)) = kv {
            let state_change = JsonEncoder::decode::<StateChange>(&key)?;
            state_changes.push(state_change);
            count += 1;
        }
        if count >= 10 {
            break;
        }
    }
    Ok(state_changes)
}
pub fn get_all_rows_from_cf<V>(
    column: IndexifyObjectsColumns,
    db: &OptimisticTransactionDB,
) -> Result<Vec<(String, V)>>
where
    V: DeserializeOwned,
{
    let cf_handle = db
        .cf_handle(column.as_ref())
        .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;
    let iter = db.iterator_cf(cf_handle, IteratorMode::Start);

    iter.map(|item| {
        item.map_err(|e| anyhow::anyhow!(e.to_string()))
            .and_then(|(key, value)| {
                let key =
                    String::from_utf8(key.to_vec()).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                let value =
                    JsonEncoder::decode(&value).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                Ok((key, value))
            })
    })
    .collect::<Result<Vec<(String, V)>, _>>()
}
