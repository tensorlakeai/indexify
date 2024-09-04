use std::{mem, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraph, DataObject, Namespace, StateChange, Task};
use rocksdb::{Direction, IteratorMode, ReadOptions, TransactionDB};
use serde::de::DeserializeOwned;

use super::state_machine::IndexifyObjectsColumns;
use crate::serializer::{JsonEncode, JsonEncoder};
#[derive(Debug)]
pub struct FilterResponse<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub cursor: Vec<u8>,
}

pub struct StateReader {
    db: Arc<TransactionDB>,
}

impl StateReader {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self { db }
    }

    pub fn get_rows_from_cf_with_limits<V>(
        &self,
        start_key_prefix: Option<String>,
        column: IndexifyObjectsColumns,
        limit: Option<usize>,
    ) -> Result<(Vec<V>, Vec<u8>)>
    where
        V: DeserializeOwned,
    {
        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let prefix = start_key_prefix
            .unwrap_or("".to_string())
            .as_bytes()
            .to_vec();
        let iterator_mode = IteratorMode::From(&prefix, Direction::Forward);
        let iter = self
            .db
            .iterator_cf_opt(&cf_handle, read_options, iterator_mode);

        let mut items = Vec::new();
        let mut total = 0;
        let limit = limit.unwrap_or(usize::MAX);
        let mut restart_key = "".to_string();
        for kv in iter {
            let kv = kv?;
            let value = JsonEncoder::decode(&kv.1).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            items.push(value);
            total += 1;
            if total >= limit {
                restart_key = String::from_utf8(kv.0.to_vec())
                    .map_err(|e| anyhow!("unable to convert key to string {}", e))?;
                break;
            }
        }
        Ok((items, restart_key.into()))
    }

    pub fn filter_join_cf<T, F, K>(
        &self,
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
        let index_cf = index_column.cf_db(&self.db);
        let data_cf = data_column.cf_db(&self.db);
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
        let iter = self.db.iterator_cf_opt(&index_cf, read_options, mode);
        let mut items = Vec::new();
        let mut total = 0;
        let limit = limit.unwrap_or(usize::MAX);
        let mut restart_key = Vec::new();
        let mut lookup_keys = Vec::new();
        let mut keys = Vec::<Box<[u8]>>::new();

        let mut get_entries = |lookup_keys, keys: Vec<Box<[u8]>>| -> Result<bool> {
            let res = &self.db.multi_get_cf(lookup_keys);
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
                lookup_keys.push((&data_cf, key_reference(&key)?));
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
        &self,
        column: IndexifyObjectsColumns,
        filter: F,
        start: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<FilterResponse<T>, anyhow::Error>
    where
        T: DeserializeOwned,
        F: Fn(&T) -> bool,
    {
        let cf = column.cf_db(&self.db);
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let mode = match start {
            Some(start) => IteratorMode::From(start, Direction::Forward),
            None => IteratorMode::Start,
        };
        let iter = self.db.iterator_cf_opt(&cf, read_options, mode);
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
        &self,
        column: &IndexifyObjectsColumns,
        key: K,
    ) -> Result<Option<T>, anyhow::Error>
    where
        T: DeserializeOwned,
        K: AsRef<[u8]>,
    {
        let result_bytes = match self.db.get_cf(&column.cf_db(&self.db), key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let result = JsonEncoder::decode::<T>(&result_bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

        Ok(Some(result))
    }

    pub fn get_unprocessed_state_changes(&self) -> Result<Vec<StateChange>> {
        let cf = IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&self.db);
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
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
        &self,
        column: IndexifyObjectsColumns,
    ) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned,
    {
        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;
        let iter = self.db.iterator_cf(&cf_handle, IteratorMode::Start);

        iter.map(|item| {
            item.map_err(|e| anyhow::anyhow!(e.to_string()))
                .and_then(|(key, value)| {
                    let key = String::from_utf8(key.to_vec())
                        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
                    let value =
                        JsonEncoder::decode(&value).map_err(|e| anyhow::anyhow!(e.to_string()))?;
                    Ok((key, value))
                })
        })
        .collect::<Result<Vec<(String, V)>, _>>()
    }

    pub fn get_all_namespaces(&self, limit: Option<usize>) -> Result<Vec<Namespace>> {
        let (namespaces, _) = self.get_rows_from_cf_with_limits::<Namespace>(
            None,
            IndexifyObjectsColumns::Namespaces,
            limit,
        )?;
        Ok(namespaces)
    }

    pub fn get_all_compute_graphs(&self, limit: Option<usize>) -> Result<Vec<ComputeGraph>> {
        let (compute_graphs, _) = self.get_rows_from_cf_with_limits::<ComputeGraph>(
            None,
            IndexifyObjectsColumns::ComputeGraphs,
            limit,
        )?;
        Ok(compute_graphs)
    }

    pub fn get_compute_graph(&self, namespace: &str, name: &str) -> Result<Option<ComputeGraph>> {
        let key = format!("{}_{}", namespace, name);
        let compute_graph = self.get_from_cf(&IndexifyObjectsColumns::ComputeGraphs, key)?;
        Ok(compute_graph)
    }

    pub fn get_task(
        &self,
        namespace: &str,
        compute_graph: &str,
        compute_fn: &str,
        task_id: &str,
    ) -> Result<Option<Task>> {
        let key = format!("{}_{}_{}_{}", namespace, compute_graph, compute_fn, task_id);
        let task = self.get_from_cf(&IndexifyObjectsColumns::Tasks, key)?;
        Ok(task)
    }

    pub fn get_task_by_compute_graph(
        &self,
        namespace: &str,
        compute_graph: &str,
    ) -> Result<Vec<Task>> {
        let key = format!("{}_{}", namespace, compute_graph);
        let (tasks, _) = self.get_rows_from_cf_with_limits::<Task>(
            Some(key),
            IndexifyObjectsColumns::Tasks,
            None,
        )?;
        Ok(tasks)
    }

    pub fn get_task_outputs(&self, namespace: &str, compute_graph: &str, compute_fn: &str) -> Result<Vec<DataObject>> {
        let key = format!("{}_{}_{}", namespace, compute_graph, compute_fn);
        let (data_objects, _) = self.get_rows_from_cf_with_limits::<DataObject>(
            Some(key),
            IndexifyObjectsColumns::FnOutputData,
            None,
        )?;
        Ok(data_objects)
    }
}
#[cfg(test)]
mod tests {
    use super::super::requests::{NamespaceRequest, RequestType};
    use super::super::IndexifyState;
    use super::*;
    use data_model::Namespace;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_get_rows_from_cf_with_limits() {
        let temp_dir = TempDir::new().unwrap();
        let indexify_state =
            IndexifyState::new(PathBuf::from(temp_dir.path().join("state"))).unwrap();
        for i in 0..4 {
            let name = format!("test_{}", i);
            indexify_state
                .write(RequestType::CreateNameSpace(NamespaceRequest {
                    name: name.clone(),
                }))
                .await
                .unwrap();
        }

        let reader = indexify_state.reader();
        let result = reader
            .get_rows_from_cf_with_limits::<Namespace>(
                Some("test_".to_string()),
                IndexifyObjectsColumns::Namespaces,
                Some(3),
            )
            .unwrap();
        let cursor = String::from_utf8(result.1.clone()).unwrap();

        assert_eq!(result.0.len(), 3);
        assert_eq!(cursor, "test_2");

        let result = reader
            .get_rows_from_cf_with_limits::<Namespace>(
                Some("test_2".to_string()),
                IndexifyObjectsColumns::Namespaces,
                Some(3),
            )
            .unwrap();
        let cursor = String::from_utf8(result.1.clone()).unwrap();
        assert_eq!(result.0.len(), 2);
        assert_eq!(cursor, "");
    }
}
