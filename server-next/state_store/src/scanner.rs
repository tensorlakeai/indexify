use std::{mem, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{
    ComputeGraph,
    ExecutorId,
    ExecutorMetadata,
    GraphInvocationCtx,
    InvocationPayload,
    Namespace,
    NodeOutput,
    StateChange,
    Task,
};
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

    pub fn get_rows_from_cf_multi_key<V>(
        &self,
        keys: Vec<&[u8]>,
        column: IndexifyObjectsColumns,
    ) -> Result<Vec<V>>
    where
        V: DeserializeOwned,
    {
        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;
        let mut items = Vec::new();
        for key in keys {
            let value = self.db.get_cf(&cf_handle, key)?.ok_or(anyhow::anyhow!(
                "Key not found {}",
                String::from_utf8(key.to_vec()).unwrap_or_default()
            ))?;
            let value = JsonEncoder::decode(&value).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            items.push(value);
        }
        Ok(items)
    }

    pub fn get_keys_from_cf_with_limits(
        &self,
        key_prefix: &[u8],
        restart_key: Option<&[u8]>,
        column: IndexifyObjectsColumns,
        limit: Option<usize>,
    ) -> Result<(Vec<String>, Option<Vec<u8>>)> {
        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let iterator_mode = match restart_key {
            Some(restart_key) => IteratorMode::From(restart_key, Direction::Forward),
            None => IteratorMode::From(&key_prefix, Direction::Forward),
        };
        let iter = self
            .db
            .iterator_cf_opt(&cf_handle, read_options, iterator_mode);

        let mut items = Vec::new();
        let limit = limit.unwrap_or(usize::MAX);
        let mut restart_key = None;
        for kv in iter {
            let (key, _) = kv?;
            if !key.starts_with(key_prefix) {
                break;
            }
            if items.len() < limit {
                items
                    .push(String::from_utf8(key.to_vec()).map_err(|e| {
                        anyhow::anyhow!("unable to convert bytes to string: {}", e)
                    })?);
            } else {
                restart_key.replace(key.into());
                break;
            }
        }
        Ok((items, restart_key))
    }

    pub fn get_rows_from_cf_with_limits<V>(
        &self,
        key_prefix: &[u8],
        restart_key: Option<&[u8]>,
        column: IndexifyObjectsColumns,
        limit: Option<usize>,
    ) -> Result<(Vec<V>, Option<Vec<u8>>)>
    where
        V: DeserializeOwned,
    {
        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let iterator_mode = match restart_key {
            Some(restart_key) => IteratorMode::From(restart_key, Direction::Forward),
            None => IteratorMode::From(&key_prefix, Direction::Forward),
        };
        let iter = self
            .db
            .iterator_cf_opt(&cf_handle, read_options, iterator_mode);

        let mut items = Vec::new();
        let limit = limit.unwrap_or(usize::MAX);
        let mut restart_key = None;
        for kv in iter {
            let (key, value) = kv?;
            if !key.starts_with(key_prefix) {
                break;
            }
            let value = JsonEncoder::decode(&value).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            if items.len() < limit {
                items.push(value);
            } else {
                restart_key.replace(key.into());
                break;
            }
        }
        Ok((items, restart_key))
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
                print!(
                    "Task key {:?}",
                    String::from_utf8(key_reference(&key).unwrap()).unwrap()
                );
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
            if let Ok((_, serialized_sc)) = kv {
                let state_change = JsonEncoder::decode::<StateChange>(&serialized_sc)?;
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

    pub fn get_all_namespaces(&self) -> Result<Vec<Namespace>> {
        let (namespaces, _) = self.get_rows_from_cf_with_limits::<Namespace>(
            &[],
            None,
            IndexifyObjectsColumns::Namespaces,
            None,
        )?;
        Ok(namespaces)
    }

    pub fn list_invocations(
        &self,
        namespace: &str,
        compute_graph: &str,
        cursor: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<InvocationPayload>, Option<Vec<u8>>)> {
        let key = format!("{}_{}", namespace, compute_graph);
        self.get_rows_from_cf_with_limits::<InvocationPayload>(
            key.as_bytes(),
            cursor,
            IndexifyObjectsColumns::GraphInvocations,
            limit,
        )
    }

    pub fn list_compute_graphs(
        &self,
        namespace: &str,
        cursor: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<ComputeGraph>, Option<Vec<u8>>)> {
        let (compute_graphs, cursor) = self.get_rows_from_cf_with_limits::<ComputeGraph>(
            namespace.as_bytes(),
            cursor,
            IndexifyObjectsColumns::ComputeGraphs,
            limit,
        )?;
        Ok((compute_graphs, cursor))
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
        invocation_id: &str,
        compute_fn: &str,
        task_id: &str,
    ) -> Result<Option<Task>> {
        let key = format!(
            "{}_{}_{}_{}_{}",
            namespace, compute_graph, invocation_id, compute_fn, task_id
        );
        let task = self.get_from_cf(&IndexifyObjectsColumns::Tasks, key)?;
        Ok(task)
    }

    pub fn list_tasks_by_compute_graph(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        restart_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<Task>, Option<Vec<u8>>)> {
        let key = format!("{}_{}_{}_", namespace, compute_graph, invocation_id);
        self.get_rows_from_cf_with_limits::<Task>(
            key.as_bytes(),
            restart_key,
            IndexifyObjectsColumns::Tasks,
            limit,
        )
    }

    pub fn get_task_outputs(&self, namespace: &str, task_id: &str) -> Result<Vec<NodeOutput>> {
        let key = format!("{}_{}", namespace, task_id);
        let (node_output_keys, _) = self.get_rows_from_cf_with_limits::<String>(
            key.as_bytes(),
            None,
            IndexifyObjectsColumns::TaskOutputs,
            None,
        )?;
        let keys = node_output_keys.iter().map(|key| key.as_bytes()).collect();
        let data_objects =
            self.get_rows_from_cf_multi_key::<NodeOutput>(keys, IndexifyObjectsColumns::FnOutputs)?;
        Ok(data_objects)
    }

    pub fn get_tasks_by_executor(&self, executor: &ExecutorId, limit: usize) -> Result<Vec<Task>> {
        let prefix = format!("{}_", executor);
        let task_keys = self.get_rows_from_cf_with_limits::<String>(
            prefix.as_bytes(),
            None,
            IndexifyObjectsColumns::TaskAllocations,
            Some(limit),
        )?;
        let keys: Vec<&[u8]> = task_keys.0.iter().map(|key| key.as_bytes()).collect();
        let tasks = self.get_rows_from_cf_multi_key::<Task>(keys, IndexifyObjectsColumns::Tasks)?;
        Ok(tasks)
    }

    pub fn get_all_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        let (executors, _) = self.get_rows_from_cf_with_limits::<ExecutorMetadata>(
            &[],
            None,
            IndexifyObjectsColumns::Executors,
            None,
        )?;
        Ok(executors)
    }

    pub fn invocation_ctx(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<GraphInvocationCtx> {
        let key = GraphInvocationCtx::key_from(namespace, compute_graph, invocation_id);
        let value = self.db.get_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&self.db),
            &key,
        )?;
        match value {
            Some(value) => Ok(JsonEncoder::decode(&value)?),
            None => Err(anyhow!("invocation ctx not found")),
        }
    }

    pub fn invocation_payload(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<InvocationPayload> {
        let key = InvocationPayload::key_from(namespace, compute_graph, invocation_id);
        let value = self.db.get_cf(
            &IndexifyObjectsColumns::GraphInvocations.cf_db(&self.db),
            &key,
        )?;
        match value {
            Some(value) => Ok(JsonEncoder::decode(&value)?),
            None => Err(anyhow!("invocation payload not found")),
        }
    }

    pub fn unallocated_tasks(&self) -> Result<Vec<Task>> {
        let (tasks, _) = self
            .get_keys_from_cf_with_limits(&[], None, IndexifyObjectsColumns::UnallocatedTasks, None)
            .map_err(|e| anyhow!("unable to read unallocated tasks {}", e))?;
        let keys = tasks.iter().map(|key| key.as_bytes()).collect();
        let tasks = self.get_rows_from_cf_multi_key(keys, IndexifyObjectsColumns::Tasks)?;
        Ok(tasks)
    }

    pub fn fn_output_payload(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
        id: &str,
    ) -> Result<NodeOutput> {
        let key = NodeOutput::key_from(namespace, compute_graph, invocation_id, compute_fn, id);
        let value = self
            .db
            .get_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&self.db), &key)?;
        match value {
            Some(value) => Ok(JsonEncoder::decode(&value)?),
            None => Err(anyhow!("fn output not found")),
        }
    }
}
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use data_model::Namespace;
    use tempfile::TempDir;

    use super::{
        super::{
            requests::{NamespaceRequest, RequestPayload},
            IndexifyState,
        },
        *,
    };
    use crate::requests::StateMachineUpdateRequest;

    #[tokio::test]
    async fn test_get_rows_from_cf_with_limits() {
        let temp_dir = TempDir::new().unwrap();
        let indexify_state =
            IndexifyState::new(PathBuf::from(temp_dir.path().join("state"))).unwrap();
        for i in 0..4 {
            let name = format!("test_{}", i);
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                        name: name.clone(),
                    }),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();
        }

        let reader = indexify_state.reader();
        let result = reader
            .get_rows_from_cf_with_limits::<Namespace>(
                "test_".as_bytes(),
                None,
                IndexifyObjectsColumns::Namespaces,
                Some(3),
            )
            .unwrap();
        let cursor = String::from_utf8(result.1.unwrap().clone()).unwrap();

        assert_eq!(result.0.len(), 3);
        assert_eq!(cursor, "test_3");

        let result = reader
            .get_rows_from_cf_with_limits::<Namespace>(
                "test_".as_bytes(),
                Some("test_2".as_bytes()),
                IndexifyObjectsColumns::Namespaces,
                Some(3),
            )
            .unwrap();
        let cursor = result.1;
        assert_eq!(result.0.len(), 2);
        assert_eq!(cursor, None);
    }
}
