use std::sync::Arc;

use anyhow::{anyhow, Result};
use opentelemetry::KeyValue;
use rocksdb::{Direction, IteratorMode, ReadOptions, TransactionDB};
use serde::de::DeserializeOwned;
use tracing::{debug, trace, warn};

use super::state_machine::IndexifyObjectsColumns;
use crate::{
    data_model::{
        Allocation,
        ComputeGraph,
        ComputeGraphVersion,
        DataPayload,
        FunctionExecutorDiagnostics,
        GcUrl,
        GraphInvocationCtx,
        GraphVersion,
        InvocationPayload,
        Namespace,
        NodeOutput,
        StateChange,
        Task,
        UnprocessedStateChanges,
    },
    metrics::{self, Timer},
    state_store::serializer::{JsonEncode, JsonEncoder},
    utils::get_epoch_time_in_ms,
};

pub enum CursorDirection {
    Forward,
    Backward,
}

pub struct StateReader {
    db: Arc<TransactionDB>,
    metrics: Arc<metrics::StateStoreMetrics>,
}

impl StateReader {
    pub fn new(db: Arc<TransactionDB>, metrics: Arc<metrics::StateStoreMetrics>) -> Self {
        Self { db, metrics }
    }

    pub fn get_rows_from_cf_multi_key<V>(
        &self,
        keys: Vec<&[u8]>,
        column: IndexifyObjectsColumns,
    ) -> Result<Vec<Option<V>>>
    where
        V: DeserializeOwned,
    {
        let kvs = &[KeyValue::new("op", "get_rows_from_cf_multi_key")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;
        let mut items = Vec::with_capacity(keys.len());
        let multi_get_keys: Vec<_> = keys.iter().map(|k| (&cf_handle, k.to_vec())).collect();
        let values = self.db.multi_get_cf(multi_get_keys);
        for (key, value) in keys.into_iter().zip(values.into_iter()) {
            let val: Option<V> = if let Some(value) = value? {
                Some(JsonEncoder::decode(&value).map_err(|e| anyhow::anyhow!(e.to_string()))?)
            } else {
                warn!(
                    "Key not found: {}",
                    String::from_utf8(key.to_vec()).unwrap_or_default()
                );
                None
            };
            items.push(val);
        }
        Ok(items)
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
        let kvs = &[KeyValue::new("op", "get_rows_from_cf_with_limits")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let cf_handle = self
            .db
            .cf_handle(column.as_ref())
            .ok_or(anyhow::anyhow!("Failed to get column family {}", column))?;

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(10_194_304);
        let iterator_mode = match restart_key {
            Some(restart_key) => IteratorMode::From(restart_key, Direction::Forward),
            None => IteratorMode::From(key_prefix, Direction::Forward),
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
        let kvs = &[KeyValue::new("op", "get_from_cf")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let result_bytes = match self.db.get_cf(&column.cf_db(&self.db), key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let result = JsonEncoder::decode::<T>(&result_bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

        Ok(Some(result))
    }

    pub fn get_diagnostic_payload(
        &self,
        ns: &str,
        cg: &str,
        inv_id: &str,
        id: &str,
        file: &str,
    ) -> Result<Option<DataPayload>> {
        let kvs = &[KeyValue::new("op", "get_diagnostic_payload")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let allocation_key = Allocation::key_from(ns, cg, inv_id, id);

        let Some(allocation) = self.get_allocation(&allocation_key)? else {
            return Ok(None);
        };

        if let Some(diagnostics) = allocation.diagnostics {
            match file {
                "stdout" => {
                    if let Some(stdout) = diagnostics.stdout {
                        return Ok(Some(stdout));
                    }
                }
                "stderr" => {
                    if let Some(stderr) = diagnostics.stderr {
                        return Ok(Some(stderr));
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!("Invalid file type"));
                }
            }
        }

        Ok(None)
    }

    pub fn get_function_executor_startup_diagnostic_payload(
        &self,
        ns: &str,
        cg: &str,
        func: &str,
        ver: &str,
        fe_id: &str,
        file: &str,
    ) -> Result<Option<DataPayload>> {
        let kvs = &[KeyValue::new(
            "op",
            "get_function_executor_diagnostic_payload",
        )];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let fe_diagnostics_key = FunctionExecutorDiagnostics::key_from(ns, cg, func, ver, fe_id);
        let fe_diagnostics: Option<FunctionExecutorDiagnostics> = self.get_from_cf(
            &IndexifyObjectsColumns::FunctionExecutorDiagnostics,
            fe_diagnostics_key,
        )?;
        let Some(fe_diagnostics) = fe_diagnostics else {
            return Ok(None);
        };

        match file {
            "stdout" => {
                if let Some(stdout) = fe_diagnostics.startup_stdout {
                    return Ok(Some(stdout));
                }
            }
            "stderr" => {
                if let Some(stderr) = fe_diagnostics.startup_stderr {
                    return Ok(Some(stderr));
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Invalid file type"));
            }
        }

        Ok(None)
    }

    pub fn get_gc_urls(&self, limit: Option<usize>) -> Result<(Vec<GcUrl>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "get_gc_urls")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let limit = limit.unwrap_or(usize::MAX);
        let (urls, cursor) = self.get_rows_from_cf_with_limits::<GcUrl>(
            &[],
            None,
            IndexifyObjectsColumns::GcUrls,
            Some(limit),
        )?;
        Ok((urls, cursor))
    }

    pub fn get_graph_input(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<Option<DataPayload>> {
        let kvs = &[KeyValue::new("op", "get_graph_input")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let key = InvocationPayload::key_from(namespace, compute_graph, invocation_id);
        let Some(input) = self.get_from_cf::<InvocationPayload, _>(
            &IndexifyObjectsColumns::GraphInvocations,
            key.as_bytes(),
        )?
        else {
            return Ok(None);
        };
        Ok(Some(input.payload))
    }

    pub fn all_unprocessed_state_changes(&self) -> Result<Vec<StateChange>> {
        let kvs = &[KeyValue::new("op", "all_unprocessed_state_changes")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);

        let cf = IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&self.db);
        let iter = self
            .db
            .iterator_cf_opt(&cf, read_options, IteratorMode::Start);
        let mut state_changes = Vec::new();
        for kv in iter {
            if let Ok((_, value)) = kv {
                let state_change = JsonEncoder::decode::<StateChange>(&value)?;
                state_changes.push(state_change);
            }
        }
        Ok(state_changes)
    }

    pub fn unprocessed_state_changes(
        &self,
        global_index: &Option<Vec<u8>>,
        ns_index: &Option<Vec<u8>>,
    ) -> Result<UnprocessedStateChanges> {
        let kvs = &[KeyValue::new("op", "unprocessed_state_changes")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let mut state_changes = Vec::new();
        let (global_state_changes, global_state_change_cursor) = self
            .get_rows_from_cf_with_limits::<StateChange>(
                "global".as_bytes(),
                global_index.as_deref(),
                IndexifyObjectsColumns::UnprocessedStateChanges,
                None,
            )?;
        let global_count = global_state_changes.len();
        state_changes.extend(global_state_changes);
        if state_changes.len() >= 100 {
            trace!(
                total = state_changes.len(),
                global = global_count,
                ns = 0,
                "Returning unprocessed state changes (no ns messages fetched)",
            );
            return Ok(UnprocessedStateChanges {
                changes: state_changes,
                last_global_state_change_cursor: global_state_change_cursor,
                // returning previous ns cursor as we did not fetch ns messages
                last_namespace_state_change_cursor: ns_index.clone(),
            });
        }

        let (ns_state_changes, ns_state_change_cursor) = self
            .get_rows_from_cf_with_limits::<StateChange>(
                "ns".as_bytes(),
                ns_index.as_deref(),
                IndexifyObjectsColumns::UnprocessedStateChanges,
                Some(100 - state_changes.len()),
            )?;
        let ns_count = ns_state_changes.len();
        state_changes.extend(ns_state_changes);

        trace!(
            total = state_changes.len(),
            global = global_count,
            ns = ns_count,
            "Returning unprocessed state changes",
        );
        Ok(UnprocessedStateChanges {
            changes: state_changes,
            last_global_state_change_cursor: global_state_change_cursor,
            last_namespace_state_change_cursor: ns_state_change_cursor,
        })
    }

    pub fn get_all_rows_from_cf<V>(
        &self,
        column: IndexifyObjectsColumns,
    ) -> Result<Vec<(String, V)>>
    where
        V: DeserializeOwned,
    {
        let kvs = &[KeyValue::new("op", "get_all_rows_from_cf")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

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

    pub fn get_namespace(&self, namespace: &str) -> Result<Option<Namespace>> {
        let kvs = &[KeyValue::new("op", "get_namespace")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let ns = self.get_from_cf(&IndexifyObjectsColumns::Namespaces, namespace)?;
        Ok(ns)
    }

    pub fn get_all_namespaces(&self) -> Result<Vec<Namespace>> {
        let kvs = &[KeyValue::new("op", "get_all_namespaces")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

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
        limit: usize,
        direction: Option<CursorDirection>,
    ) -> Result<(Vec<GraphInvocationCtx>, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "list_invocations")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let key_prefix = [namespace.as_bytes(), b"|", compute_graph.as_bytes(), b"|"].concat();

        let direction = direction.unwrap_or(CursorDirection::Forward);
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(10_194_304);

        let mut upper_bound = key_prefix.clone();
        upper_bound.extend(&get_epoch_time_in_ms().to_be_bytes());
        read_options.set_iterate_upper_bound(upper_bound);

        let mut lower_bound = key_prefix.clone();
        lower_bound.extend(&0_u64.to_be_bytes());
        read_options.set_iterate_lower_bound(lower_bound);

        let mut iter = self.db.raw_iterator_cf_opt(
            &IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.cf_db(&self.db),
            read_options,
        );

        match cursor {
            Some(cursor) => {
                match direction {
                    CursorDirection::Backward => iter.seek(cursor),
                    CursorDirection::Forward => iter.seek_for_prev(cursor),
                }
                // Skip the first item (cursor position)
                if iter.valid() {
                    match direction {
                        CursorDirection::Forward => iter.prev(),
                        CursorDirection::Backward => iter.next(),
                    }
                }
            }
            None => match direction {
                CursorDirection::Backward => iter.seek_to_first(), // Start at beginning of range
                CursorDirection::Forward => iter.seek_to_last(),   // Start at end of range
            },
        }

        let mut rows = Vec::new();
        let mut next_cursor = None;
        let mut prev_cursor = None;

        // Collect results
        while iter.valid() && rows.len() < limit {
            if let Some((key, _v)) = iter.item() {
                rows.push(key.to_vec());
            } else {
                break; // No valid item found
            }

            // Move the iterator after capturing the current item
            match direction {
                CursorDirection::Forward => iter.prev(),
                CursorDirection::Backward => iter.next(),
            }
        }

        // Check if there are more items after our limit
        if iter.valid() {
            let key = rows.last().cloned();
            match direction {
                CursorDirection::Forward => {
                    next_cursor = key;
                }
                CursorDirection::Backward => {
                    prev_cursor = key;
                }
            }
        }

        // Set the previous cursor if we have a valid item
        if cursor.is_some() {
            let key = rows.first().cloned();
            match direction {
                CursorDirection::Forward => {
                    prev_cursor = key;
                }
                CursorDirection::Backward => {
                    next_cursor = key;
                }
            }
        }

        // Process the collected keys
        let mut invocation_prefixes: Vec<Vec<u8>> = Vec::new();
        for key in rows {
            if let Some(invocation_id) =
                GraphInvocationCtx::get_invocation_id_from_secondary_index_key(&key)
            {
                invocation_prefixes.push(
                    GraphInvocationCtx::key_from(namespace, compute_graph, &invocation_id)
                        .as_bytes()
                        .to_vec(),
                );
            }
        }

        match direction {
            CursorDirection::Forward => {}
            CursorDirection::Backward => {
                // We keep the ordering the same even if we traverse in the opposite direction
                invocation_prefixes.reverse();
            }
        }

        let invocations = self.get_rows_from_cf_multi_key::<GraphInvocationCtx>(
            invocation_prefixes.iter().map(|v| v.as_slice()).collect(),
            IndexifyObjectsColumns::GraphInvocationCtx,
        )?;

        Ok((
            invocations.into_iter().flatten().collect(),
            prev_cursor,
            next_cursor,
        ))
    }

    pub fn list_compute_graphs(
        &self,
        namespace: &str,
        cursor: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<ComputeGraph>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "list_compute_graphs")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let (compute_graphs, cursor) = self.get_rows_from_cf_with_limits::<ComputeGraph>(
            namespace.as_bytes(),
            cursor,
            IndexifyObjectsColumns::ComputeGraphs,
            limit,
        )?;
        Ok((compute_graphs, cursor))
    }

    pub fn get_compute_graph(&self, namespace: &str, name: &str) -> Result<Option<ComputeGraph>> {
        let kvs = &[KeyValue::new("op", "get_compute_graph")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = ComputeGraph::key_from(namespace, name);
        let compute_graph = self.get_from_cf(&IndexifyObjectsColumns::ComputeGraphs, key)?;
        Ok(compute_graph)
    }

    pub fn get_compute_graph_version(
        &self,
        namespace: &str,
        name: &str,
        version: &GraphVersion,
    ) -> Result<Option<ComputeGraphVersion>> {
        let kvs = &[KeyValue::new("op", "get_compute_graph_version")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = ComputeGraphVersion::key_from(namespace, name, version);
        let compute_graph_version =
            self.get_from_cf(&IndexifyObjectsColumns::ComputeGraphVersions, key)?;
        if compute_graph_version.is_some() {
            return Ok(compute_graph_version);
        }

        debug!(
            "Falling back to compute graph to get version for graph {}",
            name
        );
        let compute_graph = self.get_compute_graph(namespace, name)?;
        match compute_graph {
            Some(compute_graph) => Ok(Some(compute_graph.into_version())),
            None => Ok(None),
        }
    }

    pub fn get_allocation(&self, allocation_id: &str) -> Result<Option<Allocation>> {
        let kvs = &[KeyValue::new("op", "get_allocation")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let allocation = self.get_from_cf(&IndexifyObjectsColumns::Allocations, allocation_id)?;
        Ok(allocation)
    }

    pub fn get_allocations_by_invocation(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<Vec<Allocation>> {
        let kvs = &[KeyValue::new("op", "get_allocations_by_task_id")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let prefix =
            Allocation::key_prefix_from_invocation(namespace, compute_graph, invocation_id);

        let (allocations, _) = self.get_rows_from_cf_with_limits::<Allocation>(
            prefix.as_bytes(),
            None,
            IndexifyObjectsColumns::Allocations,
            None,
        )?;
        Ok(allocations)
    }

    pub fn list_outputs_by_compute_graph(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        restart_key: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<NodeOutput>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "list_outputs_by_compute_graph")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key_prefix = NodeOutput::key_prefix_from(namespace, compute_graph, invocation_id);
        let (node_outputs, cursor) = self.get_rows_from_cf_with_limits::<NodeOutput>(
            key_prefix.as_bytes(),
            restart_key,
            IndexifyObjectsColumns::FnOutputs,
            limit,
        )?;
        let node_outputs = node_outputs
            .into_iter()
            .filter(|node_output| !node_output.reducer_output)
            .collect();
        Ok((node_outputs, cursor))
    }

    pub fn get_task(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
        task_id: &str,
    ) -> Result<Option<Task>> {
        let kvs = &[KeyValue::new("op", "get_task")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = Task::key_from(namespace, compute_graph, invocation_id, compute_fn, task_id);
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
        let kvs = &[KeyValue::new("op", "list_tasks_by_compute_graph")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key_prefix = Task::key_prefix_for_invocation(namespace, compute_graph, invocation_id);
        self.get_rows_from_cf_with_limits::<Task>(
            key_prefix.as_bytes(),
            restart_key,
            IndexifyObjectsColumns::Tasks,
            limit,
        )
    }

    pub fn get_node_output_by_key(&self, key: &str) -> Result<Option<NodeOutput>> {
        let kvs = &[KeyValue::new("op", "get_node_output_by_key")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let value = self
            .db
            .get_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&self.db), key)?;
        match value {
            Some(value) => Ok(JsonEncoder::decode(&value)?),
            None => Ok(None),
        }
    }

    pub fn invocation_ctx(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<Option<GraphInvocationCtx>> {
        let kvs = &[KeyValue::new("op", "invocation_ctx")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = GraphInvocationCtx::key_from(namespace, compute_graph, invocation_id);
        let value = self.db.get_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&self.db),
            &key,
        )?;
        if value.is_none() {
            return Ok(None);
        }
        let invocation_ctx: GraphInvocationCtx = JsonEncoder::decode(&value.unwrap())
            .map_err(|e| anyhow!("unable to decode invocation ctx: {}", e))?;
        Ok(Some(invocation_ctx))
    }

    pub fn fn_output_payload(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
        id: &str,
    ) -> Result<Option<NodeOutput>> {
        let kvs = &[KeyValue::new("op", "fn_output_payload")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = NodeOutput::key_from(namespace, compute_graph, invocation_id, compute_fn, id);
        let value = self
            .db
            .get_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&self.db), &key)
            .map_err(|e| anyhow!("unable to get output payload: {}", e))?;
        match value {
            Some(value) => Ok(JsonEncoder::decode(&value)?),
            None => Ok(None),
        }
    }

    pub fn fn_output_payload_first(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
    ) -> Result<Option<NodeOutput>> {
        let kvs = &[KeyValue::new("op", "fn_output_payload_first")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = format!(
            "{}|{}|{}|{}",
            namespace, compute_graph, invocation_id, compute_fn
        );

        let (node_outputs, _) = self.get_rows_from_cf_with_limits::<NodeOutput>(
            key.as_bytes(),
            None,
            IndexifyObjectsColumns::FnOutputs,
            Some(1),
        )?;
        Ok(node_outputs.first().cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_model::Namespace,
        state_store::{
            requests::{NamespaceRequest, RequestPayload, StateMachineUpdateRequest},
            test_state_store::TestStateStore,
        },
    };

    #[tokio::test]
    async fn test_get_rows_from_cf_with_limits() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;
        for i in 0..4 {
            let name = format!("test_{i}");
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                        name: name.clone(),
                        blob_storage_bucket: None,
                        blob_storage_region: None,
                    }),
                    processed_state_changes: vec![],
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

        Ok(())
    }
}
