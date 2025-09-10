use std::sync::Arc;

use anyhow::{anyhow, Result};
use opentelemetry::KeyValue;
use serde::de::DeserializeOwned;
use tracing::{debug, trace};

use super::state_machine::IndexifyObjectsColumns;
use crate::{
    data_model::{
        Allocation,
        ComputeGraph,
        ComputeGraphVersion,
        GcUrl,
        GraphInvocationCtx,
        GraphVersion,
        Namespace,
        StateChange,
        UnprocessedStateChanges,
    },
    metrics::{self, Timer},
    state_store::{
        driver::{rocksdb::RocksDBDriver, IterOptions, RangeOptionsBuilder, Reader},
        serializer::{JsonEncode, JsonEncoder},
    },
    utils::get_epoch_time_in_ms,
};

#[derive(Clone, Debug, Default)]
pub enum CursorDirection {
    #[default]
    Forward,
    Backward,
}

impl CursorDirection {
    pub fn is_forward(&self) -> bool {
        matches!(self, Self::Forward)
    }

    pub fn is_backward(&self) -> bool {
        !self.is_forward()
    }
}

pub struct StateReader {
    db: Arc<RocksDBDriver>,
    metrics: Arc<metrics::StateStoreMetrics>,
}

impl StateReader {
    pub fn new(db: Arc<RocksDBDriver>, metrics: Arc<metrics::StateStoreMetrics>) -> Self {
        Self { db, metrics }
    }

    pub fn get_rows_from_cf_multi_key<V>(
        &self,
        keys: Vec<&[u8]>,
        column: IndexifyObjectsColumns,
    ) -> Result<Vec<V>>
    where
        V: DeserializeOwned,
    {
        let kvs = &[KeyValue::new("op", "get_rows_from_cf_multi_key")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let result = self.db.list_existent_items(column.as_ref(), keys)?;
        let mut items: Vec<V> = Vec::with_capacity(result.len());
        for v in result {
            let de = JsonEncoder::decode(v.as_ref())?;
            items.push(de);
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

        let iter_options = IterOptions::default().starting_at(restart_key.unwrap_or(key_prefix));

        let iter = self.db.iter(column.as_ref(), iter_options);

        let mut items = Vec::new();
        let limit = limit.unwrap_or(usize::MAX);
        let mut restart_key = None;
        for kv in iter {
            let (key, value) = kv?;
            if !key.starts_with(key_prefix) {
                break;
            }
            let value = JsonEncoder::decode(&value)?;
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

        let result_bytes = match self.db.get(column.as_ref(), key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let result = JsonEncoder::decode::<T>(&result_bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

        Ok(Some(result))
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

    pub fn all_unprocessed_state_changes(&self) -> Result<Vec<StateChange>> {
        let kvs = &[KeyValue::new("op", "all_unprocessed_state_changes")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let iter = self.db.iter(
            IndexifyObjectsColumns::UnprocessedStateChanges.as_ref(),
            Default::default(),
        );
        let mut state_changes = Vec::new();
        for (_, value) in iter.flatten() {
            let state_change = JsonEncoder::decode::<StateChange>(&value)?;
            state_changes.push(state_change);
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

        let iter = self.db.iter(column.as_ref(), Default::default());

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

    #[allow(clippy::type_complexity)]
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

        let mut upper_bound = key_prefix.clone();
        upper_bound.extend(&get_epoch_time_in_ms().to_be_bytes());

        let mut lower_bound = key_prefix.clone();
        lower_bound.extend(&0_u64.to_be_bytes());

        let range_options = RangeOptionsBuilder::default()
            .upper_bound(upper_bound.into())
            .lower_bound(lower_bound.into())
            .direction(direction.clone())
            .cursor(cursor)
            .limit(limit)
            .build()?;

        let range = self.db.get_key_range(
            IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
            range_options,
        )?;

        // Process the collected keys
        let mut invocation_prefixes = range
            .items
            .iter()
            .flat_map(|key| GraphInvocationCtx::get_invocation_id_from_secondary_index_key(key))
            .map(|invocation_id| {
                GraphInvocationCtx::key_from(namespace, compute_graph, &invocation_id)
                    .as_bytes()
                    .to_vec()
            })
            .collect::<Vec<_>>();

        if range.direction.is_backward() {
            // We keep the ordering the same even if we traverse in the opposite direction
            invocation_prefixes.reverse();
        }

        let invocations = self.get_rows_from_cf_multi_key::<GraphInvocationCtx>(
            invocation_prefixes.iter().map(|v| v.as_slice()).collect(),
            IndexifyObjectsColumns::GraphInvocationCtx,
        )?;

        Ok((
            invocations,
            range.prev_cursor.map(|c| c.to_vec()),
            range.next_cursor.map(|c| c.to_vec()),
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
            Some(compute_graph) => compute_graph.to_version().map(Some),
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

    pub fn invocation_ctx(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Result<Option<GraphInvocationCtx>> {
        let kvs = &[KeyValue::new("op", "invocation_ctx")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let cf = IndexifyObjectsColumns::GraphInvocationCtx.as_ref();
        let key = GraphInvocationCtx::key_from(namespace, compute_graph, invocation_id);
        let value = self.db.get(cf, &key)?;
        if value.is_none() {
            return Ok(None);
        }
        let invocation_ctx: GraphInvocationCtx = JsonEncoder::decode(&value.unwrap())
            .map_err(|e| anyhow!("unable to decode invocation ctx: {}", e))?;
        Ok(Some(invocation_ctx))
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

    #[tokio::test]
    async fn test_get_rows_from_cf_multi_key() -> Result<()> {
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
                })
                .await
                .unwrap();
        }

        let reader = indexify_state.reader();
        let keys = vec![
            "test_1".as_bytes(),
            "test_2".as_bytes(),
            "test_3".as_bytes(),
            "test_non_existent".as_bytes(),
        ];
        let result = reader
            .get_rows_from_cf_multi_key::<Namespace>(keys, IndexifyObjectsColumns::Namespaces)
            .unwrap();

        assert_eq!(3, result.len());
        assert!(result.iter().any(|r| r.name == "test_1"));
        assert!(result.iter().any(|r| r.name == "test_2"));
        assert!(result.iter().any(|r| r.name == "test_3"));

        Ok(())
    }
}
