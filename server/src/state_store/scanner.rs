use std::sync::Arc;

use anyhow::{Result, anyhow};
use opentelemetry::KeyValue;
use serde::de::DeserializeOwned;
use tracing::{debug, trace};

use super::state_machine::IndexifyObjectsColumns;
use crate::{
    data_model::{
        Allocation,
        AllocationUsage,
        Application,
        ApplicationVersion,
        GcUrl,
        Namespace,
        RequestCtx,
        StateChange,
        UnprocessedStateChanges,
    },
    metrics::{self, Timer},
    state_store::{
        driver::{IterOptions, RangeOptionsBuilder, Reader, rocksdb::RocksDBDriver},
        serializer::{JsonEncode, JsonEncoder},
    },
    utils::get_epoch_time_in_ms,
};

const MAX_FETCH_LIMIT: usize = 100;

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

        let mut iter_options =
            IterOptions::default().starting_at(restart_key.unwrap_or(key_prefix));
        if limit.is_some() {
            iter_options = iter_options.with_block_size(IterOptions::LARGE_BLOCK_SIZE);
        }

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
            .map_err(|e| anyhow::anyhow!("Deserialization error: {e}"))?;

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
        executor_events_cursor: &Option<Vec<u8>>,
        application_events_cursor: &Option<Vec<u8>>,
    ) -> Result<UnprocessedStateChanges> {
        let kvs = &[KeyValue::new("op", "unprocessed_state_changes")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let mut state_changes = Vec::new();
        let (executor_events, executor_events_cursor) = self
            .get_rows_from_cf_with_limits::<StateChange>(
                &[],
                executor_events_cursor.as_deref(),
                IndexifyObjectsColumns::ExecutorStateChanges,
                None,
            )?;
        let num_executor_events = executor_events.len();
        state_changes.extend(executor_events);
        if state_changes.len() >= 100 {
            trace!(
                total = state_changes.len(),
                executor_events = num_executor_events,
                application_events = 0,
                "Returning unprocessed state changes (no ns messages fetched)",
            );
            return Ok(UnprocessedStateChanges {
                changes: state_changes,
                executor_state_change_cursor: executor_events_cursor,
                // returning previous ns cursor as we did not fetch ns messages
                application_state_change_cursor: application_events_cursor.clone(),
            });
        }

        let (application_events, application_events_cursor) = self
            .get_rows_from_cf_with_limits::<StateChange>(
                &[],
                application_events_cursor.as_deref(),
                IndexifyObjectsColumns::ApplicationStateChanges,
                Some(100 - state_changes.len()),
            )?;
        let num_application_events = application_events.len();
        state_changes.extend(application_events);

        trace!(
            total = state_changes.len(),
            executor_events = num_executor_events,
            application_events = num_application_events,
            "returning unprocessed state changes",
        );
        Ok(UnprocessedStateChanges {
            changes: state_changes,
            application_state_change_cursor: application_events_cursor,
            executor_state_change_cursor: executor_events_cursor,
        })
    }

    /// Fetch allocation usage records with pagination support.
    /// It fetches up to 100 records at a time.
    ///
    /// `cursor`: Optional cursor to start fetching from (exclusive).
    ///
    /// Returns a tuple containing:
    /// - A vector of `AllocationUsage` records.
    /// - An optional cursor for the next page (if more records are available).
    pub fn allocation_usage(
        &self,
        cursor: Option<&Vec<u8>>,
    ) -> Result<(Vec<AllocationUsage>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "allocation_usage")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let cursor = cursor.map(|c| c.as_slice());

        let (events, cursor) = self.get_rows_from_cf_with_limits::<AllocationUsage>(
            &[],
            cursor,
            IndexifyObjectsColumns::AllocationUsage,
            Some(MAX_FETCH_LIMIT),
        )?;

        Ok((events, cursor))
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
    pub fn list_requests(
        &self,
        namespace: &str,
        application: &str,
        cursor: Option<&[u8]>,
        limit: usize,
        direction: Option<CursorDirection>,
    ) -> Result<(Vec<RequestCtx>, Option<Vec<u8>>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "list_requests")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);
        let key_prefix = [namespace.as_bytes(), b"|", application.as_bytes(), b"|"].concat();

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
            IndexifyObjectsColumns::RequestCtxSecondaryIndex.as_ref(),
            range_options,
        )?;

        // Process the collected keys
        let mut request_prefixes = range
            .items
            .iter()
            .flat_map(|key| RequestCtx::get_request_id_from_secondary_index_key(key))
            .map(|request_id| {
                RequestCtx::key_from(namespace, application, &request_id)
                    .as_bytes()
                    .to_vec()
            })
            .collect::<Vec<_>>();

        if range.direction.is_backward() {
            // We keep the ordering the same even if we traverse in the opposite direction
            request_prefixes.reverse();
        }

        let requests = self.get_rows_from_cf_multi_key::<RequestCtx>(
            request_prefixes.iter().map(|v| v.as_slice()).collect(),
            IndexifyObjectsColumns::RequestCtx,
        )?;

        Ok((
            requests,
            range.prev_cursor.map(|c| c.to_vec()),
            range.next_cursor.map(|c| c.to_vec()),
        ))
    }

    pub fn list_applications(
        &self,
        namespace: &str,
        cursor: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<(Vec<Application>, Option<Vec<u8>>)> {
        let kvs = &[KeyValue::new("op", "list_applications")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let (applications, cursor) = self.get_rows_from_cf_with_limits::<Application>(
            namespace.as_bytes(),
            cursor,
            IndexifyObjectsColumns::Applications,
            limit,
        )?;
        Ok((applications, cursor))
    }

    pub fn get_application(&self, namespace: &str, name: &str) -> Result<Option<Application>> {
        let kvs = &[KeyValue::new("op", "get_application")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = Application::key_from(namespace, name);
        let application = self.get_from_cf(&IndexifyObjectsColumns::Applications, key)?;
        Ok(application)
    }

    pub fn get_application_version(
        &self,
        namespace: &str,
        name: &str,
        version: &str,
    ) -> Result<Option<ApplicationVersion>> {
        let kvs = &[KeyValue::new("op", "get_application_version")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let key = ApplicationVersion::key_from(namespace, name, version);
        let application_version =
            self.get_from_cf(&IndexifyObjectsColumns::ApplicationVersions, key)?;
        if application_version.is_some() {
            return Ok(application_version);
        }

        debug!(
            "Falling back to application to get version for application {}",
            name
        );
        let application = self.get_application(namespace, name)?;
        match application {
            Some(application) => application.to_version().map(Some),
            None => Ok(None),
        }
    }

    pub fn get_allocation(&self, allocation_id: &str) -> Result<Option<Allocation>> {
        let kvs = &[KeyValue::new("op", "get_allocation")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let allocation = self.get_from_cf(&IndexifyObjectsColumns::Allocations, allocation_id)?;
        Ok(allocation)
    }

    pub fn get_allocations_by_request_id(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> Result<Vec<Allocation>> {
        let kvs = &[KeyValue::new("op", "get_allocations_by_request_id")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let prefix = Allocation::key_prefix_from_request(namespace, application, request_id);

        let (allocations, _) = self.get_rows_from_cf_with_limits::<Allocation>(
            prefix.as_bytes(),
            None,
            IndexifyObjectsColumns::Allocations,
            None,
        )?;
        Ok(allocations)
    }

    pub fn request_ctx(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> Result<Option<RequestCtx>> {
        let kvs = &[KeyValue::new("op", "request_ctx")];
        let _timer = Timer::start_with_labels(&self.metrics.state_read, kvs);

        let cf = IndexifyObjectsColumns::RequestCtx.as_ref();
        let key = RequestCtx::key_from(namespace, application, request_id);
        let value = self.db.get(cf, &key)?;
        if value.is_none() {
            return Ok(None);
        }
        let request_ctx: RequestCtx = JsonEncoder::decode(&value.unwrap())
            .map_err(|e| anyhow!("unable to decode request ctx: {e}"))?;
        Ok(Some(request_ctx))
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
