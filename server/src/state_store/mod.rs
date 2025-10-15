use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
};

use anyhow::{Result, anyhow};
use in_memory_state::{InMemoryMetrics, InMemoryState};
use opentelemetry::KeyValue;
use request_events::{RequestFinishedEvent, RequestStateChangeEvent};
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{ExecutorId, StateMachineMetadata},
    metrics::{StateStoreMetrics, Timer},
    state_store::{
        driver::{
            Writer,
            rocksdb::{RocksDBConfig, RocksDBDriver},
        },
        request_events::RequestStartedEvent,
    },
};

#[derive(Debug, Clone, Default)]
pub struct ExecutorCatalog {
    pub entries: Vec<ExecutorCatalogEntry>,
}

impl ExecutorCatalog {
    /// Returns true if no catalog entries are configured.
    pub fn empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub mod driver;
pub mod in_memory_state;
pub mod kv;
pub mod migration_runner;
pub mod migrations;
pub mod request_events;
pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_changes;
pub mod state_machine;

#[cfg(test)]
pub mod test_state_store;

#[derive(Debug)]
pub struct ExecutorState {
    pub new_state_channel: watch::Sender<()>,
}

impl ExecutorState {
    pub fn new() -> Self {
        let (new_state_channel, _) = watch::channel(());
        Self { new_state_channel }
    }

    pub fn notify(&mut self) {
        if let Err(err) = self.new_state_channel.send(()) {
            debug!(
                "failed to notify executor state change, ignoring: {:?}",
                err
            );
        }
    }

    pub fn subscribe(&mut self) -> watch::Receiver<()> {
        self.new_state_channel.subscribe()
    }
}

impl Default for ExecutorState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IndexifyState {
    pub db: Arc<RocksDBDriver>,
    pub executor_states: RwLock<HashMap<ExecutorId, ExecutorState>>,
    pub db_version: u64,
    pub state_change_id_seq: Arc<AtomicU64>,
    pub function_run_event_tx: tokio::sync::broadcast::Sender<RequestStateChangeEvent>,
    pub gc_tx: tokio::sync::watch::Sender<()>,
    pub gc_rx: tokio::sync::watch::Receiver<()>,
    pub change_events_tx: tokio::sync::watch::Sender<()>,
    pub change_events_rx: tokio::sync::watch::Receiver<()>,
    pub usage_events_tx: tokio::sync::watch::Sender<()>,
    pub usage_events_rx: tokio::sync::watch::Receiver<()>,

    pub metrics: Arc<StateStoreMetrics>,
    pub in_memory_state: Arc<RwLock<in_memory_state::InMemoryState>>,
    // keep handle to in_memory_state metrics to avoid dropping it
    _in_memory_state_metrics: InMemoryMetrics,
}

pub(crate) fn open_database<I>(
    path: PathBuf,
    config: RocksDBConfig,
    column_families: I,
    metrics: Arc<StateStoreMetrics>,
) -> Result<RocksDBDriver>
where
    I: Iterator<Item = ColumnFamilyDescriptor>,
{
    info!(
        "opening state store database at {} with config {}",
        path.display(),
        config
    );

    let options = driver::ConnectionOptions::RocksDB(driver::rocksdb::Options {
        path,
        config,
        column_families: column_families.collect::<Vec<_>>(),
    });

    driver::open_database(options, metrics).map_err(Into::into)
}

impl IndexifyState {
    pub async fn new(
        path: PathBuf,
        config: RocksDBConfig,
        executor_catalog: ExecutorCatalog,
    ) -> Result<Arc<Self>> {
        fs::create_dir_all(path.clone())
            .map_err(|e| anyhow!("failed to create state store dir: {e}"))?;

        // Migrate the db before opening with all column families.
        // This is because the migration process may delete older column families.
        // If we open the db with all column families, it would fail to open.
        let sm_meta = migration_runner::run(&path, config.clone())?;

        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        let db = Arc::new(open_database(
            path,
            config,
            sm_column_families,
            state_store_metrics.clone(),
        )?);

        let (gc_tx, gc_rx) = tokio::sync::watch::channel(());
        let (task_event_tx, _) = tokio::sync::broadcast::channel(100);
        let (change_events_tx, change_events_rx) = tokio::sync::watch::channel(());
        let (usage_events_tx, usage_events_rx) = tokio::sync::watch::channel(());

        let indexes = Arc::new(RwLock::new(InMemoryState::new(
            sm_meta.last_change_idx,
            scanner::StateReader::new(db.clone(), state_store_metrics.clone()),
            executor_catalog,
        )?));
        let in_memory_state_metrics = InMemoryMetrics::new(indexes.clone());
        let s = Arc::new(Self {
            db,
            db_version: sm_meta.db_version,
            state_change_id_seq: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            executor_states: RwLock::new(HashMap::new()),
            function_run_event_tx: task_event_tx,
            gc_tx,
            gc_rx,
            metrics: state_store_metrics,
            _in_memory_state_metrics: in_memory_state_metrics,
            change_events_tx,
            change_events_rx,
            in_memory_state: indexes,
            usage_events_tx,
            usage_events_rx,
        });

        info!(
            "initialized state store with last state change id: {}",
            s.state_change_id_seq.load(atomic::Ordering::Relaxed)
        );
        info!("db version discovered: {}", sm_meta.db_version);

        Ok(s)
    }

    pub fn get_gc_watcher(&self) -> tokio::sync::watch::Receiver<()> {
        self.gc_rx.clone()
    }

    pub fn state_change_id_seq(&self) -> Arc<AtomicU64> {
        self.state_change_id_seq.clone()
    }

    pub fn can_allocation_output_be_updated(
        &self,
        request: &requests::AllocationOutput,
    ) -> Result<bool> {
        state_machine::can_allocation_output_be_updated(self.db.clone(), request)
    }

    #[tracing::instrument(
        skip(self, request),
        fields(
            request_type = request.payload.to_string(),
        )
    )]
    pub async fn write(&self, request: StateMachineUpdateRequest) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        debug!("writing state machine update request: {:#?}", request);
        let _timer = Timer::start_with_labels(&self.metrics.state_write, timer_kv);
        let txn = self.db.transaction();

        match &request.payload {
            RequestPayload::InvokeApplication(invoke_application_request) => {
                let _enter = span!(
                    tracing::Level::INFO,
                    "invoke_application",
                    namespace = invoke_application_request.namespace.clone(),
                    request_id = invoke_application_request.ctx.request_id.clone(),
                    app = invoke_application_request.application_name.clone(),
                );
                state_machine::create_request(&txn, invoke_application_request)?;
            }
            RequestPayload::SchedulerUpdate((request, processed_state_changes)) => {
                state_machine::handle_scheduler_update(&txn, request)?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes)?;
            }
            RequestPayload::CreateNameSpace(namespace_request) => {
                state_machine::upsert_namespace(self.db.clone(), namespace_request)?;
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                state_machine::create_or_update_application(
                    &txn,
                    req.application.clone(),
                    req.upgrade_requests_to_current_version,
                )?;
            }
            RequestPayload::DeleteApplicationRequest((request, processed_state_changes)) => {
                state_machine::delete_application(&txn, &request.namespace, &request.name)?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes)?;
            }
            RequestPayload::DeleteRequestRequest((request, processed_state_changes)) => {
                state_machine::delete_request(&txn, request)?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes)?;
            }
            RequestPayload::UpsertExecutor(request) => {
                for allocation_output in &request.allocation_outputs {
                    state_machine::upsert_allocation(&txn, &allocation_output.allocation)?;
                    state_machine::record_allocation_usage(&txn, &allocation_output.allocation)?;
                }

                if request.update_executor_state {
                    self.executor_states
                        .write()
                        .await
                        .entry(request.executor.id.clone())
                        .or_default();
                }
            }
            RequestPayload::DeregisterExecutor(request) => {
                self.executor_states
                    .write()
                    .await
                    .remove(&request.executor_id);
                info!(
                    executor_id = request.executor_id.get(),
                    "marking executor as tombstoned"
                );
            }
            RequestPayload::RemoveGcUrls(urls) => {
                state_machine::remove_gc_urls(&txn, urls.clone())?;
            }
            RequestPayload::ProcessStateChanges(state_changes) => {
                state_machine::mark_state_changes_processed(&txn, state_changes)?;
            }
            RequestPayload::ProcessAllocationUsageEvents(usage) => {
                state_machine::remove_allocation_usage_events(&txn, usage)?;
            }
            _ => {} // Handle other request types as needed
        };

        let new_state_changes = request.state_changes(&self.state_change_id_seq)?;
        if !new_state_changes.is_empty() {
            state_machine::save_state_changes(&txn, &new_state_changes)?;
        }

        let current_state_id = self.state_change_id_seq.load(atomic::Ordering::Relaxed);
        migration_runner::write_sm_meta(
            &txn,
            &StateMachineMetadata {
                last_change_idx: current_state_id,
                db_version: self.db_version,
            },
        )?;
        txn.commit()?;
        let changed_executors = self
            .in_memory_state
            .write()
            .await
            .update_state(current_state_id, &request.payload, "state_store")
            .map_err(|e| anyhow!("error updating in memory state: {e:?}"))?;
        // Notify the executors with state changes
        {
            let mut executor_states = self.executor_states.write().await;
            for executor_id in changed_executors {
                if let Some(executor_state) = executor_states.get_mut(&executor_id) {
                    executor_state.notify();
                }
            }
        }

        if !new_state_changes.is_empty() &&
            let Err(err) = self.change_events_tx.send(())
        {
            error!("failed to notify of state change event, ignoring: {err:?}",);
        }

        if request.notify_usage_events() &&
            let Err(err) = self.usage_events_tx.send(())
        {
            error!("failed to notify of usage event, ignoring: {err:?}",);
        }

        self.handle_request_state_changes(&request).await;

        // This needs to be after the transaction is committed because if the gc
        // runs before the gc urls are written, the gc process will not see the
        // urls.
        match &request.payload {
            RequestPayload::DeleteApplicationRequest(_) |
            RequestPayload::DeleteRequestRequest(_) => {
                self.gc_tx.send(()).unwrap();
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_request_state_changes(&self, update_request: &StateMachineUpdateRequest) {
        if self.function_run_event_tx.receiver_count() == 0 {
            return;
        }
        match &update_request.payload {
            RequestPayload::InvokeApplication(request) => {
                let _ = self
                    .function_run_event_tx
                    .send(RequestStateChangeEvent::RequestStarted(
                        RequestStartedEvent {
                            request_id: request.ctx.request_id.clone(),
                        },
                    ));
            }
            RequestPayload::UpsertExecutor(request) => {
                for allocation_output in &request.allocation_outputs {
                    let ev = RequestStateChangeEvent::from_finished_function_run(
                        allocation_output.clone(),
                    );
                    let _ = self.function_run_event_tx.send(ev);
                }
            }
            RequestPayload::SchedulerUpdate((sched_update, _)) => {
                for allocation in &sched_update.new_allocations {
                    let _ = self.function_run_event_tx.send(
                        RequestStateChangeEvent::FunctionRunAssigned(
                            request_events::FunctionRunAssigned {
                                request_id: allocation.request_id.clone(),
                                function_name: allocation.function.clone(),
                                function_run_id: allocation.function_call_id.to_string(),
                                executor_id: allocation.target.executor_id.get().to_string(),
                                allocation_id: allocation.id.to_string(),
                            },
                        ),
                    );
                }

                for (ctx_key, function_call_ids) in &sched_update.updated_function_runs {
                    for function_call_id in function_call_ids {
                        let ctx = sched_update.updated_request_states.get(ctx_key).cloned();
                        let function_run =
                            ctx.and_then(|ctx| ctx.function_runs.get(function_call_id).cloned());
                        if let Some(function_run) = function_run {
                            let _ = self.function_run_event_tx.send(
                                RequestStateChangeEvent::FunctionRunCreated(
                                    request_events::FunctionRunCreated {
                                        request_id: function_run.request_id.clone(),
                                        function_name: function_run.name.clone(),
                                        function_run_id: function_run.id.to_string(),
                                    },
                                ),
                            );
                        }
                    }
                }

                for request_ctx in sched_update.updated_request_states.values() {
                    if request_ctx.outcome.is_some() {
                        let _ = self.function_run_event_tx.send(
                            RequestStateChangeEvent::RequestFinished(RequestFinishedEvent {
                                request_id: request_ctx.request_id.clone(),
                            }),
                        );
                    }
                }
            }
            _ => {}
        }
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone(), self.metrics.clone())
    }

    pub fn function_run_event_stream(&self) -> broadcast::Receiver<RequestStateChangeEvent> {
        self.function_run_event_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use requests::{CreateOrUpdateApplicationRequest, InvokeApplicationRequest, NamespaceRequest};
    use test_state_store::TestStateStore;

    use super::*;
    use crate::data_model::{
        Application,
        InputArgs,
        Namespace,
        RequestCtxBuilder,
        StateChangeId,
        test_objects::tests::{
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
            mock_application,
            mock_data_payload,
            mock_function_call,
        },
    };

    #[tokio::test]
    async fn test_create_and_list_namespaces() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;

        // Create namespaces
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace1".to_string(),
                    blob_storage_bucket: None,
                    blob_storage_region: None,
                }),
            })
            .await?;
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace2".to_string(),
                    blob_storage_bucket: Some("bucket2".to_string()),
                    blob_storage_region: Some("local".to_string()),
                }),
            })
            .await?;

        // List namespaces
        let reader = indexify_state.reader();
        let result = reader
            .get_all_rows_from_cf::<Namespace>(IndexifyObjectsColumns::Namespaces)
            .unwrap();
        let namespaces = result
            .iter()
            .map(|(_, ns)| ns.clone())
            .collect::<Vec<Namespace>>();

        // Check if the namespaces were created
        assert!(namespaces.iter().any(|ns| ns.name == "namespace1"));
        assert!(namespaces.iter().any(|ns| ns.name == "namespace2"));
        assert!(
            namespaces
                .iter()
                .any(|ns| ns.blob_storage_bucket == Some("bucket2".to_string()))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_version_bump_and_graph_update() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;

        // Create a compute graph and write it
        let application = mock_application();
        _write_to_test_state_store(&indexify_state, application).await?;

        // Read the compute graph
        let applications = _read_cgs_from_state_store(&indexify_state);

        // Check if the compute graph was created
        assert!(applications.iter().any(|cg| cg.name == "graph_A"));

        for i in 2..4 {
            // Update the graph
            let mut application = mock_application();
            application.version = i.to_string();

            _write_to_test_state_store(&indexify_state, application).await?;

            // Read it again
            let application = _read_cgs_from_state_store(&indexify_state);

            // Verify the name is the same. Verify the version is different.
            assert!(application.iter().any(|cg| cg.name == "graph_A"));
            assert_eq!(application[0].version, i.to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_order_state_changes() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;
        let tx = indexify_state.db.transaction();
        let function_run = tests::mock_application()
            .to_version()
            .unwrap()
            .create_function_run(
                &mock_function_call(),
                vec![InputArgs {
                    function_call_id: None,
                    data_payload: mock_data_payload(),
                }],
                "foo1",
            )?;

        let ctx = RequestCtxBuilder::default()
            .namespace("namespace1".to_string())
            .application_name("cg1".to_string())
            .request_id("foo1".to_string())
            .function_calls(HashMap::from([(
                function_run.id.clone(),
                mock_function_call(),
            )]))
            .function_runs(HashMap::from([(
                function_run.id.clone(),
                function_run.clone(),
            )]))
            .application_version("1".to_string())
            .build()?;
        let state_change_1 = state_changes::invoke_application(
            &indexify_state.state_change_id_seq,
            &InvokeApplicationRequest {
                namespace: "namespace".to_string(),
                application_name: "graph_A".to_string(),
                ctx: ctx.clone(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_1).unwrap();
        tx.commit().unwrap();

        let tx = indexify_state.db.transaction();
        let state_change_2 = state_changes::upsert_executor(
            &indexify_state.state_change_id_seq,
            &TEST_EXECUTOR_ID.into(),
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_2).unwrap();
        tx.commit().unwrap();

        let tx = indexify_state.db.transaction();
        let state_change_3 = state_changes::invoke_application(
            &indexify_state.state_change_id_seq,
            &InvokeApplicationRequest {
                namespace: "namespace".to_string(),
                application_name: "graph_A".to_string(),
                ctx: ctx.clone(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_3).unwrap();
        tx.commit().unwrap();

        let state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)
            .unwrap();
        assert_eq!(state_changes.changes.len(), 3);
        // global state_change_2
        assert_eq!(state_changes.changes[0].id, StateChangeId::new(1));
        // state_change_1
        assert_eq!(state_changes.changes[1].id, StateChangeId::new(0));
        // state_change_3
        assert_eq!(state_changes.changes[2].id, StateChangeId::new(2));
        Ok(())
    }

    #[tokio::test]
    async fn test_load_database_with_column_families() -> Result<()> {
        // IMPORTANT:
        // These columns families match the ones defined in the production state store.

        // Do NOT remove any of the column families hardcoded below.
        // Do add new column families here when they are added to the
        // IndexifyObjectsColumns enum.

        // If one of them is removed or renamed, Indexify server won't start because
        // it won't be able to open the database with all column families.
        //
        // This test is here to guarantee that if a variant is removed from the
        // IndexifyObjectsColumns enum, this test will fail.

        // If you want to remove a column family, you need to do it via a migration.
        // See migrations module for more details.
        let columns = vec![
            "StateMachineMetadata",
            "Namespaces",
            "Applications",
            "ApplicationVersions",
            "RequestCtx",
            "RequestCtxSecondaryIndex",
            "UnprocessedStateChanges",
            "Allocations",
            "GcUrls",
            "Stats",
        ];

        let columns_iter = columns
            .clone()
            .into_iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        let tmp_dir = tempfile::tempdir()?;
        let path = tmp_dir.path().to_path_buf();

        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        let db = open_database(
            path.clone(),
            RocksDBConfig::default(),
            columns_iter,
            state_store_metrics.clone(),
        )?;
        for name in &columns {
            db.put(name, b"key", b"value")?;
        }
        drop(db);

        assert_eq!(
            columns.into_iter().map(String::from).collect::<Vec<_>>(),
            IndexifyObjectsColumns::iter()
                .map(|cf| cf.to_string())
                .collect::<Vec<_>>()
        );

        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        open_database(
            path,
            RocksDBConfig::default(),
            sm_column_families,
            state_store_metrics,
        )
        .expect(
            "failed to open database with the column families defined in IndexifyObjectsColumns",
        );

        Ok(())
    }

    fn _read_cgs_from_state_store(indexify_state: &IndexifyState) -> Vec<Application> {
        let reader = indexify_state.reader();
        let result = reader
            .get_all_rows_from_cf::<Application>(IndexifyObjectsColumns::Applications)
            .unwrap();
        let applications = result
            .iter()
            .map(|(_, cg)| cg.clone())
            .collect::<Vec<Application>>();

        applications
    }

    async fn _write_to_test_state_store(
        indexify_state: &Arc<IndexifyState>,
        application: Application,
    ) -> Result<()> {
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: application.clone(),
                        upgrade_requests_to_current_version: false,
                    },
                )),
            })
            .await
    }
}
