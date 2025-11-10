use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{self, AtomicU64},
    },
};

use anyhow::{Result, anyhow};
use in_memory_state::InMemoryState;
use opentelemetry::KeyValue;
use request_events::RequestStateChangeEvent;
use requests::{RequestPayload, StateMachineUpdateRequest};
use state_machine::IndexifyObjectsColumns;
use tokio::sync::{RwLock, broadcast, watch};
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{ExecutorId, StateChange, StateMachineMetadata},
    metrics::{StateStoreMetrics, Timer},
    state_store::{
        in_memory_metrics::InMemoryStoreGauges,
        driver::Transaction,
        request_events::RequestStartedEvent,
        serializer::{JsonEncode, JsonEncoder},
    },
};

pub mod executor_watches;
use executor_watches::ExecutorWatches;

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
pub mod in_memory_metrics;
pub mod in_memory_state;
#[cfg(feature = "migrations")]
pub mod migration_runner;
#[cfg(feature = "migrations")]
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
    pub db: Arc<dyn driver::Driver>,
    pub executor_states: RwLock<HashMap<ExecutorId, ExecutorState>>,
    pub db_version: u64,

    pub state_change_id_seq: Arc<AtomicU64>,
    pub usage_event_id_seq: Arc<AtomicU64>,
    pub request_event_id_seq: Arc<AtomicU64>,

    pub function_run_event_tx: broadcast::Sender<RequestStateChangeEvent>,
    pub change_events_tx: watch::Sender<()>,
    pub change_events_rx: watch::Receiver<()>,
    pub usage_events_tx: watch::Sender<()>,
    pub usage_events_rx: watch::Receiver<()>,
    pub request_events_tx: watch::Sender<()>,
    pub request_events_rx: watch::Receiver<()>,

    pub metrics: Arc<StateStoreMetrics>,
    pub in_memory_state: Arc<RwLock<in_memory_state::InMemoryState>>,
    // keep handle to in_memory_state metrics to avoid dropping it
    // Executor watches for function call results streaming
    pub executor_watches: ExecutorWatches,
    // Observable gauge for tracking total executors - must be kept alive for callback to fire
    _in_memory_store_gauges: InMemoryStoreGauges,
}

pub(crate) fn open_database(
    options: driver::ConnectionOptions,
    metrics: Arc<StateStoreMetrics>,
) -> Result<Arc<dyn driver::Driver>, driver::Error> {
    driver::open_database(options, metrics)
}

struct PersistentWriteResult {
    current_state_id: u64,
    should_notify_usage_reporter: bool,
    should_notify_request_events: bool,
    new_state_changes: Vec<StateChange>,
}

impl IndexifyState {
    pub async fn new(
        driver_options: driver::ConnectionOptions,
        executor_catalog: ExecutorCatalog,
    ) -> Result<Arc<Self>> {
        // Migrate the db before opening with all column families.
        // This is because the migration process may delete older column families.
        // If we open the db with all column families, it would fail to open.
        #[cfg(feature = "migrations")]
        let sm_meta = migration_runner::run(&driver_options, config.clone())?;

        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        let db = open_database(driver_options, state_store_metrics.clone())?;

        #[cfg(not(feature = "migrations"))]
        let sm_meta = read_sm_meta(db.clone()).await?;

        let (task_event_tx, _) = tokio::sync::broadcast::channel(100);
        let (change_events_tx, change_events_rx) = tokio::sync::watch::channel(());
        let (usage_events_tx, usage_events_rx) = tokio::sync::watch::channel(());
        let (request_events_tx, request_events_rx) = tokio::sync::watch::channel(());

        let indexes = Arc::new(RwLock::new(
            InMemoryState::new(
                sm_meta.last_change_idx,
                scanner::StateReader::new(db.clone(), state_store_metrics.clone()),
                executor_catalog,
            )
            .await?,
        ));

        let in_memory_store_gauges = InMemoryStoreGauges::new(indexes.clone());

        let s = Arc::new(Self {
            db,
            db_version: sm_meta.db_version,
            state_change_id_seq: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            usage_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_usage_idx)),
            request_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_request_event_idx)),
            executor_states: RwLock::new(HashMap::new()),
            function_run_event_tx: task_event_tx,
            metrics: state_store_metrics,
            change_events_tx,
            change_events_rx,
            in_memory_state: indexes,
            usage_events_tx,
            usage_events_rx,
            request_events_tx,
            request_events_rx,
            executor_watches: ExecutorWatches::new(),
            _in_memory_store_gauges: in_memory_store_gauges,
        });

        info!(
            application_state_change_id = s.state_change_id_seq.load(atomic::Ordering::Relaxed),
            "initialized state store with last state change ids",
        );

        info!(
            usage_event_id = s.usage_event_id_seq.load(atomic::Ordering::Relaxed),
            "initialized state store with last usage id",
        );

        info!(
            request_event_id = s.request_event_id_seq.load(atomic::Ordering::Relaxed),
            "initialized state store with last request event id",
        );

        info!(db_version = sm_meta.db_version, "db version discovered");

        Ok(s)
    }

    #[tracing::instrument(
        skip(self, request),
        fields(
            request_type = request.payload.to_string(),
        )
    )]
    pub async fn write(&self, request: StateMachineUpdateRequest) -> Result<()> {
        debug!("writing state machine update request: {:#?}", request);
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _timer = Timer::start_with_labels(&self.metrics.state_write, timer_kv);

        let write_result = self.write_in_persistent_store(&request, timer_kv).await?;

        let mut changed_executors = {
            let _timer = Timer::start_with_labels(&self.metrics.state_write_in_memory, timer_kv);
            self.in_memory_state
                .write()
                .await
                .update_state(
                    write_result.current_state_id,
                    &request.payload,
                    "state_store",
                )
                .map_err(|e| anyhow!("error updating in memory state: {e:?}"))?
        };

        if let RequestPayload::SchedulerUpdate((request, _)) = &request.payload {
            let impacted_executors = self
                .executor_watches
                .impacted_executors(
                    &request.updated_function_runs,
                    &request.updated_request_states,
                )
                .await;
            changed_executors.extend(impacted_executors.into_iter().map(|e| e.into()));
        }
        if let RequestPayload::UpsertExecutor(req) = &request.payload &&
            !req.watch_function_calls.is_empty() &&
            req.update_executor_state
        {
            changed_executors.insert(req.executor.id.clone());
        }

        // Notify the executors with state changes
        {
            let _timer =
                Timer::start_with_labels(&self.metrics.state_write_executor_notify, timer_kv);
            let mut executor_states = self.executor_states.write().await;
            for executor_id in changed_executors {
                if let Some(executor_state) = executor_states.get_mut(&executor_id) {
                    executor_state.notify();
                }
            }
        }

        {
            let _timer = Timer::start_with_labels(&self.metrics.state_change_notify, timer_kv);
            if !write_result.new_state_changes.is_empty() &&
                let Err(err) = self.change_events_tx.send(())
            {
                error!("failed to notify of state change event, ignoring: {err:?}",);
            }

            if write_result.should_notify_usage_reporter &&
                let Err(err) = self.usage_events_tx.send(())
            {
                error!("failed to notify of usage event, ignoring: {err:?}",);
            }

            if write_result.should_notify_request_events &&
                let Err(err) = self.request_events_tx.send(())
            {
                error!("failed to notify of request state change event, ignoring: {err:?}",);
            }
        }

        Ok(())
    }

    async fn write_in_persistent_store(
        &self,
        request: &StateMachineUpdateRequest,
        timer_kv: &[KeyValue],
    ) -> Result<PersistentWriteResult> {
        let _timer =
            Timer::start_with_labels(&self.metrics.state_write_persistent_storage, timer_kv);
        let txn = self.db.transaction()?;

        let mut should_notify_usage_reporter = false;
        let mut allocation_ingestion_events = Vec::new();

        match &request.payload {
            RequestPayload::InvokeApplication(invoke_application_request) => {
                let _enter = span!(
                    tracing::Level::INFO,
                    "invoke_application",
                    namespace = invoke_application_request.namespace.clone(),
                    request_id = invoke_application_request.ctx.request_id.clone(),
                    app = invoke_application_request.application_name.clone(),
                );
                state_machine::create_request(&txn, invoke_application_request).await?;
            }
            RequestPayload::SchedulerUpdate((request, processed_state_changes)) => {
                let scheduler_result = state_machine::handle_scheduler_update(
                    &txn,
                    request,
                    Some(&self.usage_event_id_seq),
                )
                .await?;
                if scheduler_result.usage_recorded {
                    should_notify_usage_reporter = true;
                }
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            RequestPayload::CreateNameSpace(namespace_request) => {
                state_machine::upsert_namespace(self.db.clone(), namespace_request).await?;
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                state_machine::create_or_update_application(
                    &txn,
                    req.application.clone(),
                    req.upgrade_requests_to_current_version,
                )
                .await?;
            }
            RequestPayload::DeleteApplicationRequest((request, processed_state_changes)) => {
                state_machine::delete_application(&txn, &request.namespace, &request.name).await?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            RequestPayload::DeleteRequestRequest((request, processed_state_changes)) => {
                state_machine::delete_request(&txn, request).await?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            RequestPayload::UpsertExecutor(request) => {
                // Create state changes for allocation outputs. The actual allocation
                // updates are handled by the application processor to remove contention
                // from the ingestion path.
                for allocation_output in &request.allocation_outputs {
                    info!(
                        request_id = allocation_output.allocation.request_id.as_str(),
                        executor_id = allocation_output.allocation.target.executor_id.get().to_string(),
                        app = allocation_output.allocation.application.as_str(),
                        fn = allocation_output.allocation.function.as_str(),
                        allocation_id = allocation_output.allocation.id.to_string(),
                        allocation_outcome = allocation_output.allocation.outcome.to_string(),
                        "creating allocation ingestion state change",
                    );
                    let changes = state_changes::task_outputs_ingested(
                        &self.state_change_id_seq,
                        allocation_output,
                    )?;
                    allocation_ingestion_events.extend(changes);
                }

                if request.update_executor_state {
                    self.executor_states
                        .write()
                        .await
                        .entry(request.executor.id.clone())
                        .or_default();
                }
                // Update executor watches for efficient sync
                self.executor_watches
                    .sync_watches(
                        request.executor.id.get().to_string(),
                        request.watch_function_calls.clone(),
                    )
                    .await;
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
                // Cleanup all watches for this executor
                self.executor_watches
                    .remove_executor(request.executor_id.get())
                    .await;
            }
            RequestPayload::ProcessStateChanges(state_changes) => {
                state_machine::mark_state_changes_processed(&txn, state_changes).await?;
            }
            _ => {} // Handle other request types as needed
        };

        let mut new_state_changes = request.state_changes(&self.state_change_id_seq)?;
        new_state_changes.extend(allocation_ingestion_events);
        if !new_state_changes.is_empty() {
            state_machine::save_state_changes(&txn, &new_state_changes).await?;
        }

        // Persist request state change events in the same transaction
        let request_state_changes = request_events::build_request_state_change_events(request);
        let should_notify_request_events = !request_state_changes.is_empty();
        if should_notify_request_events {
            state_machine::persist_request_state_change_events(
                &txn,
                request_state_changes,
                &self.request_event_id_seq,
            )
            .await?;
        }

        let current_state_id = self.state_change_id_seq.load(atomic::Ordering::Relaxed);
        let current_usage_sequence_id = self.usage_event_id_seq.load(atomic::Ordering::Relaxed);
        let current_request_event_id = self.request_event_id_seq.load(atomic::Ordering::Relaxed);
        write_sm_meta(
            &txn,
            &StateMachineMetadata {
                last_change_idx: current_state_id,
                last_usage_idx: current_usage_sequence_id,
                last_request_event_idx: current_request_event_id,
                db_version: self.db_version,
            },
        )
        .await?;
        txn.commit().await?;

        Ok(PersistentWriteResult {
            current_state_id,
            new_state_changes,
            should_notify_usage_reporter,
            should_notify_request_events,
        })
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone(), self.metrics.clone())
    }

    pub fn function_run_event_stream(&self) -> broadcast::Receiver<RequestStateChangeEvent> {
        self.function_run_event_tx.subscribe()
    }
}

/// Read state machine metadata from the database
async fn read_sm_meta(db: Arc<dyn driver::Driver>) -> Result<StateMachineMetadata> {
    let meta = db
        .get(
            IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
            b"sm_meta",
        )
        .await?;
    match meta {
        Some(meta) => Ok(JsonEncoder::decode(&meta)?),
        None => Ok(StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
            last_usage_idx: 0,
            last_request_event_idx: 0,
        }),
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
            .await?;
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
        let applications = _read_cgs_from_state_store(&indexify_state).await?;

        // Check if the compute graph was created
        assert!(applications.iter().any(|cg| cg.name == "graph_A"));

        for i in 2..4 {
            // Update the graph
            let mut application = mock_application();
            application.version = i.to_string();

            _write_to_test_state_store(&indexify_state, application).await?;

            // Read it again
            let application = _read_cgs_from_state_store(&indexify_state).await?;

            // Verify the name is the same. Verify the version is different.
            assert!(application.iter().any(|cg| cg.name == "graph_A"));
            assert_eq!(application[0].version, i.to_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_order_state_changes() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;
        let tx = indexify_state.db.transaction()?;
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
        state_machine::save_state_changes(&tx, &state_change_1).await?;
        tx.commit().await?;

        let tx = indexify_state.db.transaction()?;
        let state_change_2 = state_changes::upsert_executor(
            &indexify_state.state_change_id_seq,
            &TEST_EXECUTOR_ID.into(),
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_2).await?;
        tx.commit().await?;

        let tx = indexify_state.db.transaction()?;
        let state_change_3 = state_changes::invoke_application(
            &indexify_state.state_change_id_seq,
            &InvokeApplicationRequest {
                namespace: "namespace".to_string(),
                application_name: "graph_A".to_string(),
                ctx: ctx.clone(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_3).await?;
        tx.commit().await?;

        let state_changes = indexify_state
            .reader()
            .unprocessed_state_changes(&None, &None)
            .await?;
        assert_eq!(state_changes.changes.len(), 3);
        // global state_change_2
        assert_eq!(state_changes.changes[0].id, StateChangeId::new(1));
        // state_change_1
        assert_eq!(state_changes.changes[1].id, StateChangeId::new(0));
        // state_change_3
        assert_eq!(state_changes.changes[2].id, StateChangeId::new(2));
        Ok(())
    }

    async fn _read_cgs_from_state_store(
        indexify_state: &IndexifyState,
    ) -> Result<Vec<Application>> {
        let reader = indexify_state.reader();
        let result = reader
            .get_all_rows_from_cf::<Application>(IndexifyObjectsColumns::Applications)
            .await?;
        let applications = result
            .iter()
            .map(|(_, cg)| cg.clone())
            .collect::<Vec<Application>>();

        Ok(applications)
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

pub async fn write_sm_meta(txn: &Transaction, sm_meta: &StateMachineMetadata) -> Result<()> {
    let serialized_meta = JsonEncoder::encode(sm_meta)?;
    txn.put(
        IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
        b"sm_meta",
        &serialized_meta,
    )
    .await?;
    Ok(())
}
