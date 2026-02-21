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
use in_memory_state::InMemoryState;
use opentelemetry::KeyValue;
use request_events::RequestStateChangeEvent;
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{RwLock, watch};

/// Channel capacity for request state change event broadcast.
/// This provides backpressure if workers are slow.
const REQUEST_EVENT_CHANNEL_CAPACITY: usize = 10000;
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{
        ContainerPoolKey,
        ExecutorId,
        FunctionRunStatus,
        StateChange,
        StateMachineMetadata,
    },
    metrics::{StateStoreMetrics, Timer},
    processor::container_scheduler::{ContainerScheduler, ContainerSchedulerGauges},
    state_store::{
        driver::{
            Transaction,
            Writer,
            rocksdb::{RocksDBConfig, RocksDBDriver},
        },
        in_memory_metrics::InMemoryStoreGauges,
        serializer::{StateStoreEncode, StateStoreEncoder},
    },
};

pub mod executor_watches;
pub mod request_event_buffers;
use executor_watches::ExecutorWatches;
use request_event_buffers::RequestEventBuffers;

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
    pub db: Arc<RocksDBDriver>,
    pub executor_states: RwLock<HashMap<ExecutorId, ExecutorState>>,
    pub db_version: u64,

    pub state_change_id_seq: Arc<AtomicU64>,
    pub usage_event_id_seq: Arc<AtomicU64>,
    pub request_event_id_seq: Arc<AtomicU64>,

    pub change_events_tx: watch::Sender<()>,
    pub change_events_rx: watch::Receiver<()>,
    pub usage_events_tx: watch::Sender<()>,
    pub usage_events_rx: watch::Receiver<()>,
    /// Broadcast channel for request state change events.
    /// SSE worker and HTTP export worker both subscribe to this channel.
    pub request_events_tx: async_broadcast::Sender<RequestStateChangeEvent>,
    /// Keep the initial receiver alive to prevent channel from being closed.
    /// New receivers are created via sender.new_receiver() which clone from
    /// this.
    _request_events_rx: async_broadcast::InactiveReceiver<RequestStateChangeEvent>,

    pub metrics: Arc<StateStoreMetrics>,
    pub in_memory_state: Arc<RwLock<in_memory_state::InMemoryState>>,
    pub container_scheduler: Arc<RwLock<ContainerScheduler>>,
    // Executor watches for function call results streaming
    pub executor_watches: ExecutorWatches,
    pub request_event_buffers: RequestEventBuffers,
    // Observable gauges - must be kept alive for callbacks to fire
    _in_memory_store_gauges: InMemoryStoreGauges,
    _container_scheduler_gauges: ContainerSchedulerGauges,
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

struct PersistentWriteResult {
    current_state_id: u64,
    should_notify_usage_reporter: bool,
    new_state_changes: Vec<StateChange>,
    /// Request state change events to broadcast (not persisted to DB anymore)
    request_state_changes: Vec<RequestStateChangeEvent>,
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
        #[cfg(feature = "migrations")]
        let sm_meta = migration_runner::run(&path, config.clone()).await?;

        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        let db = Arc::new(open_database(
            path,
            config,
            sm_column_families,
            state_store_metrics.clone(),
        )?);

        #[cfg(not(feature = "migrations"))]
        let sm_meta = read_sm_meta(&db).await?;

        let (change_events_tx, change_events_rx) = tokio::sync::watch::channel(());
        let (usage_events_tx, usage_events_rx) = tokio::sync::watch::channel(());

        let (mut request_events_tx, request_events_rx) =
            async_broadcast::broadcast::<RequestStateChangeEvent>(REQUEST_EVENT_CHANNEL_CAPACITY);
        // Set overflow mode to drop old messages when channel is full
        request_events_tx.set_overflow(true);
        // Don't wait for active receivers - return immediately even if no one is
        // listening
        request_events_tx.set_await_active(false);
        // Deactivate the initial receiver but keep it alive to prevent channel closure
        let _request_events_rx = request_events_rx.deactivate();

        let state_reader = scanner::StateReader::new(db.clone(), state_store_metrics.clone());

        let in_memory_state = InMemoryState::new(
            sm_meta.last_change_idx,
            state_reader.clone(),
            executor_catalog,
        )
        .await?;

        let container_scheduler = Arc::new(RwLock::new(
            ContainerScheduler::new(in_memory_state.clock, &state_reader).await?,
        ));
        let container_scheduler_gauges = ContainerSchedulerGauges::new(container_scheduler.clone());
        let indexes = Arc::new(RwLock::new(in_memory_state));
        let in_memory_store_gauges = InMemoryStoreGauges::new(indexes.clone());

        let s = Arc::new(Self {
            db,
            db_version: sm_meta.db_version,
            state_change_id_seq: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            usage_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_usage_idx)),
            request_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_request_event_idx)),
            executor_states: RwLock::new(HashMap::new()),
            metrics: state_store_metrics,
            change_events_tx,
            change_events_rx,
            in_memory_state: indexes,
            container_scheduler,
            usage_events_tx,
            usage_events_rx,
            request_events_tx,
            _request_events_rx,
            executor_watches: ExecutorWatches::new(),
            request_event_buffers: RequestEventBuffers::new(),
            _in_memory_store_gauges: in_memory_store_gauges,
            _container_scheduler_gauges: container_scheduler_gauges,
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
    pub async fn write(&self, mut request: StateMachineUpdateRequest) -> Result<()> {
        debug!("writing state machine update request: {:#?}", request);
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _timer = Timer::start_with_labels(&self.metrics.state_write, timer_kv);

        let write_result = self
            .write_in_persistent_store(&mut request, timer_kv)
            .await?;

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
        {
            let _timer =
                Timer::start_with_labels(&self.metrics.state_write_container_scheduler, timer_kv);
            self.container_scheduler
                .write()
                .await
                .update(&request.payload)
                .map_err(|e| anyhow!("error updating container scheduler: {e:?}"))?;
        }

        if let RequestPayload::SchedulerUpdate(payload) = &request.payload {
            let impacted_executors = self
                .executor_watches
                .impacted_executors(
                    &payload.update.updated_function_runs,
                    &payload.update.updated_request_states,
                )
                .await;
            changed_executors.extend(impacted_executors.into_iter().map(|e| e.into()));

            for executor_id in payload.update.updated_executor_states.keys() {
                changed_executors.insert(executor_id.clone());
            }

            // Notify executors that have containers with updated state so they
            // receive a fresh desired_state push.  Without this, changes like
            // container terminations (e.g. sandbox deletion) would go unnoticed
            // until the next heartbeat reconciliation.
            for container_meta in payload.update.containers.values() {
                changed_executors.insert(container_meta.executor_id.clone());
            }
        }
        // Notify executors when a container pool is deleted so they terminate
        // containers immediately rather than waiting for the next poll cycle.
        if let RequestPayload::DeleteContainerPool((delete_req, _)) = &request.payload {
            let pool_key = ContainerPoolKey::new(&delete_req.namespace, &delete_req.pool_id);
            let container_scheduler = self.container_scheduler.read().await;
            for (_, meta) in container_scheduler.function_containers.iter() {
                if meta.function_container.belongs_to_pool(&pool_key) {
                    changed_executors.insert(meta.executor_id.clone());
                }
            }
        }
        // Notify executors when an application is deleted so they terminate
        // containers immediately rather than waiting for the stream to reconnect.
        if let RequestPayload::DeleteApplicationRequest((delete_req, _)) = &request.payload {
            let container_scheduler = self.container_scheduler.read().await;
            for (_, meta) in container_scheduler.function_containers.iter() {
                if meta.function_container.namespace == delete_req.namespace &&
                    meta.function_container.application_name == delete_req.name
                {
                    changed_executors.insert(meta.executor_id.clone());
                }
            }
        }
        if let RequestPayload::UpsertExecutor(req) = &request.payload &&
            !req.watch_function_calls.is_empty()
        {
            // Check if any watched function runs have already completed.
            // This handles the race condition where children complete before watches
            // are registered, while avoiding spurious notifications on server restart.
            let in_memory = self.in_memory_state.read().await;
            let mut completed_watches = Vec::new();
            for watch in &req.watch_function_calls {
                let key = in_memory_state::FunctionRunKey::from(watch);
                if let Some(run) = in_memory.function_runs.get(&key) &&
                    matches!(run.status, FunctionRunStatus::Completed)
                {
                    completed_watches.push(watch.function_call_id.clone());
                }
            }
            drop(in_memory);

            if !completed_watches.is_empty() {
                info!(
                    executor_id = req.executor.id.get(),
                    num_completed_watches = completed_watches.len(),
                    completed_watches = ?completed_watches,
                    "notifying executor: watched function runs already completed"
                );
                changed_executors.insert(req.executor.id.clone());
            } else {
                info!(
                    executor_id = req.executor.id.get(),
                    num_watches = req.watch_function_calls.len(),
                    "executor has watches but none completed yet, will notify via SchedulerUpdate path"
                );
            }
        }

        // Notify the executors with state changes
        {
            let _timer =
                Timer::start_with_labels(&self.metrics.state_write_executor_notify, timer_kv);
            let mut executor_states = self.executor_states.write().await;
            for executor_id in &changed_executors {
                info!(
                    executor_id = executor_id.get(),
                    "notifying executor of state change"
                );
                if let Some(executor_state) = executor_states.get_mut(executor_id) {
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

            // Broadcast request state change events to SSE and HTTP export workers
            for event in write_result.request_state_changes {
                if let Err(err) = self.request_events_tx.broadcast(event).await {
                    error!("failed to broadcast request state change event, ignoring: {err:?}",);
                }
            }
        }

        Ok(())
    }

    async fn write_in_persistent_store(
        &self,
        request: &mut StateMachineUpdateRequest,
        timer_kv: &[KeyValue],
    ) -> Result<PersistentWriteResult> {
        let _timer =
            Timer::start_with_labels(&self.metrics.state_write_persistent_storage, timer_kv);

        // Build request state change events BEFORE preparing the payload.
        // This uses FunctionRun::is_new() which checks if created_at_clock is None
        // to identify newly created function runs that need FunctionRunCreated events.
        let request_state_changes = request_events::build_request_state_change_events(request);

        // Get the current clock value for setting created_at_clock and
        // updated_at_clock.
        let current_clock = self.state_change_id_seq.load(atomic::Ordering::Relaxed);

        // Prepare the payload with clocks AFTER building events.
        // This mutates the payload in-place, so both the persistent store and
        // in-memory store will receive the same prepared objects.
        request.prepare_for_persistence(current_clock);

        let txn = self.db.transaction();

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
            RequestPayload::SchedulerUpdate(payload) => {
                let scheduler_result = state_machine::handle_scheduler_update(
                    &txn,
                    &payload.update,
                    Some(&self.usage_event_id_seq),
                    current_clock,
                )
                .await?;
                if scheduler_result.usage_recorded {
                    should_notify_usage_reporter = true;
                }
                state_machine::mark_state_changes_processed(&txn, &payload.processed_state_changes)
                    .await?;
            }
            RequestPayload::CreateNameSpace(namespace_request) => {
                state_machine::upsert_namespace(self.db.clone(), namespace_request, current_clock)
                    .await?;
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                state_machine::create_or_update_application(
                    &txn,
                    req.application.clone(),
                    req.upgrade_requests_to_current_version,
                    &req.container_pools,
                    current_clock,
                )
                .await?;
            }
            RequestPayload::DeleteApplicationRequest((request, processed_state_changes)) => {
                state_machine::delete_application(
                    &txn,
                    &request.namespace,
                    &request.name,
                    current_clock,
                )
                .await?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            RequestPayload::DeleteRequestRequest((request, processed_state_changes)) => {
                state_machine::delete_request(&txn, request, current_clock).await?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            RequestPayload::UpsertExecutor(request) => {
                // Create state changes for allocation outputs. The actual allocation
                // updates are handled by the application processor to remove contention
                // from the ingestion path.
                for allocation_output in &request.allocation_outputs {
                    info!(
                        request_id = %allocation_output.allocation.request_id,
                        executor_id = %allocation_output.allocation.target.executor_id,
                        app = %allocation_output.allocation.application,
                        fn = %allocation_output.allocation.function,
                        allocation_id = %allocation_output.allocation.id,
                        allocation_outcome = ?allocation_output.allocation.outcome,
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
                if !request.watch_function_calls.is_empty() {
                    info!(
                        executor_id = request.executor.id.get(),
                        num_watches = request.watch_function_calls.len(),
                        watches = ?request.watch_function_calls.iter().map(|w| format!("{}:{}", w.function_call_id, w.request_id)).collect::<Vec<_>>(),
                        "syncing executor watches to state store"
                    );
                }
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
            RequestPayload::CreateSandbox(request) => {
                state_machine::upsert_sandbox(&txn, &request.sandbox, current_clock).await?;
            }
            RequestPayload::CreateSnapshot(request) => {
                state_machine::upsert_snapshot(&txn, &request.snapshot, current_clock).await?;
            }
            RequestPayload::DeleteSnapshot(request) => {
                state_machine::delete_snapshot(&txn, &request.namespace, request.snapshot_id.get())
                    .await?;
            }
            RequestPayload::TerminateSandbox(request) => {
                // Update sandbox status to terminating
                state_machine::upsert_sandbox(&txn, &request.sandbox, current_clock).await?;
            }
            RequestPayload::CreateContainerPool(request) => {
                state_machine::upsert_container_pool(&txn, &request.pool, current_clock).await?;
            }
            RequestPayload::UpdateContainerPool(request) => {
                state_machine::upsert_container_pool(&txn, &request.pool, current_clock).await?;
            }
            RequestPayload::TombstoneContainerPool(_) => {}
            RequestPayload::DeleteContainerPool((request, processed_state_changes)) => {
                state_machine::delete_container_pool(
                    &txn,
                    &request.namespace,
                    request.pool_id.get(),
                )
                .await?;
                state_machine::mark_state_changes_processed(&txn, processed_state_changes).await?;
            }
            _ => {} // Handle other request types as needed
        };

        let mut new_state_changes = request.state_changes(&self.state_change_id_seq)?;
        new_state_changes.extend(allocation_ingestion_events);
        if !new_state_changes.is_empty() {
            state_machine::save_state_changes(&txn, &new_state_changes, current_clock).await?;
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
            request_state_changes,
        })
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone(), self.metrics.clone())
    }

    pub async fn subscribe_request_events(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> tokio::sync::broadcast::Receiver<RequestStateChangeEvent> {
        self.request_event_buffers
            .subscribe(namespace, application, request_id)
            .await
    }

    pub async fn unsubscribe_request_events(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) {
        self.request_event_buffers
            .unsubscribe(namespace, application, request_id)
            .await
    }

    pub async fn push_request_event(&self, event: RequestStateChangeEvent) {
        self.request_event_buffers.push_event(event).await;
    }

    /// Get a new receiver for request state change events.
    /// Used by SSE worker and HTTP export worker to subscribe to events.
    pub fn subscribe_request_state_changes(
        &self,
    ) -> async_broadcast::Receiver<RequestStateChangeEvent> {
        self.request_events_tx.new_receiver()
    }
}

/// Read state machine metadata from the database.
///
/// Handles both legacy JSON format (pre-V13) and the current postcard format,
/// since this may be called on databases that haven't been migrated yet.
#[cfg(not(feature = "migrations"))]
async fn read_sm_meta(db: &RocksDBDriver) -> Result<StateMachineMetadata> {
    let meta = db
        .get(
            IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
            b"sm_meta",
        )
        .await?;
    match meta {
        Some(meta) => {
            if meta.is_empty() {
                return Ok(StateMachineMetadata {
                    db_version: 0,
                    last_change_idx: 0,
                    last_usage_idx: 0,
                    last_request_event_idx: 0,
                });
            }
            // Try postcard (0x01 prefix) first, fall back to JSON for pre-V13 DBs
            if meta[0] == 0x01 {
                StateStoreEncoder::decode(&meta)
            } else {
                serde_json::from_slice(&meta).map_err(|e| {
                    anyhow::anyhow!(
                        "failed to decode StateMachineMetadata as JSON or postcard: {}",
                        e
                    )
                })
            }
        }
        None => Ok(StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
            last_usage_idx: 0,
            last_request_event_idx: 0,
        }),
    }
}

pub async fn write_sm_meta(txn: &Transaction, sm_meta: &StateMachineMetadata) -> Result<()> {
    let serialized_meta = StateStoreEncoder::encode(sm_meta)?;
    txn.put(
        IndexifyObjectsColumns::StateMachineMetadata.as_ref(),
        b"sm_meta",
        &serialized_meta,
    )
    .await?;
    Ok(())
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
        state_machine::save_state_changes(&tx, &state_change_1, 0).await?;
        tx.commit().await?;

        let tx = indexify_state.db.transaction();
        let state_change_2 = state_changes::upsert_executor(
            &indexify_state.state_change_id_seq,
            &TEST_EXECUTOR_ID.into(),
        )
        .unwrap();
        state_machine::save_state_changes(&tx, &state_change_2, 0).await?;
        tx.commit().await?;

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
        state_machine::save_state_changes(&tx, &state_change_3, 0).await?;
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
            "AllocationUsage",
            "GcUrls",
            "Stats",
            "ExecutorStateChanges",
            "ApplicationStateChanges",
            "RequestStateChangeEvents",
            "Sandboxes",
            "ContainerPools",
            "FunctionPools",
            "SandboxPools",
            "FunctionRuns",
            "FunctionCalls",
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
            db.put(name, b"key", b"value").await?;
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
                        container_pools: vec![],
                    },
                )),
            })
            .await
    }
}
