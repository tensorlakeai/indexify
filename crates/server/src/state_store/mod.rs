use std::{
    collections::{HashMap, HashSet},
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
use tokio::sync::{RwLock, mpsc, watch};

/// Channel capacity for request state change event broadcast.
/// This provides backpressure if workers are slow.
const REQUEST_EVENT_CHANNEL_CAPACITY: usize = 10000;
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{
        Allocation,
        ContainerId,
        ContainerPoolKey,
        ContainerState,
        ExecutorId,
        SandboxKey,
        SandboxStatus,
        SnapshotStatus,
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

pub mod request_event_buffers;
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

/// Typed event pushed onto per-executor channels from `write()`.
/// Consumers convert to proto commands — O(1) per event instead of
/// O(total-state) per notification.
#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    /// New allocation assigned to this executor. Carries full data so
    /// the consumer only needs blob store for URL conversion.
    AllocationCreated(Box<Allocation>),

    /// New container added. Consumer looks up ContainerServerMetadata
    /// + ApplicationVersion from state for proto conversion.
    ContainerAdded(ContainerId),

    /// Container should be removed.
    ContainerRemoved(ContainerId),

    /// Pre-existing container's description changed (e.g. sandbox_id set
    /// on warm-pool claim). Consumer builds an UpdateContainerDescription
    /// command with only the changed fields.
    ContainerDescriptionChanged(ContainerId),

    /// Snapshot a container's filesystem.
    SnapshotContainer {
        container_id: ContainerId,
        snapshot_id: String,
        upload_uri: String,
    },

    /// Fallback: consumer does full state recompute via
    /// get_executor_state() + CommandEmitter.
    /// Used for bulk ops (DeleteApplication, DeleteContainerPool)
    /// and edge cases.
    FullSync,
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
    /// Per-executor event channels for event-driven command streaming.
    /// `write()` pushes typed events; `command_stream_loop` consumes them.
    pub executor_event_channels: RwLock<HashMap<ExecutorId, mpsc::UnboundedSender<ExecutorEvent>>>,
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
            executor_event_channels: RwLock::new(HashMap::new()),
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
        // Snapshot pre-existing containers before updating the scheduler,
        // so we can distinguish new containers from updates in event emission.
        let pre_existing_containers: HashSet<ContainerId> =
            if let RequestPayload::SchedulerUpdate(payload) = &request.payload {
                let cs = self.container_scheduler.read().await;
                payload
                    .update
                    .containers
                    .keys()
                    .filter(|id| cs.function_containers.contains_key(id))
                    .cloned()
                    .collect()
            } else {
                HashSet::new()
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

        // --- Event-driven emission + legacy watch notification ---
        let event_channels = self.executor_event_channels.read().await;

        if let RequestPayload::SchedulerUpdate(payload) = &request.payload {
            // Container events FIRST (before allocations, so consumer sees
            // AddContainer before RunAllocation for the same container)
            for (container_id, meta) in &payload.update.containers {
                if matches!(meta.desired_state, ContainerState::Terminated { .. }) {
                    Self::send_event(
                        &event_channels,
                        &meta.executor_id,
                        ExecutorEvent::ContainerRemoved(container_id.clone()),
                    );
                } else if !pre_existing_containers.contains(container_id) {
                    Self::send_event(
                        &event_channels,
                        &meta.executor_id,
                        ExecutorEvent::ContainerAdded(container_id.clone()),
                    );
                } else {
                    // Pre-existing, non-terminated container included in this
                    // update → its description may have changed (e.g. sandbox_id
                    // set on warm-pool claim).
                    Self::send_event(
                        &event_channels,
                        &meta.executor_id,
                        ExecutorEvent::ContainerDescriptionChanged(container_id.clone()),
                    );
                }
            }

            // Allocation events
            for allocation in &payload.update.new_allocations {
                Self::send_event(
                    &event_channels,
                    &allocation.target.executor_id,
                    ExecutorEvent::AllocationCreated(Box::new(allocation.clone())),
                );
            }

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
        // When a snapshot is requested, send a SnapshotContainer event to the
        // executor hosting the sandbox's container.
        if let RequestPayload::SnapshotSandbox(req) = &request.payload {
            let in_memory = self.in_memory_state.read().await;
            let sandbox_key =
                SandboxKey::new(&req.snapshot.namespace, req.snapshot.sandbox_id.get());
            if let Some(sandbox) = in_memory.sandboxes.get(&sandbox_key) &&
                let (Some(container_id), Some(executor_id)) =
                    (&sandbox.container_id, &sandbox.executor_id)
            {
                let upload_uri = req.upload_uri.clone();
                Self::send_event(
                    &event_channels,
                    executor_id,
                    ExecutorEvent::SnapshotContainer {
                        container_id: container_id.clone(),
                        snapshot_id: req.snapshot.id.get().to_string(),
                        upload_uri,
                    },
                );
                changed_executors.insert(executor_id.clone());
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
                    Self::send_event(&event_channels, &meta.executor_id, ExecutorEvent::FullSync);
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
                    Self::send_event(&event_channels, &meta.executor_id, ExecutorEvent::FullSync);
                }
            }
        }
        drop(event_channels);

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
            RequestPayload::ProcessStateChanges(state_changes) => {
                state_machine::mark_state_changes_processed(&txn, state_changes).await?;
            }
            RequestPayload::CreateSandbox(request) => {
                state_machine::upsert_sandbox(&txn, &request.sandbox, current_clock).await?;
            }
            RequestPayload::SnapshotSandbox(request) => {
                // 1) Persist the new Snapshot object
                state_machine::upsert_snapshot(&txn, &request.snapshot).await?;

                // 2) Transition the sandbox to Snapshotting status
                let reader = scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                if let Some(mut sandbox) = reader
                    .get_sandbox(
                        &request.snapshot.namespace,
                        request.snapshot.sandbox_id.get(),
                    )
                    .await?
                {
                    sandbox.status = SandboxStatus::Snapshotting {
                        snapshot_id: request.snapshot.id.clone(),
                    };
                    sandbox.snapshot_id = Some(request.snapshot.id.clone());
                    state_machine::upsert_sandbox(&txn, &sandbox, current_clock).await?;
                }
            }
            RequestPayload::CompleteSnapshot(request) => {
                // Find the snapshot by ID. Try in-memory first, fall back to DB
                // scan to handle cases where in-memory state is stale.
                let in_memory = self.in_memory_state.read().await;
                let snapshot_opt = in_memory
                    .snapshots
                    .values()
                    .find(|s| s.id == request.snapshot_id)
                    .map(|s| (**s).clone());
                drop(in_memory);

                let snapshot_opt = match snapshot_opt {
                    Some(s) => Some(s),
                    None => {
                        // Fallback: scan DB for the snapshot
                        let reader =
                            scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                        reader
                            .find_snapshot_by_id(request.snapshot_id.get())
                            .await?
                    }
                };

                if let Some(mut snapshot) = snapshot_opt {
                    snapshot.status = SnapshotStatus::Completed;
                    snapshot.snapshot_uri = Some(request.snapshot_uri.clone());
                    snapshot.size_bytes = Some(request.size_bytes);
                    state_machine::upsert_snapshot(&txn, &snapshot).await?;

                    // Terminate the sandbox
                    let sb_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
                    let in_memory = self.in_memory_state.read().await;
                    if let Some(mut sandbox) = in_memory.sandboxes.get(&sb_key).cloned() {
                        sandbox.status = SandboxStatus::Terminated;
                        state_machine::upsert_sandbox(&txn, &sandbox, current_clock).await?;
                    }
                    drop(in_memory);
                } else {
                    error!(
                        snapshot_id = request.snapshot_id.get(),
                        "CompleteSnapshot: snapshot not found in memory or DB"
                    );
                }
            }
            RequestPayload::FailSnapshot(request) => {
                // Find the snapshot by ID. Try in-memory first, fall back to DB
                // scan to handle cases where in-memory state is stale.
                let in_memory = self.in_memory_state.read().await;
                let snapshot_opt = in_memory
                    .snapshots
                    .values()
                    .find(|s| s.id == request.snapshot_id)
                    .map(|s| (**s).clone());
                drop(in_memory);

                let snapshot_opt = match snapshot_opt {
                    Some(s) => Some(s),
                    None => {
                        // Fallback: scan DB for the snapshot
                        let reader =
                            scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                        reader
                            .find_snapshot_by_id(request.snapshot_id.get())
                            .await?
                    }
                };

                if let Some(mut snapshot) = snapshot_opt {
                    snapshot.status = SnapshotStatus::Failed {
                        error: request.error.clone(),
                    };
                    state_machine::upsert_snapshot(&txn, &snapshot).await?;

                    // Revert sandbox to Running
                    let sb_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
                    let in_memory = self.in_memory_state.read().await;
                    if let Some(mut sandbox) = in_memory.sandboxes.get(&sb_key).cloned() {
                        sandbox.status = SandboxStatus::Running;
                        state_machine::upsert_sandbox(&txn, &sandbox, current_clock).await?;
                    }
                    drop(in_memory);
                } else {
                    error!(
                        snapshot_id = request.snapshot_id.get(),
                        "FailSnapshot: snapshot not found in memory or DB"
                    );
                }
            }
            RequestPayload::DeleteSnapshot(request) => {
                state_machine::delete_snapshot(&txn, &request.namespace, request.snapshot_id.get())
                    .await?;
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

    /// Register an event channel for an executor. Returns the receiving end.
    /// Called when `command_stream` RPC connects — must be called BEFORE
    /// initial sync so no events are missed.
    pub async fn register_event_channel(
        &self,
        executor_id: ExecutorId,
    ) -> mpsc::UnboundedReceiver<ExecutorEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        info!(
            executor_id = executor_id.get(),
            "registered executor event channel"
        );
        self.executor_event_channels
            .write()
            .await
            .insert(executor_id, tx);
        rx
    }

    /// Remove the event channel for an executor. Called on disconnect.
    pub async fn deregister_event_channel(&self, executor_id: &ExecutorId) {
        info!(
            executor_id = executor_id.get(),
            "deregistered executor event channel"
        );
        self.executor_event_channels
            .write()
            .await
            .remove(executor_id);
    }

    /// Send an event to an executor's channel. Logs a warning if the channel
    /// is missing or closed.
    fn send_event(
        channels: &HashMap<ExecutorId, mpsc::UnboundedSender<ExecutorEvent>>,
        executor_id: &ExecutorId,
        event: ExecutorEvent,
    ) {
        match channels.get(executor_id) {
            Some(tx) => {
                if tx.send(event).is_err() {
                    info!(
                        executor_id = executor_id.get(),
                        "executor event dropped: channel closed"
                    );
                }
            }
            None => {
                info!(
                    executor_id = executor_id.get(),
                    "executor event dropped: no command stream connected"
                );
            }
        }
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
            "Snapshots",
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

    // --- Event emission tests ---

    use crate::data_model::{
        AllocationBuilder,
        AllocationTarget,
        Container,
        ContainerBuilder,
        ContainerResources,
        ContainerServerMetadata,
        FunctionCallId,
        FunctionRunOutcome,
    };

    fn test_allocation(executor_id: &ExecutorId) -> crate::data_model::Allocation {
        AllocationBuilder::default()
            .target(AllocationTarget::new(
                executor_id.clone(),
                ContainerId::new("c1".to_string()),
            ))
            .function_call_id(FunctionCallId("fc-1".to_string()))
            .namespace("ns".to_string())
            .application("app".to_string())
            .application_version("v1".to_string())
            .function("fn1".to_string())
            .request_id("req-1".to_string())
            .outcome(FunctionRunOutcome::Unknown)
            .input_args(vec![])
            .call_metadata(bytes::Bytes::new())
            .build()
            .unwrap()
    }

    fn test_container(container_id: &str) -> Container {
        ContainerBuilder::default()
            .id(ContainerId::new(container_id.to_string()))
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("fn1".to_string())
            .version("v1".to_string())
            .state(ContainerState::Running)
            .resources(ContainerResources {
                cpu_ms_per_sec: 100,
                memory_mb: 256,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_event_emission_allocation_created() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        // Register event channel
        let mut rx = state.register_event_channel(executor_id.clone()).await;

        // Write a SchedulerUpdate with a new allocation
        let allocation = test_allocation(&executor_id);
        let mut update = requests::SchedulerUpdateRequest::default();
        update.new_allocations.push(allocation.clone());

        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(requests::SchedulerUpdatePayload::new(
                    update,
                )),
            })
            .await?;

        // Verify AllocationCreated event
        let event = rx.try_recv().unwrap();
        assert!(
            matches!(&event, ExecutorEvent::AllocationCreated(a) if a.id == allocation.id),
            "expected AllocationCreated, got {event:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_event_emission_container_added_and_removed() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());
        let mut rx = state.register_event_channel(executor_id.clone()).await;

        // New container → ContainerAdded
        let container = test_container("c1");
        let container_meta = ContainerServerMetadata::new(
            executor_id.clone(),
            container.clone(),
            ContainerState::Running,
        );
        let mut update = requests::SchedulerUpdateRequest::default();
        update
            .containers
            .insert(ContainerId::new("c1".to_string()), Box::new(container_meta));

        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(requests::SchedulerUpdatePayload::new(
                    update,
                )),
            })
            .await?;

        let event = rx.try_recv().unwrap();
        assert!(
            matches!(&event, ExecutorEvent::ContainerAdded(id) if id.get() == "c1"),
            "expected ContainerAdded, got {event:?}"
        );

        // Terminated container → ContainerRemoved
        let container2 = test_container("c2");
        let container_meta2 = ContainerServerMetadata::new(
            executor_id.clone(),
            container2,
            ContainerState::Terminated {
                reason: crate::data_model::ContainerTerminationReason::Unknown,
            },
        );
        let mut update2 = requests::SchedulerUpdateRequest::default();
        update2.containers.insert(
            ContainerId::new("c2".to_string()),
            Box::new(container_meta2),
        );

        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(requests::SchedulerUpdatePayload::new(
                    update2,
                )),
            })
            .await?;

        let event = rx.try_recv().unwrap();
        assert!(
            matches!(&event, ExecutorEvent::ContainerRemoved(id) if id.get() == "c2"),
            "expected ContainerRemoved, got {event:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_event_emission_full_sync_on_delete_app() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());
        let mut rx = state.register_event_channel(executor_id.clone()).await;

        // First create an application and a container so the executor shows up
        // in the container_scheduler during DeleteApplication.
        let app = mock_application();
        _write_to_test_state_store(&state, app).await?;

        // Container must match the application's namespace/name for the
        // DeleteApplication handler to find it.
        let mut container = test_container("c1");
        container.namespace = TEST_NAMESPACE.to_string();
        container.application_name = "graph_A".to_string();
        let container_meta =
            ContainerServerMetadata::new(executor_id.clone(), container, ContainerState::Running);
        let mut update = requests::SchedulerUpdateRequest::default();
        update
            .containers
            .insert(ContainerId::new("c1".to_string()), Box::new(container_meta));
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(requests::SchedulerUpdatePayload::new(
                    update,
                )),
            })
            .await?;

        // Drain the ContainerAdded event
        let _ = rx.try_recv();

        // Delete the application
        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteApplicationRequest((
                    requests::DeleteApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        name: "graph_A".to_string(),
                    },
                    vec![],
                )),
            })
            .await?;

        // Should receive FullSync
        let event = rx.try_recv().unwrap();
        assert!(
            matches!(&event, ExecutorEvent::FullSync),
            "expected FullSync, got {event:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_event_no_emission_when_no_channel() -> Result<()> {
        // Events are silently dropped when no channel is registered.
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        // No channel registered — should not panic or error.
        let allocation = test_allocation(&executor_id);
        let mut update = requests::SchedulerUpdateRequest::default();
        update.new_allocations.push(allocation);

        state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(requests::SchedulerUpdatePayload::new(
                    update,
                )),
            })
            .await?;

        // Success — no panic, no error.
        Ok(())
    }

    #[tokio::test]
    async fn test_event_channel_register_deregister() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        let _rx = state.register_event_channel(executor_id.clone()).await;
        assert!(
            state
                .executor_event_channels
                .read()
                .await
                .contains_key(&executor_id)
        );

        state.deregister_event_channel(&executor_id).await;
        assert!(
            !state
                .executor_event_channels
                .read()
                .await
                .contains_key(&executor_id)
        );

        Ok(())
    }
}
