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
use arc_swap::ArcSwap;
use in_memory_state::InMemoryState;
use opentelemetry::KeyValue;
use request_events::{PersistedRequestStateChangeEvent, RequestStateChangeEvent};
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{Notify, RwLock, watch};
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{ExecutorId, SandboxKey, SandboxStatus, SnapshotStatus, StateMachineMetadata},
    executor_api::executor_api_pb,
    metrics::{StateStoreMetrics, Timer},
    processor::container_scheduler::ContainerScheduler,
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

/// Server-side connection state for a single executor.
/// Created on registration, destroyed on deregistration.
///
/// Holds the command emitter and buffered commands/results for long-poll
/// delivery. Command generation is driven by the global executor wake
/// channel — the background task diffs ArcSwap state on each wake.
pub struct ExecutorConnection {
    /// Command emitter — persists across reconnections.
    /// Fresh emitter (has_synced=false) on first registration.
    pub emitter: Arc<tokio::sync::Mutex<CommandEmitter>>,

    /// Buffered commands for poll_commands delivery.
    pending_commands: Arc<tokio::sync::Mutex<Vec<executor_api_pb::Command>>>,
    /// Wakes a held poll_commands request when new commands arrive.
    commands_notify: Arc<Notify>,

    /// Buffered results for poll_allocation_results delivery.
    pending_results: Arc<tokio::sync::Mutex<Vec<executor_api_pb::SequencedAllocationResult>>>,
    /// Monotonic counter for result sequence numbers.
    next_result_seq: Arc<AtomicU64>,
    /// Highest command sequence number acked by the dataplane.
    /// Used to detect executor restarts (ack regression to 0/None).
    last_acked_command_seq: Arc<AtomicU64>,
    /// One-shot flag consumed by heartbeat to request a full-state sync.
    request_full_state: Arc<atomic::AtomicBool>,
    /// Wakes a held poll_allocation_results request when new results arrive.
    results_notify: Arc<Notify>,

    /// Background task that diffs state and produces commands.
    /// Spawned externally (in executor_api.rs) because it needs
    /// executor_manager and blob_storage_registry.
    pub command_generator_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ExecutorConnection {
    /// Create a new connection (executor just registered).
    pub fn new() -> Self {
        Self {
            emitter: Arc::new(tokio::sync::Mutex::new(CommandEmitter::new())),
            pending_commands: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            commands_notify: Arc::new(Notify::new()),
            pending_results: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            next_result_seq: Arc::new(AtomicU64::new(1)),
            last_acked_command_seq: Arc::new(AtomicU64::new(0)),
            request_full_state: Arc::new(atomic::AtomicBool::new(false)),
            results_notify: Arc::new(Notify::new()),
            command_generator_handle: None,
        }
    }

    /// Append commands to the pending buffer and wake any waiting poll.
    pub async fn push_commands(&self, cmds: Vec<executor_api_pb::Command>) {
        if cmds.is_empty() {
            return;
        }
        let mut buf = self.pending_commands.lock().await;
        buf.extend(cmds);
        self.commands_notify.notify_one();
    }

    /// Clone all pending commands (does NOT remove them).
    pub async fn clone_commands(&self) -> Vec<executor_api_pb::Command> {
        self.pending_commands.lock().await.clone()
    }

    /// Remove commands with seq <= `acked_seq`.
    pub async fn drain_commands_up_to(&self, acked_seq: u64) {
        let mut buf = self.pending_commands.lock().await;
        buf.retain(|cmd| cmd.seq > acked_seq);
    }

    /// Observe command ack progression and detect regressions.
    ///
    /// Returns true when a regression is detected (e.g. ack resets from N>0 to
    /// 0/None), which usually indicates dataplane local-state loss/restart.
    pub fn observe_command_ack(&self, acked_seq: Option<u64>) -> bool {
        let observed = acked_seq.unwrap_or(0);
        let prev = self.last_acked_command_seq.load(atomic::Ordering::Relaxed);
        if observed < prev {
            self.last_acked_command_seq
                .store(0, atomic::Ordering::Relaxed);
            self.request_full_state
                .store(true, atomic::Ordering::SeqCst);
            return true;
        }
        if observed > prev {
            self.last_acked_command_seq
                .store(observed, atomic::Ordering::Relaxed);
        }
        false
    }

    /// Consume and clear the one-shot full-state request flag.
    pub fn take_full_state_request(&self) -> bool {
        self.request_full_state
            .swap(false, atomic::Ordering::SeqCst)
    }

    /// Buffer a new allocation log entry as a sequenced result and wake any
    /// waiting poll.
    pub async fn push_result(&self, entry: executor_api_pb::AllocationLogEntry) {
        let seq = self.next_result_seq.fetch_add(1, atomic::Ordering::Relaxed);
        let result = executor_api_pb::SequencedAllocationResult {
            seq,
            entry: Some(entry),
        };
        let mut buf = self.pending_results.lock().await;
        buf.push(result);
        self.results_notify.notify_one();
    }

    /// Clone all pending results (does NOT remove them).
    pub async fn clone_results(&self) -> Vec<executor_api_pb::SequencedAllocationResult> {
        self.pending_results.lock().await.clone()
    }

    /// Remove results with seq <= `acked_seq`.
    pub async fn drain_results_up_to(&self, acked_seq: u64) {
        let mut buf = self.pending_results.lock().await;
        buf.retain(|r| r.seq > acked_seq);
    }

    /// Get a clone of the commands notify handle (for long-poll waiters).
    pub fn commands_notify(&self) -> Arc<Notify> {
        self.commands_notify.clone()
    }

    /// Get a clone of the results notify handle (for long-poll waiters).
    pub fn results_notify(&self) -> Arc<Notify> {
        self.results_notify.clone()
    }

    /// Reset the emitter to fresh state (has_synced=false, empty tracking
    /// sets). Called on re-registration so the next command generation cycle
    /// does a full sync with accurate state instead of using stale tracking
    /// data.
    pub async fn reset_emitter(&self) {
        let mut emitter = self.emitter.lock().await;
        *emitter = CommandEmitter::new();
        self.last_acked_command_seq
            .store(0, atomic::Ordering::Relaxed);
        self.request_full_state
            .store(false, atomic::Ordering::SeqCst);
    }
}

/// Use forward declaration to avoid circular dependency with executor_api
/// module. The actual CommandEmitter is defined in executor_api.rs.
pub use crate::executor_api::CommandEmitter;

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

/// Combined snapshot of in-memory indexes and container scheduler.
/// One `ArcSwap<AppState>` replaces the two independent ArcSwaps so readers
/// always get a consistent pair (no race window between loads).
#[derive(Clone)]
pub struct AppState {
    pub indexes: in_memory_state::InMemoryState,
    pub scheduler: ContainerScheduler,
}

pub struct IndexifyState {
    pub db: Arc<RocksDBDriver>,
    pub db_version: u64,

    pub state_change_id_seq: Arc<AtomicU64>,
    pub usage_event_id_seq: Arc<AtomicU64>,
    pub request_event_id_seq: Arc<AtomicU64>,
    /// Monotonic counter for payload queue sequence IDs.
    pub payload_seq: AtomicU64,

    pub change_events_tx: watch::Sender<()>,
    pub change_events_rx: watch::Receiver<()>,
    pub usage_events_tx: watch::Sender<()>,
    pub usage_events_rx: watch::Receiver<()>,

    /// Global wake channel for diff-based command generation.
    /// The scheduler fires this after publishing to ArcSwap; per-executor
    /// command generator tasks subscribe and diff against the new state.
    pub executor_wake_tx: watch::Sender<()>,

    pub metrics: Arc<StateStoreMetrics>,
    pub app_state: Arc<ArcSwap<AppState>>,
    /// Serializes writers to app_state.
    /// ArcSwap provides lock-free reads but concurrent fork-mutate-publish
    /// cycles need serialization to prevent lost updates.
    pub write_mutex: tokio::sync::Mutex<()>,
    /// Per-executor connection state for diff-based command generation.
    /// A background task diffs against ArcSwap on wake and buffers
    /// commands/results for long-poll delivery.
    pub executor_connections: RwLock<HashMap<ExecutorId, ExecutorConnection>>,
    pub request_event_buffers: RequestEventBuffers,
    // Observable gauges - must be kept alive for callbacks to fire
    _in_memory_store_gauges: InMemoryStoreGauges,
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
    should_notify_usage_reporter: bool,
    /// Whether a payload was enqueued to the PayloadQueue for the scheduler.
    payload_enqueued: bool,
    /// Request state change events to broadcast to SSE and file dump workers.
    request_state_changes: Vec<RequestStateChangeEvent>,
}

impl IndexifyState {
    pub async fn new(
        path: PathBuf,
        config: RocksDBConfig,
        executor_catalog: ExecutorCatalog,
        request_event_buffers: RequestEventBuffers,
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

        let (change_events_tx, change_events_rx) = watch::channel(());
        let (usage_events_tx, usage_events_rx) = watch::channel(());
        let (executor_wake_tx, _executor_wake_rx) = watch::channel(());

        let state_reader = scanner::StateReader::new(db.clone(), state_store_metrics.clone());

        let in_memory_state = InMemoryState::new(
            sm_meta.last_change_idx,
            state_reader.clone(),
            executor_catalog,
        )
        .await?;

        let mut container_scheduler = ContainerScheduler::new(&state_reader).await?;

        // Load persisted executor metadata into the container scheduler so
        // it can schedule work immediately on restart without waiting for
        // executors to re-register.
        let persisted_executors = state_reader.list_executors().await?;
        for executor in &persisted_executors {
            container_scheduler.upsert_executor(executor);
        }
        if !persisted_executors.is_empty() {
            info!(
                num_executors = persisted_executors.len(),
                "loaded persisted executors into container scheduler"
            );
        }

        // Determine the starting payload_seq from any existing payloads in
        // the queue (crash recovery: pick up after the last enqueued seq).
        let pending_payloads = state_reader.read_pending_payloads().await?;
        let initial_payload_seq = pending_payloads.last().map(|(seq, _)| seq + 1).unwrap_or(0);
        if !pending_payloads.is_empty() {
            info!(
                num_pending = pending_payloads.len(),
                initial_payload_seq, "found pending payloads in queue at startup"
            );
        }

        let app_state = Arc::new(ArcSwap::from_pointee(AppState {
            indexes: in_memory_state,
            scheduler: container_scheduler,
        }));
        let in_memory_store_gauges = InMemoryStoreGauges::new(app_state.clone());

        let s = Arc::new(Self {
            db,
            db_version: sm_meta.db_version,
            state_change_id_seq: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            usage_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_usage_idx)),
            request_event_id_seq: Arc::new(AtomicU64::new(sm_meta.last_request_event_idx)),
            payload_seq: AtomicU64::new(initial_payload_seq),
            metrics: state_store_metrics,
            change_events_tx,
            change_events_rx,
            app_state,
            usage_events_tx,
            usage_events_rx,
            executor_wake_tx,
            executor_connections: RwLock::new(HashMap::new()),
            request_event_buffers,
            write_mutex: tokio::sync::Mutex::new(()),
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
    pub async fn write(&self, mut request: StateMachineUpdateRequest) -> Result<()> {
        debug!("writing state machine update request: {:#?}", request);
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _timer = Timer::start_with_labels(&self.metrics.state_write, timer_kv);

        // Serialize writers — prevents concurrent fork-mutate-publish races.
        let _write_guard = self.write_mutex.lock().await;

        let write_result = self
            .write_in_persistent_store(&mut request, timer_kv)
            .await?;

        {
            let _timer = Timer::start_with_labels(&self.metrics.state_change_notify, timer_kv);
            // Wake the scheduler when a payload is enqueued.
            if write_result.payload_enqueued &&
                let Err(err) = self.change_events_tx.send(())
            {
                error!(error = ?err, "failed to notify of state change event, ignoring");
            }

            if write_result.should_notify_usage_reporter &&
                let Err(err) = self.usage_events_tx.send(())
            {
                error!(error = ?err, "failed to notify of usage event, ignoring");
            }

            self.request_event_buffers
                .push_events(write_result.request_state_changes)
                .await;
        }

        Ok(())
    }

    /// Subscribe to the global executor wake channel. Each subscriber gets
    /// its own independent receiver that tracks which notifications it has
    /// seen.
    pub fn subscribe_executor_wake(&self) -> watch::Receiver<()> {
        self.executor_wake_tx.subscribe()
    }

    async fn write_in_persistent_store(
        &self,
        request: &mut StateMachineUpdateRequest,
        timer_kv: &[KeyValue],
    ) -> Result<PersistentWriteResult> {
        self.write_in_persistent_store_inner(request, timer_kv, None)
            .await
    }

    /// Core persistent write. When `dequeue_through_seq` is Some, processed
    /// payloads up through that sequence are deleted in the same RocksDB
    /// transaction — making the scheduler output + dequeue atomic.
    async fn write_in_persistent_store_inner(
        &self,
        request: &mut StateMachineUpdateRequest,
        timer_kv: &[KeyValue],
        dequeue_through_seq: Option<u64>,
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
            RequestPayload::DeleteApplicationRequest(request) => {
                state_machine::delete_application(
                    &txn,
                    &request.namespace,
                    &request.name,
                    current_clock,
                )
                .await?;
            }
            RequestPayload::DeleteRequestRequest(request) => {
                state_machine::delete_request(&txn, request, current_clock).await?;
            }
            RequestPayload::UpsertExecutor(request) => {
                // Persist executor metadata to RocksDB so it survives restarts.
                state_machine::upsert_executor(&txn, &request.executor).await?;
            }
            RequestPayload::DeregisterExecutor(request) => {
                info!(
                    executor_id = request.executor_id.get(),
                    "marking executor as tombstoned"
                );
            }
            RequestPayload::CreateSandbox(request) => {
                state_machine::upsert_sandbox(&txn, &request.sandbox, current_clock).await?;
            }
            RequestPayload::SnapshotSandbox(request) => {
                // 1) Persist the new Snapshot object with upload_uri
                let mut snapshot = request.snapshot.clone();
                snapshot.upload_uri = Some(request.upload_uri.clone());
                state_machine::upsert_snapshot(&txn, &snapshot).await?;

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
                let state = self.app_state.load();
                let snapshot_opt = state
                    .indexes
                    .snapshots
                    .values()
                    .find(|s| s.id == request.snapshot_id)
                    .map(|s| (**s).clone());
                drop(state);

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

                    // Terminate the sandbox. Try in-memory first, fall back to DB
                    // since ArcSwap may not have the sandbox yet (enqueued payloads
                    // are applied by the scheduler, not by write()).
                    let sb_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
                    let state = self.app_state.load();
                    let sandbox_opt = state.indexes.sandboxes.get(&sb_key).cloned();
                    drop(state);
                    let sandbox_opt = match sandbox_opt {
                        Some(s) => Some(*s),
                        None => {
                            let reader =
                                scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                            reader
                                .get_sandbox(&snapshot.namespace, snapshot.sandbox_id.get())
                                .await?
                        }
                    };
                    if let Some(mut sandbox) = sandbox_opt {
                        sandbox.status = SandboxStatus::Terminated;
                        state_machine::upsert_sandbox(&txn, &sandbox, current_clock).await?;
                    }
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
                let state = self.app_state.load();
                let snapshot_opt = state
                    .indexes
                    .snapshots
                    .values()
                    .find(|s| s.id == request.snapshot_id)
                    .map(|s| (**s).clone());
                drop(state);

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

                    // Mark sandbox as Terminated on snapshot failure.
                    // Try in-memory first, fall back to DB since ArcSwap may not
                    // have the sandbox yet.
                    let sb_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
                    let state = self.app_state.load();
                    let sandbox_opt = state.indexes.sandboxes.get(&sb_key).cloned();
                    drop(state);
                    let sandbox_opt = match sandbox_opt {
                        Some(s) => Some(*s),
                        None => {
                            let reader =
                                scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                            reader
                                .get_sandbox(&snapshot.namespace, snapshot.sandbox_id.get())
                                .await?
                        }
                    };
                    if let Some(mut sandbox) = sandbox_opt {
                        sandbox.status = SandboxStatus::Terminated;
                        state_machine::upsert_sandbox(&txn, &sandbox, current_clock).await?;
                    }
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
            RequestPayload::TombstoneContainerPool(request) => {
                // Mark pool as tombstoned in RocksDB so user-facing reads
                // (which query RocksDB) treat it as deleted immediately.
                // The scheduler later issues the actual RocksDB delete.
                let reader = scanner::StateReader::new(self.db.clone(), self.metrics.clone());
                if let Some(mut pool) = reader
                    .get_sandbox_pool(&request.namespace, request.pool_id.get())
                    .await?
                {
                    pool.tombstoned = true;
                    state_machine::upsert_container_pool(&txn, &pool, current_clock).await?;
                }
            }
            RequestPayload::DeleteContainerPool(request) => {
                state_machine::delete_container_pool(
                    &txn,
                    &request.namespace,
                    request.pool_id.get(),
                )
                .await?;
            }
            _ => {} // Handle other request types as needed
        };

        // Persist request state change events into the same transaction so they
        // become durable atomically with the state change that produced them.
        // Only written when an HTTP exporter is configured.
        if self.request_event_buffers.export_request_events() {
            for event in &request_state_changes {
                let event_id = self
                    .request_event_id_seq
                    .fetch_add(1, atomic::Ordering::Relaxed);
                let persisted =
                    PersistedRequestStateChangeEvent::new(event_id.into(), event.clone());
                state_machine::persist_single_request_state_change_event(&txn, &persisted).await?;
            }
        }

        // Enqueue the payload into the PayloadQueue so the scheduler can
        // process it. Everything is enqueued EXCEPT scheduler-output payloads
        // (which the scheduler writes via write_scheduler_output()).
        let should_enqueue = !matches!(
            &request.payload,
            RequestPayload::SchedulerUpdate(_) |
                RequestPayload::DeleteApplicationRequest(_) |
                RequestPayload::DeleteRequestRequest(_) |
                RequestPayload::DeleteContainerPool(_)
        );
        if should_enqueue {
            let payload_seq = self.payload_seq.fetch_add(1, atomic::Ordering::Relaxed);
            state_machine::enqueue_payload(&txn, payload_seq, &request.payload).await?;
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

        // When dequeue_through_seq is provided, delete processed payloads in
        // the same transaction so the scheduler output + dequeue are atomic.
        if let Some(through_seq) = dequeue_through_seq {
            state_machine::dequeue_payloads(&txn, through_seq).await?;
        }

        txn.commit().await?;

        Ok(PersistentWriteResult {
            payload_enqueued: should_enqueue,
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

    /// Ensure an executor connection exists with a fresh emitter. Called
    /// during registration (handle_v2_full_state) so events can be buffered
    /// even before the command generator task starts.
    ///
    /// If a connection already exists (re-registration without prior
    /// deregister), the emitter is reset to `has_synced = false` so the
    /// next command generation cycle does a full sync with accurate
    /// tracking state. Pending buffers are preserved so buffered
    /// commands are not lost.
    pub async fn register_executor_connection(&self, executor_id: &ExecutorId) {
        let mut connections = self.executor_connections.write().await;
        match connections.entry(executor_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                info!(
                    executor_id = executor_id.get(),
                    "created executor connection"
                );
                entry.insert(ExecutorConnection::new());
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                info!(
                    executor_id = executor_id.get(),
                    "re-registration: resetting emitter on existing executor connection"
                );
                entry.get().reset_emitter().await;
            }
        }
    }

    /// Remove the connection for an executor. Called on deregistration.
    /// Aborts the background command generator task and wakes any held
    /// long-poll requests.
    pub async fn deregister_executor_connection(&self, executor_id: &ExecutorId) {
        let removed = self.executor_connections.write().await.remove(executor_id);
        if let Some(conn) = removed {
            // Abort the background command generator task so it doesn't
            // linger if it's blocked on emitter lock or I/O.
            if let Some(handle) = conn.command_generator_handle {
                handle.abort();
            }
            // Wake any held long-poll requests so they return immediately
            // instead of blocking until the 5-minute timeout.
            conn.commands_notify.notify_one();
            conn.results_notify.notify_one();
            info!(
                executor_id = executor_id.get(),
                "deregistered executor connection"
            );
        }
    }

    /// Persist a scheduler-generated write (allocations, updated requests,
    /// etc.) without enqueuing a payload or updating the ArcSwap. The
    /// scheduler handles ArcSwap publishing itself after processing.
    pub async fn write_scheduler_output(
        &self,
        mut request: StateMachineUpdateRequest,
    ) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _write_guard = self.write_mutex.lock().await;

        let write_result = self
            .write_in_persistent_store(&mut request, timer_kv)
            .await?;

        if write_result.should_notify_usage_reporter {
            let _ = self.usage_events_tx.send(());
        }
        self.request_event_buffers
            .push_events(write_result.request_state_changes)
            .await;
        Ok(())
    }

    /// Persist scheduler output AND dequeue processed payloads in a single
    /// RocksDB transaction. This makes the two operations atomic — on crash
    /// recovery either both are committed or neither is, avoiding redundant
    /// payload replay.
    pub async fn write_scheduler_output_and_dequeue(
        &self,
        mut request: StateMachineUpdateRequest,
        dequeue_through_seq: u64,
    ) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _write_guard = self.write_mutex.lock().await;

        let write_result = self
            .write_in_persistent_store_inner(&mut request, timer_kv, Some(dequeue_through_seq))
            .await?;

        if write_result.should_notify_usage_reporter {
            let _ = self.usage_events_tx.send(());
        }
        self.request_event_buffers
            .push_events(write_result.request_state_changes)
            .await;
        Ok(())
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
    use requests::{CreateOrUpdateApplicationRequest, NamespaceRequest};
    use test_state_store::TestStateStore;

    use super::*;
    use crate::data_model::{
        Application,
        Namespace,
        test_objects::tests::{TEST_EXECUTOR_ID, TEST_NAMESPACE, mock_application},
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
            "Executors",
            "PayloadQueue",
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

    // --- Executor wake channel tests ---

    /// Helper: get the emitter from a registered executor connection.
    async fn get_emitter(
        state: &IndexifyState,
        executor_id: &ExecutorId,
    ) -> Arc<tokio::sync::Mutex<CommandEmitter>> {
        state
            .executor_connections
            .read()
            .await
            .get(executor_id)
            .expect("executor connection should exist")
            .emitter
            .clone()
    }

    #[tokio::test]
    async fn test_executor_wake_channel() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;

        // Subscribe before sending
        let mut rx = state.subscribe_executor_wake();

        // Fire wake — should be received
        let _ = state.executor_wake_tx.send(());
        rx.changed().await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_executor_connection_register_deregister() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        state.register_executor_connection(&executor_id).await;
        assert!(
            state
                .executor_connections
                .read()
                .await
                .contains_key(&executor_id)
        );

        state.deregister_executor_connection(&executor_id).await;
        assert!(
            !state
                .executor_connections
                .read()
                .await
                .contains_key(&executor_id)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_emitter_starts_unsynced() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        state.register_executor_connection(&executor_id).await;

        let emitter = get_emitter(&state, &executor_id).await;
        {
            let emitter = emitter.lock().await;
            assert!(
                !emitter.has_synced,
                "fresh emitter should have has_synced=false"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_re_registration_resets_emitter() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        state.register_executor_connection(&executor_id).await;
        let emitter = get_emitter(&state, &executor_id).await;

        // Simulate a synced emitter with tracking state
        {
            let mut emitter = emitter.lock().await;
            emitter.has_synced = true;
            emitter.known_allocations.insert("alloc-1".to_string());
            emitter
                .known_containers
                .insert("container-1".to_string(), Default::default());
        }

        // Re-register (simulates server asking for full state again)
        state.register_executor_connection(&executor_id).await;

        // Emitter should be reset: has_synced=false, empty tracking
        let emitter2 = get_emitter(&state, &executor_id).await;
        {
            let emitter = emitter2.lock().await;
            assert!(
                !emitter.has_synced,
                "emitter should be reset to has_synced=false after re-registration"
            );
            assert!(
                emitter.known_allocations.is_empty(),
                "emitter allocations should be cleared after re-registration"
            );
            assert!(
                emitter.known_containers.is_empty(),
                "emitter containers should be cleared after re-registration"
            );
        }

        Ok(())
    }
}
