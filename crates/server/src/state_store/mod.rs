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
use prost::Message;
use request_events::{PersistedRequestStateChangeEvent, RequestStateChangeEvent};
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options};
use serde::{Deserialize, Serialize};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{RwLock, watch};
use tracing::{debug, error, info, span};

use crate::{
    config::ExecutorCatalogEntry,
    data_model::{ExecutorId, SandboxKey, SandboxStatus, SnapshotStatus, StateMachineMetadata},
    executor_api::executor_api_pb,
    metrics::{StateStoreMetrics, Timer},
    processor::container_scheduler::ContainerScheduler,
    state_store::{
        driver::{
            Reader,
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

pub mod executor_connection;
pub use executor_connection::ExecutorConnection;

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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedFunctionCallRoute {
    pub parent_allocation_id: String,
    pub original_function_call_id: String,
    pub executor_id: ExecutorId,
}

#[derive(Debug, Clone)]
pub struct SchedulerCommandIntent {
    pub executor_id: ExecutorId,
    pub command: executor_api_pb::Command,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedSchedulerCommandIntent {
    pub executor_id: ExecutorId,
    pub command: Vec<u8>,
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

    pub metrics: Arc<StateStoreMetrics>,
    pub app_state: Arc<ArcSwap<AppState>>,
    /// Serializes writers to app_state.
    /// ArcSwap provides lock-free reads but concurrent fork-mutate-publish
    /// cycles need serialization to prevent lost updates.
    pub write_mutex: tokio::sync::Mutex<()>,
    /// Per-executor long-poll connection state and command/result outboxes.
    pub executor_connections: RwLock<HashMap<ExecutorId, ExecutorConnection>>,
    /// Monotonic sequence for scheduler command intent records.
    pub scheduler_command_intent_seq: AtomicU64,
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
    const EXECUTOR_CMD_ACK_PREFIX: &'static str = "ack|";
    const EXECUTOR_CMD_NEXT_PREFIX: &'static str = "next|";
    const EXECUTOR_CMD_PREFIX: &'static str = "cmd|";
    const FUNCTION_CALL_ROUTE_PREFIX: &'static str = "route|";
    const SCHEDULER_COMMAND_INTENT_PREFIX: &'static str = "intent|";

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
        let (initial_scheduler_command_intent_seq, pending_scheduler_command_intents) =
            Self::read_scheduler_command_intent_startup_state(&db).await?;
        if pending_scheduler_command_intents > 0 {
            info!(
                pending_scheduler_command_intents,
                initial_scheduler_command_intent_seq,
                "found pending scheduler command intents at startup"
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
            scheduler_command_intent_seq: AtomicU64::new(initial_scheduler_command_intent_seq),
            metrics: state_store_metrics,
            change_events_tx,
            change_events_rx,
            app_state,
            usage_events_tx,
            usage_events_rx,
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
            .write_in_persistent_store(&mut request, timer_kv, &[])
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

    async fn write_in_persistent_store(
        &self,
        request: &mut StateMachineUpdateRequest,
        timer_kv: &[KeyValue],
        scheduler_command_intents: &[SchedulerCommandIntent],
    ) -> Result<PersistentWriteResult> {
        self.write_in_persistent_store_inner(request, timer_kv, None, scheduler_command_intents)
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
        scheduler_command_intents: &[SchedulerCommandIntent],
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
        // (which the scheduler writes via write_scheduler_output_with_intents()).
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

        if !scheduler_command_intents.is_empty() {
            self.persist_scheduler_command_intents(&txn, scheduler_command_intents)
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

    fn executor_cmd_prefix_key(executor_id: &ExecutorId) -> Vec<u8> {
        format!("{}{}|", Self::EXECUTOR_CMD_PREFIX, executor_id.get()).into_bytes()
    }

    fn function_call_route_prefix_key() -> Vec<u8> {
        Self::FUNCTION_CALL_ROUTE_PREFIX.as_bytes().to_vec()
    }

    fn scheduler_command_intent_prefix_key() -> Vec<u8> {
        Self::SCHEDULER_COMMAND_INTENT_PREFIX.as_bytes().to_vec()
    }

    fn scheduler_command_intent_key(seq: u64) -> Vec<u8> {
        format!("{}{:020}", Self::SCHEDULER_COMMAND_INTENT_PREFIX, seq).into_bytes()
    }

    fn parse_scheduler_command_intent_seq(key: &[u8]) -> Option<u64> {
        let key = std::str::from_utf8(key).ok()?;
        let seq = key.strip_prefix(Self::SCHEDULER_COMMAND_INTENT_PREFIX)?;
        seq.parse::<u64>().ok()
    }

    async fn read_scheduler_command_intent_startup_state(
        db: &RocksDBDriver,
    ) -> Result<(u64, usize)> {
        let prefix = Self::scheduler_command_intent_prefix_key();
        let iter = db
            .iter(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                driver::IterOptions::default().starting_at(prefix.clone()),
            )
            .await;

        let mut max_seq: Option<u64> = None;
        let mut count = 0usize;
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let Some(seq) = Self::parse_scheduler_command_intent_seq(&key) else {
                continue;
            };
            max_seq = Some(max_seq.map_or(seq, |curr| curr.max(seq)));
            count = count.saturating_add(1);
        }

        Ok((max_seq.map_or(0, |s| s.saturating_add(1)), count))
    }

    fn function_call_route_key(function_call_id: &str) -> Vec<u8> {
        format!("{}{}", Self::FUNCTION_CALL_ROUTE_PREFIX, function_call_id).into_bytes()
    }

    fn executor_cmd_key(executor_id: &ExecutorId, seq: u64) -> Vec<u8> {
        format!(
            "{}{}|{:020}",
            Self::EXECUTOR_CMD_PREFIX,
            executor_id.get(),
            seq
        )
        .into_bytes()
    }

    fn executor_cmd_ack_key(executor_id: &ExecutorId) -> Vec<u8> {
        format!("{}{}", Self::EXECUTOR_CMD_ACK_PREFIX, executor_id.get()).into_bytes()
    }

    fn executor_cmd_next_key(executor_id: &ExecutorId) -> Vec<u8> {
        format!("{}{}", Self::EXECUTOR_CMD_NEXT_PREFIX, executor_id.get()).into_bytes()
    }

    fn parse_executor_cmd_seq(key: &[u8]) -> Option<u64> {
        let key = std::str::from_utf8(key).ok()?;
        let (_, seq) = key.rsplit_once('|')?;
        seq.parse::<u64>().ok()
    }

    async fn persist_scheduler_command_intents(
        &self,
        txn: &Transaction,
        intents: &[SchedulerCommandIntent],
    ) -> Result<()> {
        for intent in intents {
            let seq = self
                .scheduler_command_intent_seq
                .fetch_add(1, atomic::Ordering::Relaxed);
            let key = Self::scheduler_command_intent_key(seq);
            let persisted = PersistedSchedulerCommandIntent {
                executor_id: intent.executor_id.clone(),
                command: intent.command.encode_to_vec(),
            };
            let encoded = StateStoreEncoder::encode(&persisted)?;
            txn.put(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                key.as_slice(),
                encoded.as_slice(),
            )
            .await?;
        }
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn take_scheduler_command_intents(
        &self,
        max_items: usize,
    ) -> Result<Vec<SchedulerCommandIntent>> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        let prefix = Self::scheduler_command_intent_prefix_key();
        let txn = self.db.transaction();
        let iter = txn
            .iter(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                prefix.clone(),
            )
            .await;
        let mut intents = Vec::new();
        for item in iter {
            if intents.len() >= max_items {
                break;
            }
            let (key, value) = item?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let persisted = StateStoreEncoder::decode::<PersistedSchedulerCommandIntent>(&value)?;
            let command = executor_api_pb::Command::decode(persisted.command.as_slice())
                .map_err(|e| anyhow!("failed to decode persisted scheduler command intent: {e}"))?;
            intents.push(SchedulerCommandIntent {
                executor_id: persisted.executor_id,
                command,
            });
            txn.delete(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                key.as_ref(),
            )
            .await?;
        }
        txn.commit().await?;
        Ok(intents)
    }

    /// Atomically move scheduler command intents into per-executor command
    /// outboxes and return the enqueued commands grouped by executor.
    pub async fn move_scheduler_command_intents_to_outbox(
        &self,
        max_items: usize,
    ) -> Result<HashMap<ExecutorId, Vec<executor_api_pb::Command>>> {
        if max_items == 0 {
            return Ok(HashMap::new());
        }

        let prefix = Self::scheduler_command_intent_prefix_key();
        let txn = self.db.transaction();
        let iter = txn
            .iter(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                prefix.clone(),
            )
            .await;

        let mut next_seq_by_executor: HashMap<ExecutorId, u64> = HashMap::new();
        let mut enqueued: HashMap<ExecutorId, Vec<executor_api_pb::Command>> = HashMap::new();
        let mut moved = 0usize;
        for item in iter {
            if moved >= max_items {
                break;
            }
            let (key, value) = item?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }

            let persisted = StateStoreEncoder::decode::<PersistedSchedulerCommandIntent>(&value)?;
            let mut command = executor_api_pb::Command::decode(persisted.command.as_slice())
                .map_err(|e| anyhow!("failed to decode persisted scheduler command intent: {e}"))?;
            let executor_id = persisted.executor_id;

            let next_seq = if let Some(next_seq) = next_seq_by_executor.get_mut(&executor_id) {
                next_seq
            } else {
                let next_key = Self::executor_cmd_next_key(&executor_id);
                let next_seq = match txn
                    .get(
                        IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                        next_key.as_slice(),
                    )
                    .await?
                {
                    Some(bytes) => StateStoreEncoder::decode::<u64>(&bytes)?,
                    None => 1,
                };
                next_seq_by_executor.insert(executor_id.clone(), next_seq);
                next_seq_by_executor
                    .get_mut(&executor_id)
                    .expect("executor next seq should be inserted")
            };

            command.seq = *next_seq;
            let cmd_key = Self::executor_cmd_key(&executor_id, command.seq);
            txn.put(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                cmd_key.as_slice(),
                &command.encode_to_vec(),
            )
            .await?;
            *next_seq = (*next_seq).saturating_add(1);
            enqueued.entry(executor_id).or_default().push(command);

            txn.delete(
                IndexifyObjectsColumns::SchedulerCommandIntents.as_ref(),
                key.as_ref(),
            )
            .await?;
            moved = moved.saturating_add(1);
        }

        for (executor_id, next_seq) in &next_seq_by_executor {
            let next_key = Self::executor_cmd_next_key(executor_id);
            let next_bytes = StateStoreEncoder::encode(next_seq)?;
            txn.put(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                next_key.as_slice(),
                next_bytes.as_slice(),
            )
            .await?;
        }

        txn.commit().await?;
        Ok(enqueued)
    }

    async fn read_executor_cmd_ack(&self, executor_id: &ExecutorId) -> Result<u64> {
        let key = Self::executor_cmd_ack_key(executor_id);
        let Some(bytes) = self
            .db
            .get(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                key.as_slice(),
            )
            .await?
        else {
            return Ok(0);
        };
        StateStoreEncoder::decode::<u64>(&bytes)
    }

    async fn read_executor_cmd_next_seq(&self, executor_id: &ExecutorId) -> Result<u64> {
        let key = Self::executor_cmd_next_key(executor_id);
        let Some(bytes) = self
            .db
            .get(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                key.as_slice(),
            )
            .await?
        else {
            return Ok(1);
        };
        StateStoreEncoder::decode::<u64>(&bytes)
    }

    async fn load_executor_pending_commands(
        &self,
        executor_id: &ExecutorId,
    ) -> Result<(u64, Vec<executor_api_pb::Command>)> {
        let acked = self.read_executor_cmd_ack(executor_id).await?;
        let prefix = Self::executor_cmd_prefix_key(executor_id);
        let mut commands = Vec::new();

        let iter = self
            .db
            .iter(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                driver::IterOptions::default().starting_at(prefix.clone()),
            )
            .await;
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let Some(seq) = Self::parse_executor_cmd_seq(&key) else {
                continue;
            };
            if seq <= acked {
                continue;
            }
            let cmd = executor_api_pb::Command::decode(value.as_ref())
                .map_err(|e| anyhow!("failed to decode persisted command: {e}"))?;
            commands.push(cmd);
        }

        Ok((acked, commands))
    }

    /// Persist commands in the executor outbox, assign durable sequence
    /// numbers, and append to the in-memory long-poll buffer.
    pub async fn enqueue_executor_commands(
        &self,
        executor_id: &ExecutorId,
        mut commands: Vec<executor_api_pb::Command>,
    ) -> Result<()> {
        if commands.is_empty() {
            return Ok(());
        }

        let txn = self.db.transaction();
        let mut next_seq = self.read_executor_cmd_next_seq(executor_id).await?;
        for cmd in &mut commands {
            cmd.seq = next_seq;
            let key = Self::executor_cmd_key(executor_id, next_seq);
            txn.put(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                key.as_slice(),
                &cmd.encode_to_vec(),
            )
            .await?;
            next_seq = next_seq.saturating_add(1);
        }

        let next_key = Self::executor_cmd_next_key(executor_id);
        let next_bytes = StateStoreEncoder::encode(&next_seq)?;
        txn.put(
            IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
            next_key.as_slice(),
            next_bytes.as_slice(),
        )
        .await?;
        txn.commit().await?;

        let connections = self.executor_connections.read().await;
        if let Some(conn) = connections.get(executor_id) {
            conn.push_commands(commands).await;
        }
        Ok(())
    }

    /// Persist command ack progression and prune acked outbox entries.
    pub async fn ack_executor_commands(
        &self,
        executor_id: &ExecutorId,
        acked_seq: u64,
    ) -> Result<()> {
        let prev_acked = self.read_executor_cmd_ack(executor_id).await?;
        let effective_acked = acked_seq.max(prev_acked);

        let txn = self.db.transaction();
        let ack_key = Self::executor_cmd_ack_key(executor_id);
        let ack_bytes = StateStoreEncoder::encode(&effective_acked)?;
        txn.put(
            IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
            ack_key.as_slice(),
            ack_bytes.as_slice(),
        )
        .await?;

        let prefix = Self::executor_cmd_prefix_key(executor_id);
        let iter = txn
            .iter(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                prefix.clone(),
            )
            .await;
        for kv in iter {
            let (key, _) = kv?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            if let Some(seq) = Self::parse_executor_cmd_seq(&key) &&
                seq <= effective_acked
            {
                txn.delete(
                    IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                    key.as_ref(),
                )
                .await?;
            }
        }

        txn.commit().await?;
        Ok(())
    }

    /// Drop all persisted command outbox state for this executor and reset
    /// cursor records.
    pub async fn reset_executor_command_outbox(&self, executor_id: &ExecutorId) -> Result<()> {
        let txn = self.db.transaction();
        let prefix = Self::executor_cmd_prefix_key(executor_id);
        let iter = txn
            .iter(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                prefix.clone(),
            )
            .await;
        for kv in iter {
            let (key, _) = kv?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            txn.delete(
                IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
                key.as_ref(),
            )
            .await?;
        }

        let ack_key = Self::executor_cmd_ack_key(executor_id);
        let next_key = Self::executor_cmd_next_key(executor_id);
        let ack_bytes = StateStoreEncoder::encode(&0u64)?;
        let next_bytes = StateStoreEncoder::encode(&1u64)?;
        txn.put(
            IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
            ack_key.as_slice(),
            ack_bytes.as_slice(),
        )
        .await?;
        txn.put(
            IndexifyObjectsColumns::ExecutorCommandOutbox.as_ref(),
            next_key.as_slice(),
            next_bytes.as_slice(),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn upsert_function_call_route(
        &self,
        function_call_id: &str,
        parent_allocation_id: &str,
        original_function_call_id: &str,
        executor_id: &ExecutorId,
    ) -> Result<()> {
        let key = Self::function_call_route_key(function_call_id);
        let route = PersistedFunctionCallRoute {
            parent_allocation_id: parent_allocation_id.to_string(),
            original_function_call_id: original_function_call_id.to_string(),
            executor_id: executor_id.clone(),
        };
        let encoded = StateStoreEncoder::encode(&route)?;
        self.db
            .put(
                IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
                key.as_slice(),
                encoded.as_slice(),
            )
            .await?;
        Ok(())
    }

    pub async fn get_function_call_route(
        &self,
        function_call_id: &str,
    ) -> Result<Option<PersistedFunctionCallRoute>> {
        let key = Self::function_call_route_key(function_call_id);
        let value = self
            .db
            .get(
                IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
                key.as_slice(),
            )
            .await?;
        match value {
            Some(value) => Ok(Some(
                StateStoreEncoder::decode::<PersistedFunctionCallRoute>(&value)?,
            )),
            None => Ok(None),
        }
    }

    pub async fn take_function_call_route(
        &self,
        function_call_id: &str,
    ) -> Result<Option<PersistedFunctionCallRoute>> {
        let key = Self::function_call_route_key(function_call_id);
        let txn = self.db.transaction();
        let value = txn
            .get(
                IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
                key.as_slice(),
            )
            .await?;
        let Some(value) = value else {
            return Ok(None);
        };
        let route = StateStoreEncoder::decode::<PersistedFunctionCallRoute>(&value)?;
        txn.delete(
            IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
            key.as_slice(),
        )
        .await?;
        txn.commit().await?;
        Ok(Some(route))
    }

    pub async fn delete_function_call_route(&self, function_call_id: &str) -> Result<()> {
        let key = Self::function_call_route_key(function_call_id);
        let txn = self.db.transaction();
        txn.delete(
            IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
            key.as_slice(),
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn purge_function_call_routes_for_executor(
        &self,
        executor_id: &ExecutorId,
    ) -> Result<usize> {
        let prefix = Self::function_call_route_prefix_key();
        let txn = self.db.transaction();
        let iter = txn
            .iter(
                IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
                prefix.clone(),
            )
            .await;
        let mut purged = 0usize;
        for kv in iter {
            let (key, value) = kv?;
            if !key.starts_with(prefix.as_slice()) {
                break;
            }
            let route = StateStoreEncoder::decode::<PersistedFunctionCallRoute>(&value)?;
            if route.executor_id == *executor_id {
                txn.delete(
                    IndexifyObjectsColumns::FunctionCallResultRoutes.as_ref(),
                    key.as_ref(),
                )
                .await?;
                purged += 1;
            }
        }
        txn.commit().await?;
        Ok(purged)
    }

    /// Ensure an executor connection exists with a fresh command buffer.
    /// Called during registration (handle_v2_full_state) so commands/results
    /// can be buffered for long-poll delivery.
    ///
    /// If a connection already exists (re-registration without prior
    /// deregister), command emission state and persisted outbox are reset so
    /// the executor gets a clean full-sync command stream.
    pub async fn register_executor_connection(&self, executor_id: &ExecutorId) {
        let (acked, pending_commands) = match self.load_executor_pending_commands(executor_id).await
        {
            Ok(v) => v,
            Err(err) => {
                error!(
                    executor_id = executor_id.get(),
                    error = ?err,
                    "failed loading persisted command outbox; continuing with empty buffer"
                );
                (0, Vec::new())
            }
        };

        let conn = ExecutorConnection::new();
        conn.restore_acked_command_seq(acked);
        conn.replace_commands(pending_commands).await;

        let mut existing_conn: Option<ExecutorConnection> = None;
        let mut connections = self.executor_connections.write().await;
        match connections.entry(executor_id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                info!(
                    executor_id = executor_id.get(),
                    "created executor connection"
                );
                entry.insert(conn);
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                info!(
                    executor_id = executor_id.get(),
                    "re-registration: resetting command outbox and command buffers"
                );
                existing_conn = Some(entry.get().clone());
            }
        }
        drop(connections);

        if let Some(existing_conn) = existing_conn {
            if let Err(err) = self.reset_executor_command_outbox(executor_id).await {
                error!(
                    executor_id = executor_id.get(),
                    error = ?err,
                    "failed to reset persisted command outbox during re-registration"
                );
            }
            existing_conn.reset_for_full_sync().await;
        }
    }

    /// Remove the connection for an executor. Called on deregistration.
    /// Wakes any held long-poll requests.
    pub async fn deregister_executor_connection(&self, executor_id: &ExecutorId) {
        let removed = self.executor_connections.write().await.remove(executor_id);
        if let Some(conn) = removed {
            if let Err(err) = self.reset_executor_command_outbox(executor_id).await {
                error!(
                    executor_id = executor_id.get(),
                    error = ?err,
                    "failed to reset persisted command outbox during deregistration"
                );
            }
            // Wake any held long-poll requests so they return immediately
            // instead of blocking until the 5-minute timeout.
            conn.commands_notify().notify_one();
            conn.results_notify().notify_one();
            info!(
                executor_id = executor_id.get(),
                "deregistered executor connection"
            );
        }
    }

    /// Persist a scheduler-generated write (allocations, updated requests,
    /// etc.) together with scheduler command intents, without enqueuing a
    /// payload or updating the ArcSwap. The scheduler handles ArcSwap
    /// publishing itself after processing.
    pub async fn write_scheduler_output_with_intents(
        &self,
        mut request: StateMachineUpdateRequest,
        scheduler_command_intents: &[SchedulerCommandIntent],
    ) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _write_guard = self.write_mutex.lock().await;

        let write_result = self
            .write_in_persistent_store(&mut request, timer_kv, scheduler_command_intents)
            .await?;

        if write_result.should_notify_usage_reporter {
            let _ = self.usage_events_tx.send(());
        }
        self.request_event_buffers
            .push_events(write_result.request_state_changes)
            .await;
        Ok(())
    }

    /// Persist scheduler output + scheduler command intents AND dequeue
    /// processed payloads in a single RocksDB transaction. This makes the
    /// operations atomic — on crash recovery either all are committed or
    /// none are, avoiding redundant payload replay and lost command batches.
    pub async fn write_scheduler_output_and_dequeue_with_intents(
        &self,
        mut request: StateMachineUpdateRequest,
        dequeue_through_seq: u64,
        scheduler_command_intents: &[SchedulerCommandIntent],
    ) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        let _write_guard = self.write_mutex.lock().await;

        let write_result = self
            .write_in_persistent_store_inner(
                &mut request,
                timer_kv,
                Some(dequeue_through_seq),
                scheduler_command_intents,
            )
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
            "ExecutorCommandOutbox",
            "FunctionCallResultRoutes",
            "SchedulerCommandIntents",
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
    async fn test_re_registration_resets_command_outbox_and_cursors() -> Result<()> {
        let state = TestStateStore::new().await?.indexify_state;
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        state.register_executor_connection(&executor_id).await;
        state
            .enqueue_executor_commands(
                &executor_id,
                vec![
                    executor_api_pb::Command {
                        seq: 0,
                        command: None,
                    },
                    executor_api_pb::Command {
                        seq: 0,
                        command: None,
                    },
                ],
            )
            .await?;
        state.ack_executor_commands(&executor_id, 1).await?;

        // Re-register (simulates server asking for full state again)
        state.register_executor_connection(&executor_id).await;

        // Pending commands must be cleared and durable cursors reset.
        {
            let connections = state.executor_connections.read().await;
            let conn = connections
                .get(&executor_id)
                .expect("executor connection should exist");
            assert!(
                conn.clone_commands().await.is_empty(),
                "pending commands should be cleared after re-registration"
            );
        }
        assert_eq!(
            state.read_executor_cmd_ack(&executor_id).await?,
            0,
            "acked command cursor should reset on re-registration"
        );
        assert_eq!(
            state.read_executor_cmd_next_seq(&executor_id).await?,
            1,
            "next command sequence should reset on re-registration"
        );
        let (acked, pending) = state.load_executor_pending_commands(&executor_id).await?;
        assert_eq!(acked, 0, "restored ack cursor should be reset");
        assert!(
            pending.is_empty(),
            "no persisted pending commands should remain after reset"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_executor_command_outbox_persists_across_restart() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("state");
        let executor_id = ExecutorId::new(TEST_EXECUTOR_ID.to_string());

        {
            let state = IndexifyState::new(
                db_path.clone(),
                RocksDBConfig::default(),
                ExecutorCatalog::default(),
                RequestEventBuffers::default(),
            )
            .await?;

            state.register_executor_connection(&executor_id).await;
            state
                .enqueue_executor_commands(
                    &executor_id,
                    vec![
                        executor_api_pb::Command {
                            seq: 0,
                            command: None,
                        },
                        executor_api_pb::Command {
                            seq: 0,
                            command: None,
                        },
                        executor_api_pb::Command {
                            seq: 0,
                            command: None,
                        },
                    ],
                )
                .await?;
            state.ack_executor_commands(&executor_id, 2).await?;
        }

        {
            let state = IndexifyState::new(
                db_path,
                RocksDBConfig::default(),
                ExecutorCatalog::default(),
                RequestEventBuffers::default(),
            )
            .await?;
            state.register_executor_connection(&executor_id).await;
            let connections = state.executor_connections.read().await;
            let conn = connections
                .get(&executor_id)
                .expect("executor connection should exist");
            let commands = conn.clone_commands().await;
            assert_eq!(commands.len(), 1, "only unacked commands should restore");
            assert_eq!(commands[0].seq, 3, "restored command seq should be stable");
        }

        Ok(())
    }
}
