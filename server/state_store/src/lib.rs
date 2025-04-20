use std::{
    collections::{HashMap, HashSet},
    fs,
    ops::Deref,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, StateMachineMetadata, Task, TaskId};
use futures::Stream;
use in_memory_state::{InMemoryMetrics, InMemoryState};
use invocation_events::{InvocationFinishedEvent, InvocationStateChangeEvent};
use metrics::{StateStoreMetrics, Timer};
use opentelemetry::KeyValue;
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{broadcast, watch, RwLock};
use tracing::{debug, error, info, span, warn};

pub mod in_memory_state;
pub mod invocation_events;
pub mod kv;
pub mod migration_runner;
pub mod migrations;
pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_changes;
pub mod state_machine;
pub mod test_state_store;

#[derive(Debug)]
pub struct ExecutorState {
    pub new_task_channel: watch::Sender<()>,
    pub task_ids_sent: HashSet<TaskId>,
}

impl ExecutorState {
    pub fn new() -> Self {
        let (new_task_channel, _) = watch::channel(());
        Self {
            new_task_channel,
            task_ids_sent: HashSet::new(),
        }
    }

    pub fn notify(&mut self) {
        let _ = self.new_task_channel.send(());
    }

    pub fn added(&mut self, task_ids: &Vec<TaskId>) {
        self.task_ids_sent.extend(task_ids.clone());
    }

    // Send notification for remove because new task can be available if
    // were at maximum number of tasks before task completion.
    pub fn removed(&mut self, task_id: TaskId) {
        self.task_ids_sent.remove(&task_id);
        let _ = self.new_task_channel.send(());
    }

    pub fn subscribe(&mut self) -> watch::Receiver<()> {
        self.task_ids_sent.clear();
        self.new_task_channel.subscribe()
    }
}

impl Default for ExecutorState {
    fn default() -> Self {
        Self::new()
    }
}

pub type TaskStream = Pin<Box<dyn Stream<Item = Result<Vec<Task>>> + Send + Sync>>;
pub type StateChangeStream =
    Pin<Box<dyn Stream<Item = Result<InvocationStateChangeEvent>> + Send + Sync>>;

pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
    pub executor_states: RwLock<HashMap<ExecutorId, ExecutorState>>,
    pub db_version: u64,
    pub last_state_change_id: Arc<AtomicU64>,
    pub task_event_tx: tokio::sync::broadcast::Sender<InvocationStateChangeEvent>,
    pub gc_tx: tokio::sync::watch::Sender<()>,
    pub gc_rx: tokio::sync::watch::Receiver<()>,
    pub system_tasks_tx: tokio::sync::watch::Sender<()>,
    pub system_tasks_rx: tokio::sync::watch::Receiver<()>,
    pub change_events_tx: tokio::sync::watch::Sender<()>,
    pub change_events_rx: tokio::sync::watch::Receiver<()>,
    pub metrics: Arc<StateStoreMetrics>,
    pub in_memory_state: Arc<RwLock<in_memory_state::InMemoryState>>,
    // keep handle to in_memory_state metrics to avoid dropping it
    _in_memory_state_metrics: InMemoryMetrics,
}

impl IndexifyState {
    pub async fn new(path: PathBuf) -> Result<Arc<Self>> {
        fs::create_dir_all(path.clone())
            .map_err(|e| anyhow!("failed to create state store dir: {}", e))?;

        // Migrate the db before opening with all column families.
        // This is because the migration process may delete older column families.
        // If we open the db with all column families, it would fail to open.
        let sm_meta = migration_runner::run(&path)?;

        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = Arc::new(
            TransactionDB::open_cf_descriptors(
                &db_opts,
                &TransactionDBOptions::default(),
                path,
                sm_column_families,
            )
            .map_err(|e| anyhow!("failed to open db: {}", e))?,
        );
        let (gc_tx, gc_rx) = tokio::sync::watch::channel(());
        let (task_event_tx, _) = tokio::sync::broadcast::channel(100);
        let (system_tasks_tx, system_tasks_rx) = tokio::sync::watch::channel(());
        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        let (change_events_tx, change_events_rx) = tokio::sync::watch::channel(());
        let indexes = Arc::new(RwLock::new(InMemoryState::new(scanner::StateReader::new(
            db.clone(),
            state_store_metrics.clone(),
        ))?));
        let in_memory_state_metrics = InMemoryMetrics::new(indexes.clone());
        let s = Arc::new(Self {
            db,
            db_version: sm_meta.db_version,
            last_state_change_id: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            executor_states: RwLock::new(HashMap::new()),
            task_event_tx,
            gc_tx,
            gc_rx,
            system_tasks_tx,
            system_tasks_rx,
            metrics: state_store_metrics,
            _in_memory_state_metrics: in_memory_state_metrics,
            change_events_tx,
            change_events_rx,
            in_memory_state: indexes,
        });

        info!(
            "initialized state store with last state change id: {}",
            s.last_state_change_id.load(atomic::Ordering::Relaxed)
        );
        info!("db version discovered: {}", sm_meta.db_version);

        Ok(s)
    }

    pub fn get_gc_watcher(&self) -> tokio::sync::watch::Receiver<()> {
        self.gc_rx.clone()
    }

    pub fn get_system_tasks_watcher(&self) -> tokio::sync::watch::Receiver<()> {
        self.system_tasks_rx.clone()
    }

    #[tracing::instrument(
        skip(self, request),
        fields(
            request_type = request.payload.to_string(),
        )
    )]
    pub async fn write(&self, request: StateMachineUpdateRequest) -> Result<()> {
        let timer_kv = &[KeyValue::new("request", request.payload.to_string())];
        debug!("writing state machine update request",);
        let _timer = Timer::start_with_labels(&self.metrics.state_write, timer_kv);
        let mut allocated_tasks_by_executor = Vec::new();
        let mut tasks_finalized: HashMap<ExecutorId, Vec<TaskId>> = HashMap::new();
        let txn = self.db.transaction();
        let new_state_changes = match &request.payload {
            RequestPayload::InvokeComputeGraph(invoke_compute_graph_request) => {
                let _enter = span!(
                    tracing::Level::INFO,
                    "invoke_compute_graph",
                    namespace = invoke_compute_graph_request.namespace.clone(),
                    invocation_id = invoke_compute_graph_request.invocation_payload.id.clone(),
                    compute_graph = invoke_compute_graph_request.compute_graph_name.clone(),
                );
                let state_changes = state_changes::invoke_compute_graph(
                    &self.last_state_change_id,
                    &invoke_compute_graph_request,
                )?;
                state_machine::create_invocation(
                    self.db.clone(),
                    &txn,
                    &invoke_compute_graph_request,
                )?;
                state_changes
            }
            RequestPayload::SchedulerUpdate(request) => {
                state_machine::handle_scheduler_update(self.db.clone(), &txn, request)?;
                for allocation in &request.new_allocations {
                    allocated_tasks_by_executor.push(allocation.executor_id.clone());
                }
                vec![]
            }
            RequestPayload::IngestTaskOutputs(task_outputs) => {
                let ingested = state_machine::ingest_task_outputs(
                    self.db.clone(),
                    &txn,
                    task_outputs.clone(),
                )?;
                if ingested {
                    tasks_finalized
                        .entry(task_outputs.executor_id.clone())
                        .or_default()
                        .push(task_outputs.task.id.clone());
                    state_changes::task_outputs_ingested(&self.last_state_change_id, task_outputs)?
                } else {
                    vec![]
                }
            }
            RequestPayload::CreateNameSpace(namespace_request) => {
                state_machine::create_namespace(self.db.clone(), &namespace_request)?;
                vec![]
            }
            RequestPayload::CreateOrUpdateComputeGraph(req) => {
                state_machine::create_or_update_compute_graph(
                    self.db.clone(),
                    &txn,
                    req.compute_graph.clone(),
                    req.upgrade_tasks_to_current_version,
                )?;
                vec![]
            }
            RequestPayload::TombstoneComputeGraph(request) => {
                state_changes::tombstone_compute_graph(&self.last_state_change_id, request)?
            }
            RequestPayload::DeleteComputeGraphRequest(request) => {
                state_machine::delete_compute_graph(
                    self.db.clone(),
                    &txn,
                    &request.namespace,
                    &request.name,
                )?;
                self.gc_tx.send(()).unwrap();
                vec![]
            }
            RequestPayload::TombstoneInvocation(request) => {
                state_changes::tombstone_invocation(&self.last_state_change_id, request)?
            }
            RequestPayload::DeleteInvocationRequest(request) => {
                state_machine::delete_invocation(self.db.clone(), &txn, request)?;
                self.gc_tx.send(()).unwrap();
                vec![]
            }
            RequestPayload::UpsertExecutor(request) => {
                self.executor_states
                    .write()
                    .await
                    .entry(request.executor.id.clone())
                    .or_default();

                state_changes::register_executor(&self.last_state_change_id, &request)
                    .map_err(|e| anyhow!("error getting state changes {}", e))?
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
                state_changes::tombstone_executor(&self.last_state_change_id, &request)?
            }
            RequestPayload::RemoveGcUrls(urls) => {
                state_machine::remove_gc_urls(self.db.clone(), &txn, urls.clone())?;
                vec![]
            }
            RequestPayload::Noop => vec![],
        };
        if !new_state_changes.is_empty() {
            state_machine::save_state_changes(self.db.clone(), &txn, &new_state_changes)?;
        }
        state_machine::mark_state_changes_processed(
            self.db.clone(),
            &txn,
            &request.processed_state_changes,
        )?;
        migration_runner::write_sm_meta(
            &self.db,
            &txn,
            &StateMachineMetadata {
                last_change_idx: self.last_state_change_id.load(atomic::Ordering::Relaxed),
                db_version: self.db_version,
            },
        )?;
        txn.commit()?;
        self.in_memory_state
            .write()
            .await
            .update_state(&request)
            .map_err(|e| anyhow!("error updating in memory state: {:?}", e))?;
        for executor_id in allocated_tasks_by_executor {
            self.executor_states
                .write()
                .await
                .get_mut(&executor_id)
                .map(|executor_state| {
                    executor_state.notify();
                });
        }
        for (executor_id, tasks) in tasks_finalized {
            self.executor_states
                .write()
                .await
                .get_mut(&executor_id)
                .map(|executor_state| {
                    for task_id in tasks {
                        executor_state.removed(task_id);
                    }
                });
        }
        self.handle_invocation_state_changes(&request).await;
        if new_state_changes.len() > 0 {
            self.change_events_tx.send(()).unwrap();
        }
        Ok(())
    }

    async fn handle_invocation_state_changes(&self, update_request: &StateMachineUpdateRequest) {
        if self.task_event_tx.receiver_count() == 0 {
            return;
        }
        match &update_request.payload {
            RequestPayload::IngestTaskOutputs(task_finished_event) => {
                let ev =
                    InvocationStateChangeEvent::from_task_finished(task_finished_event.clone());
                let _ = self.task_event_tx.send(ev);
            }
            RequestPayload::SchedulerUpdate(sched_update) => {
                for task in &sched_update.new_allocations {
                    let _ = self
                        .task_event_tx
                        .send(InvocationStateChangeEvent::TaskAssigned(
                            invocation_events::TaskAssigned {
                                invocation_id: task.invocation_id.clone(),
                                fn_name: task.compute_fn.clone(),
                                task_id: task.id.to_string(),
                                executor_id: task.executor_id.get().to_string(),
                            },
                        ));
                }
                for (_, task) in &sched_update.updated_tasks {
                    let _ = self
                        .task_event_tx
                        .send(InvocationStateChangeEvent::TaskCreated(
                            invocation_events::TaskCreated {
                                invocation_id: task.invocation_id.clone(),
                                fn_name: task.compute_fn_name.clone(),
                                task_id: task.id.to_string(),
                            },
                        ));
                }

                for invocation_ctx in &sched_update.updated_invocations_states {
                    if invocation_ctx.completed {
                        let _ = self.task_event_tx.send(
                            InvocationStateChangeEvent::InvocationFinished(
                                InvocationFinishedEvent {
                                    id: invocation_ctx.invocation_id.clone(),
                                },
                            ),
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

    pub fn task_event_stream(&self) -> broadcast::Receiver<InvocationStateChangeEvent> {
        self.task_event_tx.subscribe()
    }
}

pub fn task_stream(state: Arc<IndexifyState>, executor_id: ExecutorId) -> TaskStream {
    let stream = async_stream::stream! {
        let mut rx = if let Some(rx) = state
            .executor_states
            .write()
            .await
            .get_mut(&executor_id)
            .map(|s| s.subscribe()) {
            rx
        } else {
            // Executor not found, closing stream.
            warn!(executor_id=executor_id.get(), "executor not found, stopping task stream");
            return;
        };

        loop {
            // Copy the task_ids_sent before reading the tasks.
            // The update thread modifies tasks first and then updates task_ids_sent,
            // this thread does the opposite. This avoids sending the same task multiple times.
            let task_ids_sent = state.executor_states.read().await.get(&executor_id).map(|s| {
                s.task_ids_sent.clone()
            }).unwrap_or_default();
            let active_tasks = state.in_memory_state.read().await.active_tasks_for_executor(&executor_id);
            if active_tasks.len() > 0 {
                let state = state.clone();
                let mut filtered_tasks = vec![];
                {
                    if let Some(executor) = state.executor_states.write().await.get_mut(&executor_id) {
                        for task in &active_tasks{
                            if !task_ids_sent.contains(&task.id) {
                                filtered_tasks.push(task.deref().clone());
                                executor.added(&vec![task.id.clone()]);
                            }
                        }
                    } else {
                        error!(executor_id=executor_id.get(), "executor removed, stopping task stream");
                        break;
                    }
                }

                yield Ok(filtered_tasks);
            }

            if let Err(_) = rx.changed().await {
                info!(executor_id=executor_id.get(), "executor channel closed, stopping task stream");
                break;
            }
        }
    };

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use data_model::{
        test_objects::tests::{
            mock_dev_executor,
            mock_executor_id,
            mock_graph_a,
            mock_invocation_payload,
            TEST_NAMESPACE,
        },
        ComputeGraph,
        GraphInvocationCtxBuilder,
        GraphVersion,
        Namespace,
        StateChangeId,
    };
    use requests::{
        CreateOrUpdateComputeGraphRequest,
        InvokeComputeGraphRequest,
        NamespaceRequest,
        UpsertExecutorRequest,
    };
    use test_state_store::TestStateStore;
    use tokio;

    use super::*;

    #[tokio::test]
    async fn test_create_and_list_namespaces() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;

        // Create namespaces
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace1".to_string(),
                }),
                processed_state_changes: vec![],
            })
            .await?;
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace2".to_string(),
                }),
                processed_state_changes: vec![],
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

        Ok(())
    }

    #[tokio::test]
    async fn test_version_bump_and_graph_update() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;

        // Create a compute graph and write it
        let compute_graph = mock_graph_a("Old Hash".to_string());
        _write_to_test_state_store(&indexify_state, compute_graph).await?;

        // Read the compute graph
        let compute_graphs = _read_cgs_from_state_store(&indexify_state);

        // Check if the compute graph was created
        assert!(compute_graphs.iter().any(|cg| cg.name == "graph_A"));

        let nodes = &compute_graphs[0].nodes;
        assert_eq!(nodes["fn_a"].image_hash(), "Old Hash");
        assert_eq!(nodes["fn_b"].image_hash(), "Old Hash");
        assert_eq!(nodes["fn_c"].image_hash(), "Old Hash");

        for i in 2..4 {
            // Update the graph
            let new_hash = format!("this is a new hash {}", i);
            let mut compute_graph = mock_graph_a(new_hash.clone());
            compute_graph.version = GraphVersion(i.to_string());

            _write_to_test_state_store(&indexify_state, compute_graph).await?;

            // Read it again
            let compute_graphs = _read_cgs_from_state_store(&indexify_state);

            // Verify the name is the same. Verify the version is different.
            assert!(compute_graphs.iter().any(|cg| cg.name == "graph_A"));
            // println!("compute graph {:?}", compute_graphs[0]);
            assert_eq!(compute_graphs[0].version, GraphVersion(i.to_string()));
            let nodes = &compute_graphs[0].nodes;
            assert_eq!(nodes["fn_a"].image_hash(), new_hash.clone());
            assert_eq!(nodes["fn_b"].image_hash(), new_hash.clone());
            assert_eq!(nodes["fn_c"].image_hash(), new_hash.clone());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_order_state_changes() -> Result<()> {
        let indexify_state = TestStateStore::new().await?.indexify_state;
        let tx = indexify_state.db.transaction();
        let ctx = GraphInvocationCtxBuilder::default()
            .namespace("namespace1".to_string())
            .compute_graph_name("cg1".to_string())
            .invocation_id("foo1".to_string())
            .graph_version(GraphVersion("1".to_string()))
            .build(tests::mock_graph_a("image_hash".to_string()))?;
        let state_change_1 = state_changes::invoke_compute_graph(
            &indexify_state.last_state_change_id,
            &InvokeComputeGraphRequest {
                namespace: "namespace".to_string(),
                compute_graph_name: "graph_A".to_string(),
                invocation_payload: mock_invocation_payload(),
                ctx: ctx.clone(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(indexify_state.db.clone(), &tx, &state_change_1).unwrap();
        tx.commit().unwrap();
        let tx = indexify_state.db.transaction();
        let state_change_2 = state_changes::register_executor(
            &indexify_state.last_state_change_id,
            &UpsertExecutorRequest {
                executor: mock_dev_executor(mock_executor_id()),
            },
        )
        .unwrap();
        state_machine::save_state_changes(indexify_state.db.clone(), &tx, &state_change_2).unwrap();
        tx.commit().unwrap();
        let tx = indexify_state.db.transaction();
        let state_change_3 = state_changes::invoke_compute_graph(
            &indexify_state.last_state_change_id,
            &InvokeComputeGraphRequest {
                namespace: "namespace".to_string(),
                compute_graph_name: "graph_A".to_string(),
                invocation_payload: mock_invocation_payload(),
                ctx: ctx.clone(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(indexify_state.db.clone(), &tx, &state_change_3).unwrap();
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

    fn _read_cgs_from_state_store(indexify_state: &IndexifyState) -> Vec<ComputeGraph> {
        let reader = indexify_state.reader();
        let result = reader
            .get_all_rows_from_cf::<ComputeGraph>(IndexifyObjectsColumns::ComputeGraphs)
            .unwrap();
        let compute_graphs = result
            .iter()
            .map(|(_, cg)| cg.clone())
            .collect::<Vec<ComputeGraph>>();

        compute_graphs
    }

    async fn _write_to_test_state_store(
        indexify_state: &Arc<IndexifyState>,
        compute_graph: ComputeGraph,
    ) -> Result<()> {
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(
                    CreateOrUpdateComputeGraphRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        compute_graph: compute_graph.clone(),
                        upgrade_tasks_to_current_version: false,
                    },
                ),
                processed_state_changes: vec![],
            })
            .await
    }
}
