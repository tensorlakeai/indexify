use std::{
    collections::{HashMap, HashSet},
    fs,
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
use invocation_events::{InvocationFinishedEvent, InvocationStateChangeEvent};
use metrics::{state_metrics::Metrics as StateMetrics, StateStoreMetrics, Timer};
use opentelemetry::KeyValue;
use requests::{RequestPayload, StateMachineUpdateRequest};
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::{IndexifyObjectsColumns, InvocationCompletion};
use strum::IntoEnumIterator;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, span};

pub mod invocation_events;
pub mod kv;
pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_changes;
pub mod state_machine;
pub mod test_state_store;

#[derive(Debug)]
pub struct ExecutorState {
    pub new_task_channel: broadcast::Sender<()>,
    pub num_registered: u64,
    pub task_ids_sent: HashSet<TaskId>,
}

impl ExecutorState {
    pub fn new() -> Self {
        let (new_task_channel, _) = broadcast::channel(1);
        Self {
            new_task_channel,
            num_registered: 0,
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

    pub fn subscribe(&mut self) -> broadcast::Receiver<()> {
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
    pub last_state_change_id: Arc<AtomicU64>,
    pub task_event_tx: tokio::sync::broadcast::Sender<InvocationStateChangeEvent>,
    pub gc_tx: tokio::sync::watch::Sender<()>,
    pub gc_rx: tokio::sync::watch::Receiver<()>,
    pub system_tasks_tx: tokio::sync::watch::Sender<()>,
    pub system_tasks_rx: tokio::sync::watch::Receiver<()>,
    pub change_events_tx: tokio::sync::watch::Sender<()>,
    pub change_events_rx: tokio::sync::watch::Receiver<()>,
    pub metrics: Arc<StateStoreMetrics>,
    // state_metrics: Arc<StateMetrics>,
}

impl IndexifyState {
    pub async fn new(path: PathBuf) -> Result<Arc<Self>> {
        fs::create_dir_all(path.clone())?;
        let sm_column_families = IndexifyObjectsColumns::iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db: TransactionDB = TransactionDB::open_cf_descriptors(
            &db_opts,
            &TransactionDBOptions::default(),
            path,
            sm_column_families,
        )
        .map_err(|e| anyhow!("failed to open db: {}", e))?;
        let sm_meta = state_machine::read_sm_meta(&db)?;
        let (gc_tx, gc_rx) = tokio::sync::watch::channel(());
        let (task_event_tx, _) = tokio::sync::broadcast::channel(100);
        let (system_tasks_tx, system_tasks_rx) = tokio::sync::watch::channel(());
        let state_store_metrics = Arc::new(StateStoreMetrics::new());
        StateMetrics::new(state_store_metrics.clone());
        // let state_metrics = Arc::new(StateMetrics::new(state_store_metrics.clone()));
        let (change_events_tx, change_events_rx) = tokio::sync::watch::channel(());
        let s = Arc::new(Self {
            db: Arc::new(db),
            last_state_change_id: Arc::new(AtomicU64::new(sm_meta.last_change_idx)),
            executor_states: RwLock::new(HashMap::new()),
            task_event_tx,
            gc_tx,
            gc_rx,
            system_tasks_tx,
            system_tasks_rx,
            metrics: state_store_metrics,
            change_events_tx,
            change_events_rx,
            // state_metrics,
        });

        info!(
            "initialized state store with last state change id: {}",
            s.last_state_change_id.load(atomic::Ordering::Relaxed)
        );
        info!("db version discovered: {}", sm_meta.db_version);

        let executors = s.reader().get_all_executors()?;
        for executor in executors.iter() {
            s.executor_states
                .write()
                .await
                .entry(executor.id.clone())
                .or_default()
                .num_registered += 1;
        }
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
        debug!(
            "writing state machine update request: {}",
            request.payload.to_string(),
        );
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
            RequestPayload::IngestTaskOuputs(task_outputs) => {
                let ingested = state_machine::ingest_task_outputs(
                    self.db.clone(),
                    &txn,
                    task_outputs.clone(),
                )?;
                if ingested {
                    state_changes::task_outputs_ingested(&self.last_state_change_id, task_outputs)?
                } else {
                    vec![]
                }
            }
            RequestPayload::FinalizeTask(finalize_task) => {
                let finalized = state_machine::mark_task_finalized(
                    self.db.clone(),
                    &txn,
                    finalize_task.clone(),
                    self.metrics.clone(),
                )?;
                if finalized {
                    tasks_finalized
                        .entry(finalize_task.executor_id.clone())
                        .or_default()
                        .push(finalize_task.task_id.clone());
                    state_changes::finalized_task(&self.last_state_change_id, &finalize_task)?
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
            RequestPayload::NamespaceProcessorUpdate(request) => {
                let new_state_changes =
                    state_changes::change_events_for_namespace_processor_update(
                        &self.last_state_change_id,
                        &request,
                    )?;
                if let Some(completion) = state_machine::create_tasks(
                    self.db.clone(),
                    &txn,
                    &request.task_requests.clone(),
                    self.metrics.clone().clone(),
                    &request.namespace,
                    &request.compute_graph,
                    &request.invocation_id,
                )? {
                    let _ =
                        self.task_event_tx
                            .send(InvocationStateChangeEvent::InvocationFinished(
                                InvocationFinishedEvent {
                                    id: request.invocation_id.clone(),
                                },
                            ));
                    if completion == InvocationCompletion::System {
                        // Notify the system task handler that it can start new tasks since
                        // a task was completed
                        let _ = self.system_tasks_tx.send(());
                    }
                };
                state_machine::processed_reduction_tasks(
                    self.db.clone(),
                    &txn,
                    &request.reduction_tasks,
                )?;
                new_state_changes
            }
            RequestPayload::TaskAllocationProcessorUpdate(request) => {
                state_machine::handle_task_allocation_update(
                    self.db.clone(),
                    &txn,
                    self.metrics.clone(),
                    request,
                )?;
                for allocation in &request.allocations {
                    allocated_tasks_by_executor.push(allocation.executor.clone());
                }
                vec![]
            }
            RequestPayload::RegisterExecutor(request) => {
                {
                    let mut states = self.executor_states.write().await;
                    let entry = states.entry(request.executor.id.clone()).or_default();
                    entry.num_registered += 1;
                }
                state_machine::register_executor(
                    self.db.clone(),
                    &txn,
                    &request,
                    self.metrics.clone(),
                )?;

                state_changes::register_executor(&self.last_state_change_id, &request)
                    .map_err(|e| anyhow!("error getting state changes {}", e))?
            }
            RequestPayload::MutateClusterTopology(request) => {
                state_machine::deregister_executor(
                    self.db.clone(),
                    &txn,
                    &request.executor_removed,
                    self.metrics.clone(),
                )?;
                state_changes::deregister_executor_events(&self.last_state_change_id, &request)?
            }
            RequestPayload::DeregisterExecutor(request) => {
                let new_state_changes =
                    state_changes::tombstone_executor(&self.last_state_change_id, &request)?;
                let removed = {
                    let mut states = self.executor_states.write().await;
                    if let Some(s) = states.get_mut(&request.executor_id) {
                        s.num_registered -= 1;
                        if s.num_registered == 0 {
                            states.remove(&request.executor_id);
                            true
                        } else {
                            false
                        }
                    } else {
                        true
                    }
                };
                if removed {
                    info!("de-registering executor: {}", request.executor_id);
                    new_state_changes
                } else {
                    vec![]
                }
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
        state_machine::write_sm_meta(
            self.db.clone(),
            &txn,
            &StateMachineMetadata {
                last_change_idx: self.last_state_change_id.load(atomic::Ordering::Relaxed),
                db_version: 1,
            },
        )?;
        txn.commit()?;
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
            RequestPayload::IngestTaskOuputs(task_finished_event) => {
                let ev =
                    InvocationStateChangeEvent::from_task_finished(task_finished_event.clone());
                let _ = self.task_event_tx.send(ev);
            }
            RequestPayload::NamespaceProcessorUpdate(sched_update) => {
                for task in &sched_update.task_requests {
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
            }
            RequestPayload::TaskAllocationProcessorUpdate(sched_update) => {
                for task_allocated in &sched_update.allocations {
                    let _ = self
                        .task_event_tx
                        .send(InvocationStateChangeEvent::TaskAssigned(
                            invocation_events::TaskAssigned {
                                invocation_id: task_allocated.task.invocation_id.clone(),
                                fn_name: task_allocated.task.compute_fn_name.clone(),
                                task_id: task_allocated.task.id.to_string(),
                                executor_id: task_allocated.executor.get().to_string(),
                            },
                        ));
                }
                for diagnostic in &sched_update.placement_diagnostics {
                    let _ = self
                        .task_event_tx
                        .send(InvocationStateChangeEvent::DiagnosticMessage(
                            invocation_events::DiagnosticMessage {
                                invocation_id: diagnostic.task.invocation_id.clone(),
                                message: diagnostic.message.clone(),
                            },
                        ));
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

pub fn task_stream(state: Arc<IndexifyState>, executor: ExecutorId, limit: usize) -> TaskStream {
    let stream = async_stream::stream! {
        let mut rx = state
        .executor_states
        .write()
        .await
        .entry(executor.clone())
        .or_default()
        .subscribe();
        loop {
            // Copy the task_ids_sent before reading the tasks.
            // The update thread modifies tasks first and then updates task_ids_sent,
            // this thread does the opposite. This avoids sending the same task multiple times.
            let task_ids_sent = state.executor_states.read().await.get(&executor).unwrap().task_ids_sent.clone();
            match state
                .reader()
                .get_tasks_by_executor(&executor, limit)
                 {
                    Ok(tasks) => {
                        let state = state.clone();
                        let filtered_tasks = async {
                            let mut executor_state = state.executor_states.write().await;
                            let executor_s = executor_state.get_mut(&executor).unwrap();
                            let mut filtered_tasks = vec![];
                            for task in &tasks {
                                if !task_ids_sent.contains(&task.id) {
                                    filtered_tasks.push(task.clone());
                                    executor_s.added(&vec![task.id.clone()]);
                                }
                            }
                            filtered_tasks
                        }.await;
                        yield Ok(filtered_tasks)
                    },
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            let _ = rx.recv().await;
        }
    };

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use data_model::{
        test_objects::tests::{
            mock_executor,
            mock_graph_a,
            mock_invocation_payload,
            TEST_NAMESPACE,
        },
        ComputeGraph,
        GraphVersion,
        Namespace,
    };
    use requests::{
        CreateOrUpdateComputeGraphRequest,
        InvokeComputeGraphRequest,
        NamespaceRequest,
        RegisterExecutorRequest,
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
        let state_change_1 = state_changes::invoke_compute_graph(
            &indexify_state.last_state_change_id,
            &InvokeComputeGraphRequest {
                namespace: "namespace".to_string(),
                compute_graph_name: "graph_A".to_string(),
                invocation_payload: mock_invocation_payload(),
            },
        )
        .unwrap();
        state_machine::save_state_changes(indexify_state.db.clone(), &tx, &state_change_1).unwrap();
        tx.commit().unwrap();
        let tx = indexify_state.db.transaction();
        let state_change_2 = state_changes::register_executor(
            &indexify_state.last_state_change_id,
            &RegisterExecutorRequest {
                executor: mock_executor(),
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
