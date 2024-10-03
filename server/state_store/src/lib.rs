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
use data_model::{
    ChangeType,
    ExecutorId,
    InvokeComputeGraphEvent,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
    Task,
    TaskFinishedEvent,
    TaskId,
};
use futures::Stream;
use indexify_utils::get_epoch_time_in_ms;
use invocation_events::{InvocationFinishedEvent, InvocationStateChangeEvent};
use requests::StateMachineUpdateRequest;
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::{IndexifyObjectsColumns, InvocationCompletion};
use strum::IntoEnumIterator;
use tokio::sync::{
    broadcast,
    watch::{Receiver, Sender},
    RwLock,
};

pub mod invocation_events;
pub mod requests;
pub mod scanner;
pub mod serializer;
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

pub struct InvocationChangeSubscriber {}

pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
    pub executor_states: RwLock<HashMap<ExecutorId, ExecutorState>>,
    pub state_change_tx: Sender<StateChangeId>,
    pub state_change_rx: Receiver<StateChangeId>,
    pub last_state_change_id: Arc<AtomicU64>,
    pub task_event_tx: tokio::sync::broadcast::Sender<InvocationStateChangeEvent>,
    pub gc_tx: tokio::sync::watch::Sender<()>,
    pub gc_rx: tokio::sync::watch::Receiver<()>,
    pub system_tasks_tx: tokio::sync::watch::Sender<()>,
    pub system_tasks_rx: tokio::sync::watch::Receiver<()>,
}

impl IndexifyState {
    pub async fn new(path: PathBuf) -> Result<Arc<Self>> {
        let (tx, rx) = tokio::sync::watch::channel(StateChangeId::new(std::u64::MAX));
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
        let (gc_tx, gc_rx) = tokio::sync::watch::channel(());
        let (task_event_tx, _) = tokio::sync::broadcast::channel(100);
        let (system_tasks_tx, system_tasks_rx) = tokio::sync::watch::channel(());
        let s = Arc::new(Self {
            db: Arc::new(db),
            state_change_tx: tx,
            state_change_rx: rx,
            last_state_change_id: Arc::new(AtomicU64::new(0)),
            executor_states: RwLock::new(HashMap::new()),
            task_event_tx,
            gc_tx,
            gc_rx,
            system_tasks_tx,
            system_tasks_rx,
        });

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

    pub fn get_state_change_watcher(&self) -> Receiver<StateChangeId> {
        self.state_change_rx.clone()
    }

    pub fn get_gc_watcher(&self) -> Receiver<()> {
        self.gc_rx.clone()
    }

    pub fn get_system_tasks_watcher(&self) -> Receiver<()> {
        self.system_tasks_rx.clone()
    }

    pub async fn write(&self, request: StateMachineUpdateRequest) -> Result<()> {
        let mut allocated_tasks_by_executor = Vec::new();
        let mut tasks_finalized: HashMap<ExecutorId, Vec<TaskId>> = HashMap::new();
        let txn = self.db.transaction();
        let new_state_changes = match &request.payload {
            requests::RequestPayload::InvokeComputeGraph(invoke_compute_graph_request) => {
                let state_changes = self
                    .invoke_compute_graph(&invoke_compute_graph_request)
                    .await?;
                state_machine::create_graph_input(
                    self.db.clone(),
                    &txn,
                    &invoke_compute_graph_request,
                )?;
                state_changes
            }
            requests::RequestPayload::RerunComputeGraph(rerun_compute_graph_request) => {
                tracing::info!(
                    "rerun compute graph: {:?}",
                    rerun_compute_graph_request.compute_graph_name
                );
                state_machine::rerun_compute_graph(
                    self.db.clone(),
                    &txn,
                    rerun_compute_graph_request.clone(),
                )?;
                let _ = self.system_tasks_tx.send(());
                vec![]
            }
            requests::RequestPayload::UpdateSystemTask(update_system_task_request) => {
                state_machine::update_system_task(
                    self.db.clone(),
                    &txn,
                    update_system_task_request.clone(),
                )?;
                vec![]
            }
            requests::RequestPayload::RemoveSystemTask(remove_system_task_request) => {
                state_machine::remove_system_task(
                    self.db.clone(),
                    &txn,
                    remove_system_task_request.clone(),
                )?;
                vec![]
            }
            requests::RequestPayload::RerunInvocation(rerun_invocation_request) => {
                let mut state_changes = state_machine::rerun_invocation(
                    self.db.clone(),
                    &txn,
                    rerun_invocation_request.clone(),
                )?;
                for state_change in &mut state_changes {
                    let last_change_id = self
                        .last_state_change_id
                        .fetch_add(1, atomic::Ordering::Relaxed);
                    state_change.id = StateChangeId::new(last_change_id);
                }
                state_changes
            }
            requests::RequestPayload::FinalizeTask(finalize_task) => {
                let state_changes = if state_machine::mark_task_completed(
                    self.db.clone(),
                    &txn,
                    finalize_task.clone(),
                )? {
                    self.finalize_task(&finalize_task).await?
                } else {
                    Vec::new()
                };
                tasks_finalized
                    .entry(finalize_task.executor_id.clone())
                    .or_default()
                    .push(finalize_task.task_id.clone());
                state_changes
            }
            requests::RequestPayload::CreateNameSpace(namespace_request) => {
                state_machine::create_namespace(self.db.clone(), &namespace_request)?;
                vec![]
            }
            requests::RequestPayload::CreateComputeGraph(req) => {
                state_machine::create_compute_graph(self.db.clone(), req.compute_graph.clone())?;
                vec![]
            }
            requests::RequestPayload::DeleteComputeGraph(request) => {
                state_machine::delete_compute_graph(
                    self.db.clone(),
                    &txn,
                    &request.namespace,
                    &request.name,
                )?;
                self.gc_tx.send(()).unwrap();
                vec![]
            }
            requests::RequestPayload::DeleteInvocation(request) => {
                state_machine::delete_input_data_object(self.db.clone(), &request)?;
                vec![]
            }
            requests::RequestPayload::SchedulerUpdate(request) => {
                let new_state_changes = self.change_events_for_scheduler_update(&request);
                for req in &request.task_requests {
                    match state_machine::create_tasks(self.db.clone(), &txn, req)? {
                        Some(completion) => {
                            if let Err(err) = self.task_event_tx.send(
                                InvocationStateChangeEvent::InvocationFinished(
                                    InvocationFinishedEvent {
                                        id: req.invocation_id.clone(),
                                    },
                                ),
                            ) {
                                tracing::error!(
                                    "failed to send invocation state change: {:?}",
                                    err
                                );
                            }
                            if completion == InvocationCompletion::System {
                                // Notify the system task handler that it can start new tasks since
                                // a task was completed
                                let _ = self.system_tasks_tx.send(());
                            }
                        }
                        None => {}
                    };
                }
                state_machine::processed_reduction_tasks(
                    self.db.clone(),
                    &txn,
                    &request.reduction_tasks,
                )?;
                for allocation in &request.allocations {
                    state_machine::allocate_tasks(
                        self.db.clone(),
                        &txn,
                        &allocation.task,
                        &allocation.executor,
                    )?;
                    allocated_tasks_by_executor.push(allocation.executor.clone());
                }
                new_state_changes
            }
            requests::RequestPayload::RegisterExecutor(request) => {
                {
                    let mut states = self.executor_states.write().await;
                    let entry = states.entry(request.executor.id.clone()).or_default();
                    entry.num_registered += 1;
                }
                state_machine::register_executor(self.db.clone(), &txn, &request)?;
                self.register_executor(&request)
            }
            requests::RequestPayload::DeregisterExecutor(request) => {
                let state_changes = self.deregister_executor_events(&request);
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
                    tracing::info!("de-registering executor: {}", request.executor_id);
                    state_machine::deregister_executor(self.db.clone(), &txn, &request)?;
                }
                state_changes
            }
            requests::RequestPayload::RemoveGcUrls(urls) => {
                state_machine::remove_gc_urls(self.db.clone(), &txn, urls.clone())?;
                vec![]
            }
        };
        if !new_state_changes.is_empty() {
            state_machine::save_state_changes(self.db.clone(), &txn, &new_state_changes)?;
        }
        state_machine::mark_state_changes_processed(
            self.db.clone(),
            &txn,
            &request.state_changes_processed.clone(),
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
        for state_change in new_state_changes {
            self.state_change_tx.send(state_change.id).unwrap();
        }
        Ok(())
    }

    async fn handle_invocation_state_changes(&self, update_request: &StateMachineUpdateRequest) {
        if self.task_event_tx.receiver_count() == 0 {
            return;
        }
        match &update_request.payload {
            requests::RequestPayload::FinalizeTask(task_finished_event) => {
                let ev =
                    InvocationStateChangeEvent::from_task_finished(task_finished_event.clone());
                if let Err(err) = self.task_event_tx.send(ev) {
                    tracing::error!("failed to send invocation state change: {:?}", err);
                }
            }
            requests::RequestPayload::SchedulerUpdate(sched_update) => {
                for task_request in &sched_update.task_requests {
                    for task in task_request.tasks.iter() {
                        if let Err(err) =
                            self.task_event_tx
                                .send(InvocationStateChangeEvent::TaskCreated(
                                    invocation_events::TaskCreated {
                                        invocation_id: task.invocation_id.clone(),
                                        fn_name: task.compute_fn_name.clone(),
                                        task_id: task.id.to_string(),
                                    },
                                ))
                        {
                            tracing::error!("failed to send invocation state change: {:?}", err);
                        }
                    }
                }
                for task_allocated in &sched_update.allocations {
                    if let Err(err) =
                        self.task_event_tx
                            .send(InvocationStateChangeEvent::TaskAssigned(
                                invocation_events::TaskAssigned {
                                    invocation_id: task_allocated.task.invocation_id.clone(),
                                    fn_name: task_allocated.task.compute_fn_name.clone(),
                                    task_id: task_allocated.task.id.to_string(),
                                    executor_id: task_allocated.executor.get().to_string(),
                                },
                            ))
                    {
                        tracing::error!("failed to send invocation state change: {:?}", err);
                    }
                }
            }
            _ => {}
        }
    }

    async fn finalize_task(
        &self,
        request: &requests::FinalizeTaskRequest,
    ) -> Result<Vec<StateChange>> {
        let last_change_id = self
            .last_state_change_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::TaskFinished(TaskFinishedEvent {
                namespace: request.namespace.clone(),
                compute_graph: request.compute_graph.clone(),
                compute_fn: request.compute_fn.clone(),
                invocation_id: request.invocation_id.clone(),
                task_id: request.task_id.clone(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id(request.task_id.clone().to_string())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()?;
        Ok(vec![state_change])
    }

    async fn invoke_compute_graph(
        &self,
        request: &requests::InvokeComputeGraphRequest,
    ) -> Result<Vec<StateChange>> {
        let last_change_id = self
            .last_state_change_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::InvokeComputeGraph(InvokeComputeGraphEvent {
                namespace: request.namespace.clone(),
                invocation_id: request.invocation_payload.id.clone(),
                compute_graph: request.compute_graph_name.clone(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id(request.invocation_payload.id.clone())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()?;
        Ok(vec![state_change])
    }

    fn change_events_for_scheduler_update(
        &self,
        req: &requests::SchedulerUpdateRequest,
    ) -> Vec<StateChange> {
        let mut state_changes = Vec::new();
        for task_request in &req.task_requests {
            let last_change_id = self
                .last_state_change_id
                .fetch_add(1, atomic::Ordering::Relaxed);
            for task in &task_request.tasks {
                let state_change = StateChangeBuilder::default()
                    .change_type(ChangeType::TaskCreated)
                    .created_at(get_epoch_time_in_ms())
                    .object_id(task.id.to_string())
                    .id(StateChangeId::new(last_change_id))
                    .processed_at(None)
                    .build()
                    .unwrap();
                state_changes.push(state_change);
            }
        }
        state_changes
    }

    fn deregister_executor_events(
        &self,
        request: &requests::DeregisterExecutorRequest,
    ) -> Vec<StateChange> {
        let last_change_id = self
            .last_state_change_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::ExecutorRemoved)
            .created_at(get_epoch_time_in_ms())
            .object_id(request.executor_id.get().to_string())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()
            .unwrap();
        vec![state_change]
    }

    fn register_executor(&self, request: &requests::RegisterExecutorRequest) -> Vec<StateChange> {
        let last_change_id = self
            .last_state_change_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::ExecutorAdded)
            .created_at(get_epoch_time_in_ms())
            .object_id(request.executor.id.to_string())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()
            .unwrap();
        vec![state_change]
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
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
    use std::collections::HashMap;

    use data_model::{
        test_objects::tests::{create_mock_task, mock_graph_a, TEST_NAMESPACE},
        ComputeGraph,
        GraphInvocationCtxBuilder,
        Namespace,
    };
    use futures::StreamExt;
    use requests::{
        CreateComputeGraphRequest,
        DeleteComputeGraphRequest,
        ReductionTasks,
        SchedulerUpdateRequest,
        TaskPlacement,
    };
    use tempfile::TempDir;
    use tokio;

    use super::{
        requests::{NamespaceRequest, RequestPayload},
        *,
    };
    use crate::serializer::{JsonEncode, JsonEncoder};

    #[tokio::test]
    async fn test_create_and_list_namespaces() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state")).await?;

        // Create namespaces
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace1".to_string(),
                }),
                state_changes_processed: vec![],
            })
            .await?;
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                    name: "namespace2".to_string(),
                }),
                state_changes_processed: vec![],
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
    async fn test_create_read_and_delete_compute_graph() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state")).await?;

        // Create a compute graph
        let compute_graph = mock_graph_a();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateComputeGraph(CreateComputeGraphRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    compute_graph: compute_graph.clone(),
                }),
                state_changes_processed: vec![],
            })
            .await?;

        // Read the compute graph
        let reader = indexify_state.reader();
        let result = reader
            .get_all_rows_from_cf::<ComputeGraph>(IndexifyObjectsColumns::ComputeGraphs)
            .unwrap();
        let compute_graphs = result
            .iter()
            .map(|(_, cg)| cg.clone())
            .collect::<Vec<ComputeGraph>>();

        // Check if the compute graph was created
        assert!(compute_graphs.iter().any(|cg| cg.name == "graph_A"));

        // Delete the compute graph
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraph(DeleteComputeGraphRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: "graph_A".to_string(),
                }),
                state_changes_processed: vec![],
            })
            .await?;

        // Read the compute graph again
        let result = reader
            .get_all_rows_from_cf::<ComputeGraph>(IndexifyObjectsColumns::ComputeGraphs)
            .unwrap();
        let compute_graphs = result
            .iter()
            .map(|(_, cg)| cg.clone())
            .collect::<Vec<ComputeGraph>>();

        // Check if the compute graph was deleted
        assert!(!compute_graphs.iter().any(|cg| cg.name == "graph_A"));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_stream() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state")).await?;

        let executor_id = ExecutorId::new("executor1".to_string());
        let cg = mock_graph_a();
        let task = create_mock_task(
            &cg,
            "fn",
            "namespace|graph|ingested_id|fn|id_1",
            "ingested_id",
        );
        let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .invocation_id(task.invocation_id.clone())
            .fn_task_analytics(HashMap::new())
            .build(cg.clone())?;
        indexify_state.db.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
            graph_invocation_ctx.key(),
            &JsonEncoder::encode(&graph_invocation_ctx)?,
        )?;

        let create_tasks_request = requests::CreateTasksRequest {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            invocation_id: task.invocation_id.clone(),
            tasks: vec![task.clone()],
        };

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: requests::RequestPayload::SchedulerUpdate(SchedulerUpdateRequest {
                    task_requests: vec![create_tasks_request],
                    allocations: vec![TaskPlacement {
                        task: task.clone(),
                        executor: executor_id.clone(),
                    }],
                    reduction_tasks: ReductionTasks::default(),
                }),
                state_changes_processed: vec![],
            })
            .await?;

        let res = indexify_state
            .reader()
            .get_tasks_by_executor(&executor_id, 10)?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, task.id);

        let mut stream = task_stream(indexify_state.clone(), executor_id.clone(), 10);
        let res = stream.next().await.unwrap()?;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, task.id);

        let task_1 = create_mock_task(
            &cg,
            "fn",
            "namespace|graph|ingested_id|fn|id_2",
            "ingested_id",
        );

        let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .invocation_id(task.invocation_id.clone())
            .fn_task_analytics(HashMap::new())
            .build(cg.clone())?;
        indexify_state.db.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
            graph_invocation_ctx.key(),
            &JsonEncoder::encode(&graph_invocation_ctx)?,
        )?;

        let request = SchedulerUpdateRequest {
            task_requests: vec![requests::CreateTasksRequest {
                tasks: vec![task_1.clone()],
                namespace: task_1.namespace.clone(),
                compute_graph: task_1.compute_graph_name.clone(),
                invocation_id: task_1.invocation_id.clone(),
            }],
            allocations: vec![TaskPlacement {
                task: task_1.clone(),
                executor: executor_id.clone(),
            }],
            reduction_tasks: ReductionTasks::default(),
        };

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: requests::RequestPayload::SchedulerUpdate(request),
                state_changes_processed: vec![],
            })
            .await?;

        let res = indexify_state
            .reader()
            .get_tasks_by_executor(&executor_id, 10)?;
        assert_eq!(res.len(), 2);
        assert_eq!(res[1].id, task_1.id);

        let res = stream.next().await.unwrap()?;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, task_1.id);

        Ok(())
    }
}
