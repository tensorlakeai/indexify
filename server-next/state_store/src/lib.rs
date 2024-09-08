use std::{
    cmp::Ordering,
    collections::HashMap,
    fs,
    path::PathBuf,
    pin::Pin,
    sync::{atomic, atomic::AtomicU64, Arc, RwLock},
    time::SystemTime,
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    ExecutorId,
    InvokeComputeGraphEvent,
    Namespace,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
    Task,
    TaskFinishedEvent,
    TaskId,
};
use futures::Stream;
use indexify_utils::get_epoch_time_in_ms;
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::{
    broadcast,
    watch::{Receiver, Sender},
};

pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_machine;
pub mod test_state_store;

#[derive(Debug, Clone)]
pub struct UnfinishedTask {
    pub id: TaskId,
    pub creation_time: SystemTime,
}

impl PartialEq for UnfinishedTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for UnfinishedTask {}

impl PartialOrd for UnfinishedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Sort unfinished tasks by creation time so we can process them in order
impl Ord for UnfinishedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.creation_time
            .cmp(&other.creation_time)
            .then_with(|| self.id.cmp(&other.id))
    }
}

#[derive(Debug)]
pub struct ExecutorUnfinishedTasks {
    pub new_task_channel: broadcast::Sender<()>,
}

impl ExecutorUnfinishedTasks {
    pub fn new() -> Self {
        let (new_task_channel, _) = broadcast::channel(1);
        Self { new_task_channel }
    }

    pub fn added(&mut self) {
        let _ = self.new_task_channel.send(());
    }

    // Send notification for remove because new task can be available if
    // were at maximum number of tasks before task completion.
    pub fn removed(&mut self) {
        let _ = self.new_task_channel.send(());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.new_task_channel.subscribe()
    }
}

impl Default for ExecutorUnfinishedTasks {
    fn default() -> Self {
        Self::new()
    }
}

pub type TaskStream = Pin<Box<dyn Stream<Item = Result<Vec<Task>>> + Send + Sync>>;

pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
    pub unfinished_tasks_by_executor: RwLock<HashMap<ExecutorId, ExecutorUnfinishedTasks>>,
    pub state_change_tx: Sender<StateChangeId>,
    pub state_change_rx: Receiver<StateChangeId>,
    pub last_state_change_id: Arc<AtomicU64>,
}

impl IndexifyState {
    pub fn new(path: PathBuf) -> Result<Self> {
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
        Ok(Self {
            db: Arc::new(db),
            state_change_tx: tx,
            state_change_rx: rx,
            last_state_change_id: Arc::new(AtomicU64::new(0)),
            unfinished_tasks_by_executor: RwLock::new(HashMap::new()),
        })
    }

    pub fn get_state_change_watcher(&self) -> Receiver<StateChangeId> {
        self.state_change_rx.clone()
    }

    pub async fn write(&self, request: requests::RequestType) -> Result<()> {
        let state_changes = match request {
            requests::RequestType::InvokeComputeGraph(invoke_compute_graph_request) => {
                self.invoke_compute_graph(&invoke_compute_graph_request)
                    .await?
            }
            requests::RequestType::FinalizeTask(finalize_task) => {
                self.finalize_task(&finalize_task).await?
            }
            requests::RequestType::CreateNameSpace(namespace_request) => {
                self.create_namespace(&namespace_request.name).await?
            }
            requests::RequestType::CreateComputeGraph(create_compute_graph_request) => {
                self.create_compute_graph(&create_compute_graph_request)
                    .await?
            }
            requests::RequestType::DeleteComputeGraph(delete_compute_graph_request) => {
                self.delete_compute_graph(&delete_compute_graph_request)
                    .await?
            }
            requests::RequestType::CreateTasks(create_tasks_request) => {
                self.create_tasks(&create_tasks_request).await?
            }
            requests::RequestType::DeleteInvocation(delete_invocation_request) => {
                self.delete_invocation(&delete_invocation_request).await?
            }
        };
        for state_change in state_changes {
            self.state_change_tx.send(state_change.id).unwrap();
        }
        Ok(())
    }

    async fn finalize_task(
        &self,
        request: &requests::FinalizeTaskRequest,
    ) -> Result<Vec<StateChange>> {
        let txn = self.db.transaction();
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

        state_machine::mark_task_completed(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.compute_graph,
            &request.compute_fn,
            &request.invocation_id,
            &request.task_id.to_string(),
            &request.task_outcome,
            request.node_output.clone(),
            &request.executor_id.clone(),
        )?;
        state_machine::save_state_changes(self.db.clone(), &txn, vec![state_change.clone()])?;
        txn.commit()?;
        Ok(vec![state_change])
    }

    async fn invoke_compute_graph(
        &self,
        request: &requests::InvokeComputeGraphRequest,
    ) -> Result<Vec<StateChange>> {
        let txn = self.db.transaction();
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
        state_machine::create_graph_input(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.compute_graph_name,
            request.invocation_payload.clone(),
        )?;
        state_machine::save_state_changes(self.db.clone(), &txn, vec![state_change.clone()])?;
        txn.commit()?;
        Ok(vec![state_change])
    }

    async fn delete_invocation(
        &self,
        request: &requests::DeleteInvocationRequest,
    ) -> Result<Vec<StateChange>> {
        state_machine::delete_input_data_object(
            self.db.clone(),
            &request.namespace,
            &request.compute_graph,
            &request.invocation_id,
        )?;
        Ok(vec![])
    }

    async fn create_namespace(&self, name: &str) -> Result<Vec<StateChange>> {
        let namespace = Namespace {
            name: name.to_string(),
            created_at: get_epoch_time_in_ms(),
        };
        state_machine::create_namespace(self.db.clone(), &namespace)?;
        Ok(vec![])
    }

    async fn create_compute_graph(
        &self,
        create_compute_graph_request: &requests::CreateComputeGraphRequest,
    ) -> Result<Vec<StateChange>> {
        let compute_graph = create_compute_graph_request.compute_graph.clone();
        state_machine::create_compute_graph(self.db.clone(), compute_graph)?;
        Ok(vec![])
    }

    async fn create_tasks(
        &self,
        create_tasks_request: &requests::CreateTaskRequest,
    ) -> Result<Vec<StateChange>> {
        let txn = self.db.transaction();
        state_machine::create_tasks(self.db.clone(), &txn, create_tasks_request.tasks.clone())?;
        txn.commit()?;
        Ok(vec![])
    }

    async fn delete_compute_graph(
        &self,
        request: &requests::DeleteComputeGraphRequest,
    ) -> Result<Vec<StateChange>> {
        let txn = self.db.transaction();
        state_machine::delete_compute_graph(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.name,
        )?;
        txn.commit()?;
        Ok(vec![])
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
    }

    pub fn update_task_assignment(
        &self,
        task: &Task,
        executor_id: &ExecutorId,
        should_add: bool,
    ) -> Result<()> {
        let txn = self.db.transaction();
        state_machine::update_task_assignment(
            self.db.as_ref(),
            &txn,
            task,
            executor_id,
            should_add,
        )?;
        txn.commit()?;

        self.unfinished_tasks_by_executor
            .write()
            .unwrap()
            .get_mut(executor_id)
            .map(|tasks| {
                if should_add {
                    tasks.added();
                } else {
                    tasks.removed();
                }
            });

        Ok(())
    }
}

pub fn task_stream(state: Arc<IndexifyState>, executor: ExecutorId, limit: usize) -> TaskStream {
    let stream = async_stream::stream! {
        let mut rx = state
        .unfinished_tasks_by_executor
        .write()
        .unwrap()
        .entry(executor.clone())
        .or_default()
        .subscribe();
        loop {
            match state
                .reader()
                .get_tasks_by_executor(&executor, limit)
                 {
                    Ok(tasks) => yield Ok(tasks),
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
        test_objects::tests::{mock_graph_a, TEST_NAMESPACE},
        ComputeGraph,
        GraphInvocationCtxBuilder,
        Namespace,
        TaskBuilder,
    };
    use futures::StreamExt;
    use requests::{CreateComputeGraphRequest, DeleteComputeGraphRequest};
    use tempfile::TempDir;
    use tokio;

    use super::{
        requests::{NamespaceRequest, RequestType},
        *,
    };
    use crate::serializer::{JsonEncode, JsonEncoder};

    #[tokio::test]
    async fn test_create_and_list_namespaces() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state"))?;

        // Create namespaces
        indexify_state
            .write(RequestType::CreateNameSpace(NamespaceRequest {
                name: "namespace1".to_string(),
            }))
            .await?;
        indexify_state
            .write(RequestType::CreateNameSpace(NamespaceRequest {
                name: "namespace2".to_string(),
            }))
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
        let indexify_state = IndexifyState::new(temp_dir.path().join("state"))?;

        // Create a compute graph
        let compute_graph = mock_graph_a();
        indexify_state
            .write(RequestType::CreateComputeGraph(CreateComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: compute_graph.clone(),
            }))
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
            .write(RequestType::DeleteComputeGraph(DeleteComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph_A".to_string(),
            }))
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
        let indexify_state = Arc::new(IndexifyState::new(temp_dir.path().join("state"))?);

        let executor_id = ExecutorId::new("executor1".to_string());
        let task = TaskBuilder::default()
            .namespace("namespace".to_string())
            .compute_fn_name("fn".to_string())
            .compute_graph_name("graph".to_string())
            .input_data_id("id_1".to_string())
            .invocation_id("ingested_id".to_string())
            .build()?;

        let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .invocation_id(task.invocation_id.clone())
            .fn_task_analytics(HashMap::new())
            .build()?;
        indexify_state.db.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
            graph_invocation_ctx.key(),
            &JsonEncoder::encode(&graph_invocation_ctx)?,
        )?;

        indexify_state
            .write(requests::RequestType::CreateTasks(
                requests::CreateTaskRequest {
                    tasks: vec![task.clone()],
                    processed_state_changes: vec![],
                },
            ))
            .await?;

        indexify_state.update_task_assignment(&task, &executor_id, true)?;

        let res = indexify_state
            .reader()
            .get_tasks_by_executor(&executor_id, 10)?;
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, task.id);

        let mut stream = task_stream(indexify_state.clone(), executor_id.clone(), 10);
        let res = stream.next().await.unwrap()?;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].id, task.id);

        let task_1 = TaskBuilder::default()
            .namespace("namespace".to_string())
            .compute_fn_name("fn".to_string())
            .compute_graph_name("graph".to_string())
            .input_data_id("id_2".to_string())
            .invocation_id("ingested_id".to_string())
            .build()?;

        let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .invocation_id(task.invocation_id.clone())
            .fn_task_analytics(HashMap::new())
            .build()?;
        indexify_state.db.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
            graph_invocation_ctx.key(),
            &JsonEncoder::encode(&graph_invocation_ctx)?,
        )?;

        indexify_state
            .write(requests::RequestType::CreateTasks(
                requests::CreateTaskRequest {
                    tasks: vec![task_1.clone()],
                    processed_state_changes: vec![],
                },
            ))
            .await?;

        indexify_state.update_task_assignment(&task_1, &executor_id, true)?;

        let res = stream.next().await.unwrap()?;

        assert_eq!(res.len(), 2);
        assert_eq!(res[0].id, task.id);
        assert_eq!(res[1].id, task_1.id);

        Ok(())
    }
}
