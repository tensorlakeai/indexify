use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    InvokeComputeGraphEvent,
    Namespace,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
};
use indexify_utils::get_epoch_time_in_ms;
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;
use tokio::sync::watch::{Receiver, Sender};

pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_machine;

#[derive(Clone)]
pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
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

    async fn invoke_compute_graph(
        &self,
        request: &requests::InvokeComputeGraphRequest,
    ) -> Result<Vec<StateChange>> {
        let txn = self.db.transaction();
        let last_change_id = self.last_state_change_id.fetch_add(1, Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::InvokeComputeGraph(InvokeComputeGraphEvent {
                namespace: request.namespace.clone(),
                invocation_id: request.data_object.id.clone(),
                compute_graph: request.compute_graph_name.clone(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id(request.data_object.id.clone())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()?;
        state_machine::create_graph_input(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.compute_graph_name,
            request.data_object.clone(),
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use data_model::{
        filter::LabelsFilter,
        ComputeFn,
        ComputeGraph,
        ComputeGraphCode,
        Namespace,
        Node,
    };
    use requests::{CreateComputeGraphRequest, DeleteComputeGraphRequest};
    use tempfile::TempDir;
    use tokio;

    use super::{
        requests::{NamespaceRequest, RequestType},
        *,
    };

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
        let compute_graph = ComputeGraph {
            namespace: "namespace1".to_string(),
            name: "compute_graph1".to_string(),
            description: "A test compute graph".to_string(),
            code: ComputeGraphCode {
                sha256_hash: "hash".to_string(),
                size: 123,
                path: "http://url".to_string(),
            },
            create_at: 0,
            tomb_stoned: false,
            start_fn: Node::Compute(ComputeFn {
                name: "start_fn".to_string(),
                description: "Start function".to_string(),
                placement_constraints: LabelsFilter::default(),
                fn_name: "start_fn".to_string(),
            }),
            edges: HashMap::new(),
        };

        indexify_state
            .write(RequestType::CreateComputeGraph(CreateComputeGraphRequest {
                namespace: "namespace1".to_string(),
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
        assert!(compute_graphs.iter().any(|cg| cg.name == "compute_graph1"));

        // Delete the compute graph
        indexify_state
            .write(RequestType::DeleteComputeGraph(DeleteComputeGraphRequest {
                namespace: "namespace1".to_string(),
                name: "compute_graph1".to_string(),
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
        assert!(!compute_graphs.iter().any(|cg| cg.name == "compute_graph1"));

        Ok(())
    }
}
