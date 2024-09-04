use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{Namespace, StateChangeId};
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
        })
    }

    pub fn get_state_change_watcher(&self) -> Receiver<StateChangeId> {
        self.state_change_rx.clone()
    }

    pub async fn write(&self, request: requests::RequestType) -> Result<()> {
        match request {
            requests::RequestType::InvokeComputeGraph(invoke_compute_graph_request) => {
                self.invoke_compute_graph(&invoke_compute_graph_request)
                    .await?;
            }
            requests::RequestType::CreateNameSpace(namespace_request) => {
                self.create_namespace(&namespace_request.name).await?;
            }
            requests::RequestType::CreateComputeGraph(create_compute_graph_request) => {
                self.create_compute_graph(&create_compute_graph_request)
                    .await?;
            }
            requests::RequestType::DeleteComputeGraph(delete_compute_graph_request) => {
                self.delete_compute_graph(&delete_compute_graph_request)
                    .await?;
            }
            requests::RequestType::CreateTasks(create_tasks_request) => {
                self.create_tasks(&create_tasks_request).await?;
            }
        }
        let state_changes = vec![StateChangeId::new(get_epoch_time_in_ms())];
        for state_change in state_changes {
            self.state_change_tx.send(state_change).unwrap();
        }
        Ok(())
    }

    async fn invoke_compute_graph(
        &self,
        request: &requests::InvokeComputeGraphRequest,
    ) -> Result<()> {
        let txn = self.db.transaction();
        state_machine::create_graph_input(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.compute_graph_name,
            request.data_object.clone(),
        )?;
        txn.commit()?;
        Ok(())
    }

    async fn create_namespace(&self, name: &str) -> Result<()> {
        let namespace = Namespace {
            name: name.to_string(),
            created_at: get_epoch_time_in_ms(),
        };
        state_machine::create_namespace(self.db.clone(), &namespace)?;
        Ok(())
    }

    async fn create_compute_graph(
        &self,
        create_compute_graph_request: &requests::CreateComputeGraphRequest,
    ) -> Result<()> {
        let compute_graph = create_compute_graph_request.compute_graph.clone();
        state_machine::create_compute_graph(self.db.clone(), compute_graph)?;
        Ok(())
    }

    async fn create_tasks(&self, create_tasks_request: &requests::CreateTaskRequest) -> Result<()> {
        let txn = self.db.transaction();
        state_machine::create_tasks(self.db.clone(), &txn, create_tasks_request.tasks.clone())?;
        txn.commit()?;
        Ok(())
    }

    async fn delete_compute_graph(
        &self,
        request: &requests::DeleteComputeGraphRequest,
    ) -> Result<()> {
        let txn = self.db.transaction();
        state_machine::delete_compute_graph(
            self.db.clone(),
            &txn,
            &request.namespace,
            &request.name,
        )?;
        txn.commit()?;
        Ok(())
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::requests::{NamespaceRequest, RequestType};
    use super::*;
    use data_model::{filter::LabelsFilter, ComputeFn, ComputeGraph, Namespace};
    use requests::{CreateComputeGraphRequest, DeleteComputeGraphRequest};
    use tempfile::TempDir;
    use tokio;

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
            code_path: "/path/to/code".to_string(),
            create_at: 0,
            tomb_stoned: false,
            start_fn: ComputeFn {
                name: "start_fn".to_string(),
                description: "Start function".to_string(),
                placement_constraints: LabelsFilter::default(),
                fn_name: "start_fn".to_string(),
            },
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
