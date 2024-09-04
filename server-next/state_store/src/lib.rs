#![deny(unused_qualifications)]

use std::{fs, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraph, Namespace};
use indexify_utils::get_epoch_time_in_ms;
use rocksdb::{ColumnFamilyDescriptor, Options, TransactionDB, TransactionDBOptions};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;

pub mod requests;
pub mod scanner;
pub mod serializer;
pub mod state_machine;

use requests::RequestType;

#[derive(Clone)]
pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
}

impl IndexifyState {
    pub fn new(path: PathBuf) -> Result<Self> {
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
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn write(&self, request: RequestType) -> Result<()> {
        match request {
            RequestType::CreateNameSpace(namespace_request) => {
                self.create_namespace(&namespace_request.name).await
            }
            RequestType::CreateComputeGraph(graph) => self.create_compute_graph(graph).await,
        }
    }

    async fn create_namespace(&self, name: &str) -> Result<()> {
        let namespace = Namespace {
            name: name.to_string(),
            created_at: get_epoch_time_in_ms(),
        };
        state_machine::create_namespace(&namespace, &self.db)?;
        Ok(())
    }

    async fn create_compute_graph(&self, graph: ComputeGraph) -> Result<()> {
        state_machine::create_compute_graph(&self.db, &graph)
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use data_model::Namespace;
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
    async fn test_create_and_list_compute_graph() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state"))?;
        let ns = "namespace1";

        // Create namespaces
        indexify_state
            .write(RequestType::CreateNameSpace(NamespaceRequest {
                name: ns.to_string(),
            }))
            .await?;

        let graph_1 = ComputeGraph {
            name: "graph1".to_string(),
            namespace: ns.to_string(),
            code_path: "".to_string(),
            description: "graph1".to_string(),
            start_fn: data_model::ComputeFn {
                name: "fn1".to_string(),
                fn_name: "fn1".to_string(),
                description: "fn1".to_string(),
                placement_constraints: Default::default(),
            },
            edges: Default::default(),
            created_at: SystemTime::now(),
            tombstoned: false,
        };

        let graph_2 = ComputeGraph {
            name: "graph2".to_string(),
            namespace: ns.to_string(),
            code_path: "".to_string(),
            description: "graph2".to_string(),
            start_fn: data_model::ComputeFn {
                name: "fn1".to_string(),
                fn_name: "fn1".to_string(),
                description: "fn1".to_string(),
                placement_constraints: Default::default(),
            },
            edges: Default::default(),
            created_at: SystemTime::now(),
            tombstoned: false,
        };

        indexify_state
            .write(RequestType::CreateComputeGraph(graph_1))
            .await?;
        indexify_state
            .write(RequestType::CreateComputeGraph(graph_2))
            .await?;

        let reader = indexify_state.reader();
        let res = reader.filter_cf::<ComputeGraph, _>(
            IndexifyObjectsColumns::ComputeGraphs,
            |_| true,
            ComputeGraph::namespace_prefix(ns).as_bytes(),
            None,
            None,
        )?;

        assert_eq!(res.items.len(), 2);
        assert_eq!(res.items[0].name, "graph1");
        assert_eq!(res.items[0].namespace, ns);
        assert_eq!(res.items[1].name, "graph2");
        assert_eq!(res.items[1].namespace, ns);

        Ok(())
    }
}
