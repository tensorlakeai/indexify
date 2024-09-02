use anyhow::Result;
use data_model::{ComputeGraph, Namespace};

pub mod scanner;
pub mod serializer;
pub mod state_machine;

#[derive(Clone)]
pub struct IndexifyState {}

impl IndexifyState {
    pub async fn create_namespace(&self, name: &str) -> Result<()> {
        Ok(())
    }

    pub fn namespaces(&self) -> Result<Vec<Namespace>> {
        Ok(vec![])
    }

    pub fn get_compute_graph(&self, namespace: &str, name: &str) -> Result<ComputeGraph> {
        todo!()
    }

    pub fn compute_graphs(&self, namespace: &str) -> Result<Vec<ComputeGraph>> {
        Ok(vec![])
    }
}
