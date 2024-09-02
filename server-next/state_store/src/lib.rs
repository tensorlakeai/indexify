use std::sync::Arc;

use anyhow::Result;
use data_model::{ComputeGraph, Namespace};
use rocksdb::DB;

pub mod scanner;
pub mod serializer;
pub mod state_machine;

pub struct IndexifyState {
    pub db: Arc<DB>,
}

impl IndexifyState {

    pub fn new(path: &str) -> Self {
        todo!()
    }
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

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
    }
}
