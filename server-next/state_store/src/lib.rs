use std::{
    f32::consts::E,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraph, Namespace};
use rocksdb::{
    ColumnFamilyDescriptor, Options, SingleThreaded, TransactionDB, TransactionDBOptions,
};
use state_machine::IndexifyObjectsColumns;
use strum::IntoEnumIterator;

pub mod scanner;
pub mod serializer;
pub mod state_machine;

#[derive(Clone)]
pub struct IndexifyState {
    pub db: Arc<TransactionDB>,
}

impl IndexifyState {
    pub fn new(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(path.clone())?;
        TransactionDB::open_default(path)
            .map(|db| Self { db: Arc::new(db) })
            .map_err(|e| anyhow!("failed to open db: {}", e))
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
