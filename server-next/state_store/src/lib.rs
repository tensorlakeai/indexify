use std::{
    f32::consts::E,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraph, Namespace};
use indexify_utils::get_epoch_time_in_ms;
use rocksdb::{
    ColumnFamilyDescriptor, Options, SingleThreaded, TransactionDB, TransactionDBOptions, DB,
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

    pub async fn create_namespace(&self, name: &str) -> Result<()> {
        let namespace = Namespace {
            name: name.to_string(),
            created_at: get_epoch_time_in_ms(),
        };
        state_machine::create_namespace(&namespace, self.db.clone())?;
        Ok(())
    }

    pub fn reader(&self) -> scanner::StateReader {
        scanner::StateReader::new(self.db.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_model::Namespace;
    use tempfile::TempDir;
    use tokio;

    #[tokio::test]
    async fn test_create_and_list_namespaces() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let indexify_state = IndexifyState::new("/tmp/foo".into())?;

        // Create namespaces
        indexify_state.create_namespace("namespace1").await?;
        indexify_state.create_namespace("namespace2").await?;

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
}
