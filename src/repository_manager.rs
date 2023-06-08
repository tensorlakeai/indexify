use std::sync::Arc;
use thiserror::Error;

use crate::persistence::{DataRepository, Repository, RepositoryError};

#[derive(Error, Debug)]
pub enum DataRepositoryError {
    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error("Data repository not found: {0}")]
    NotFound(String),
}

pub struct DataRepositoryManager {
    repository: Arc<Repository>,
}

impl DataRepositoryManager {
    pub async fn new(db_url: &str) -> Result<Self, RepositoryError> {
        let repository = Arc::new(Repository::new(db_url).await?);
        Ok(Self { repository })
    }

    pub fn list_repositories(&self) -> Result<Vec<DataRepository>, DataRepositoryError> {
        Ok(vec![])
    }

    pub fn sync(&self, repository: &DataRepository) -> Result<(), DataRepositoryError> {
        Ok(())
    }

    pub fn get(&self, name: String) -> Result<DataRepository, DataRepositoryError> {
        Err(DataRepositoryError::NotFound(name))
    }
}
