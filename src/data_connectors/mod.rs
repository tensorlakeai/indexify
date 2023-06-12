pub mod google;
use std::sync::Arc;
use thiserror::Error;

use async_trait::async_trait;

use crate::persistence::{Repository, RepositoryError, SourceType, Text};

use self::google::contacts::GoogleContactsDataConnector;
use self::google::gmail::GmailDataConnector;

#[derive(Error, Debug)]
pub enum DataConnectorError {
    #[error(transparent)]
    Persistence(#[from] RepositoryError),

    #[error("GoogleApiError: {0}")]
    GoogleApiError(String),
}

pub type DataConnectorTS = Arc<dyn DataConnector + Sync + Send>;

#[async_trait]
pub trait DataConnector {
    async fn fetch_data(&self) -> Result<Vec<Text>, DataConnectorError>;
    async fn index_data(&self, data: Vec<Text>) -> Result<(), DataConnectorError>;
}

pub struct DataConnectorManager {
    readers: Vec<DataConnectorTS>,
}

impl DataConnectorManager {
    pub async fn new(
        repository: Arc<Repository>,
        repository_name: &String,
    ) -> Result<Self, DataConnectorError> {
        let mut readers = Vec::new();
        let data_connector_config = repository
            .get_data_connectors(repository_name.into())
            .await?;
        for config in data_connector_config {
            let reader = match config.source {
                SourceType::Gmail {
                    access_token,
                    refresh_token,
                } => Arc::new(GmailDataConnector::new(
                    access_token,
                    refresh_token,
                    repository.clone(),
                    repository_name.into(),
                )) as DataConnectorTS,
                SourceType::GoogleContact {
                    access_token,
                    refresh_token,
                } => Arc::new(GoogleContactsDataConnector::new(
                    access_token,
                    refresh_token,
                    repository.clone(),
                    repository_name,
                )) as DataConnectorTS,
            };
            readers.push(reader);
        }
        Ok(Self { readers })
    }

    pub async fn run_data_connectors(&self) -> Result<(), DataConnectorError> {
        for reader in &self.readers {
            let data = reader.fetch_data().await?;
            reader.index_data(data).await?;
        }
        Ok(())
    }

    pub fn get_data_connectors(&self) -> Result<Vec<DataConnectorTS>, DataConnectorError> {
        Ok(self.readers.clone())
    }
}
