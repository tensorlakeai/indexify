pub mod google;
use std::sync::Arc;
use thiserror::Error;

use async_trait::async_trait;

use crate::persistence::{Repository, RepositoryError, SourceType, Text};

use self::google::contacts::GoogleContactsDataConnector;
use self::google::gmail::GmailDataConnector;
use self::google::GoogleCredentials;

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
                    refresh_token,
                    client_id,
                    client_secret,
                } => Arc::new(GmailDataConnector::new(
                    repository.clone(),
                    repository_name.into(),
                    GoogleCredentials::new(refresh_token, client_id, client_secret),
                )) as DataConnectorTS,
                SourceType::GoogleContact {
                    refresh_token,
                    client_id,
                    client_secret,
                } => Arc::new(GoogleContactsDataConnector::new(
                    repository.clone(),
                    repository_name,
                    GoogleCredentials::new(refresh_token, client_id, client_secret),
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
