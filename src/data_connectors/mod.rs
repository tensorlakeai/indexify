pub mod google;
use std::sync::Arc;
use thiserror::Error;

use async_trait::async_trait;

use crate::persistence::{Repository, RepositoryError, SourceType};

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
    async fn fetch_data(&self) -> Result<Vec<String>, DataConnectorError>;
}

pub struct DataConnectorManager {
    readers: Vec<DataConnectorTS>,
}

impl DataConnectorManager {
    pub async fn new(
        repository: &Repository,
        repository_name: String,
    ) -> Result<Self, DataConnectorError> {
        let mut readers = Vec::new();
        let data_connector_config = repository.get_data_connectors(repository_name).await?;
        for config in data_connector_config {
            let reader = match config.source {
                SourceType::Gmail {
                    access_token,
                    refresh_token,
                } => Arc::new(GmailDataConnector::new(access_token, refresh_token))
                    as DataConnectorTS,
                SourceType::GoogleContact {
                    access_token,
                    refresh_token,
                } => Arc::new(GoogleContactsDataConnector::new(
                    access_token,
                    refresh_token,
                )) as DataConnectorTS,
            };
            readers.push(reader);
        }
        Ok(Self { readers })
    }

    pub fn get_data_connectors(&self) -> Result<Vec<DataConnectorTS>, DataConnectorError> {
        Ok(self.readers.clone())
    }
}
