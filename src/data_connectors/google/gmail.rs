use std::sync::Arc;

use crate::{
    persistence::{Repository, Text},
    DataConnector, DataConnectorError,
};
use async_trait::async_trait;

use super::GoogleCredentials;

pub struct GmailDataConnector {
    repository: Arc<Repository>,
    repository_name: String,
    _credentials: GoogleCredentials,
}

impl GmailDataConnector {
    pub fn new(
        repository: Arc<Repository>,
        repository_name: String,
        credentials: GoogleCredentials,
    ) -> Self {
        Self {
            repository,
            repository_name,
            _credentials: credentials,
        }
    }
}

#[async_trait]
impl DataConnector for GmailDataConnector {
    async fn fetch_data(&self) -> Result<Vec<Text>, DataConnectorError> {
        // TODO: implement data connector
        return Ok(Vec::new());
    }

    async fn index_data(&self, data: Vec<Text>) -> Result<(), DataConnectorError> {
        self.repository
            .add_text_to_repo(&self.repository_name, data, None)
            .await?;
        Ok(())
    }
}
