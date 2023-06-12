use std::sync::Arc;

use crate::{
    persistence::{Repository, Text},
    DataConnector, DataConnectorError,
};
use async_trait::async_trait;

pub struct GmailDataConnector {
    _access_token: String,
    _refresh_token: String,
    _repository: Arc<Repository>,
    _repository_name: String,
}

impl GmailDataConnector {
    pub fn new(
        _access_token: String,
        _refresh_token: String,
        _repository: Arc<Repository>,
        _repository_name: String,
    ) -> Self {
        Self {
            _access_token,
            _refresh_token,
            _repository,
            _repository_name,
        }
    }
}

#[async_trait]
impl DataConnector for GmailDataConnector {
    async fn fetch_data(&self) -> Result<Vec<Text>, DataConnectorError> {
        // TODO: implement data connector
        return Ok(Vec::new());
    }

    async fn index_data(&self, _data: Vec<Text>) -> Result<(), DataConnectorError> {
        // TODO: implement indexing
        Ok(())
    }
}
