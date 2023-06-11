use crate::{DataConnector, DataConnectorError};
use async_trait::async_trait;

pub struct GmailDataConnector {
    _access_token: String,
    _refresh_token: String,
}

impl GmailDataConnector {
    pub fn new(access_token: String, refresh_token: String) -> Self {
        Self {
            _access_token: access_token,
            _refresh_token: refresh_token,
        }
    }
}

#[async_trait]
impl DataConnector for GmailDataConnector {
    async fn fetch_data(&self) -> Result<Vec<String>, DataConnectorError> {
        // TODO: implement data connector
        return Ok(Vec::new());
    }
}
