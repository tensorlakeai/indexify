pub mod contacts;
pub mod gmail;

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::DataConnectorError;

#[derive(Clone, Debug)]
pub struct GoogleCredentials {
    refresh_token: String,
    client_id: String,
    client_secret: String,
}

impl GoogleCredentials {
    pub fn new(refresh_token: String, client_id: String, client_secret: String) -> Self {
        GoogleCredentials {
            refresh_token,
            client_id,
            client_secret,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RefreshTokenResponse {
    access_token: String,
    expires_in: u64,
    token_type: String,
}

pub async fn get_access_token(
    credentials: GoogleCredentials,
) -> Result<String, DataConnectorError> {
    let client = Client::new();

    let params = [
        ("refresh_token", credentials.refresh_token),
        ("client_id", credentials.client_id),
        ("client_secret", credentials.client_secret),
        ("grant_type", "refresh_token".into()),
    ];

    let response = client
        .post("https://oauth2.googleapis.com/token")
        .form(&params)
        .send()
        .await
        .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))?
        .json::<RefreshTokenResponse>()
        .await
        .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))?;

    Ok(response.access_token)
}
