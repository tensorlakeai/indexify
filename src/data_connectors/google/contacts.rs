use std::{collections::HashMap, sync::Arc};

use crate::{
    persistence::{Repository, Text},
    DataConnector, DataConnectorError,
};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, ACCEPT, AUTHORIZATION};
use serde::Deserialize;

use super::{get_access_token, GoogleCredentials};

#[derive(Debug, Deserialize)]
struct Name {
    displayName: String,
}

#[derive(Debug, Deserialize)]
struct EmailAddress {
    value: String,
}

#[derive(Debug, Deserialize)]
struct PhoneNumber {
    value: String,
}

#[derive(Debug, Deserialize)]
struct Connection {
    names: Option<Vec<Name>>,
    phoneNumbers: Option<Vec<PhoneNumber>>,
    emailAddresses: Option<Vec<EmailAddress>>,
}

#[derive(Debug, Deserialize)]
struct ContactResponse {
    connections: Vec<Connection>,
}

#[derive(Debug, Deserialize)]
struct Contact {
    name: String,
    emails: Vec<String>,
    phone: Vec<String>,
}

impl From<Contact> for Text {
    fn from(contact: Contact) -> Self {
        let name = contact.name;
        let mut emails = "".to_string();
        let mut phone_numbers = "".to_string();
        for email in contact.emails {
            emails = format!("{}, {}", emails, email);
        }
        for phone_number in contact.phone {
            phone_numbers = format!("{}, {}", phone_numbers, phone_number);
        }
        Text {
            text: format!(
                "name: {}\nemails: {}\nphone: {}",
                name, emails, phone_numbers
            ),
            metadata: HashMap::new(),
        }
    }
}

pub struct GoogleContactsDataConnector {
    repository: Arc<Repository>,
    repository_name: String,
    credentials: GoogleCredentials,
}

impl GoogleContactsDataConnector {
    // Client will initiate OAuth flow and provide the access and refresh tokens
    pub fn new(
        repository: Arc<Repository>,
        repository_name: &String,
        credentials: GoogleCredentials,
    ) -> Self {
        Self {
            repository,
            repository_name: repository_name.into(),
            credentials,
        }
    }

    async fn fetch_data_internal(&self) -> Result<Vec<Contact>, DataConnectorError> {
        let url = "https://people.googleapis.com/v1/people/me/connections?personFields=names,phone_numbers,email_addresses";
        let access_token = get_access_token(self.credentials.clone()).await?;
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, "application/json".parse().unwrap());
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", access_token).parse().unwrap(),
        );

        let client = reqwest::Client::new();

        let response = client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))?;

        // Parse the JSON response and extract the contacts
        let contact_response: ContactResponse = serde_json::from_value(response).unwrap();
        let contacts = contact_response
            .connections
            .into_iter()
            .map(|connection| {
                let name = connection
                    .names
                    .unwrap_or(Vec::new())
                    .into_iter()
                    .next()
                    .map(|name| name.displayName)
                    .unwrap_or_default();
                let phone_numbers = connection
                    .phoneNumbers
                    .unwrap_or(Vec::new())
                    .into_iter()
                    .map(|phone_number| phone_number.value)
                    .collect();
                let emails = connection
                    .emailAddresses
                    .unwrap_or(Vec::new())
                    .into_iter()
                    .map(|email| email.value)
                    .collect();

                Contact {
                    name,
                    emails,
                    phone: phone_numbers,
                }
            })
            .collect();
        Ok(contacts)
    }
}

#[async_trait]
impl DataConnector for GoogleContactsDataConnector {
    async fn fetch_data(&self) -> Result<Vec<Text>, DataConnectorError> {
        let contacts = self.fetch_data_internal().await?;
        let data: Vec<Text> = contacts.into_iter().map(|contact| contact.into()).collect();
        Ok(data)
    }

    async fn index_data(&self, data: Vec<Text>) -> Result<(), DataConnectorError> {
        self.repository
            .add_text_to_repo(&self.repository_name, data, None)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util;

    use super::*;

    #[tokio::test]
    async fn test_simple_data_fetching() {
        // TODO: set up env vars in CI, testing on my own account locally
        let refresh_token = std::env::var("GOOGLE_REFRESH_TOKEN");
        let client_id = std::env::var("GOOGLE_CLIENT_ID");
        let client_secret = std::env::var("GOOGLE_CLIENT_SECRET");
        if refresh_token.is_err() || client_id.is_err() || client_secret.is_err() {
            return;
        }
        let credentials = GoogleCredentials {
            refresh_token: refresh_token.unwrap(),
            client_id: client_id.unwrap(),
            client_secret: client_secret.unwrap(),
        };

        let repository = test_util::db_utils::create_repository().await;
        let data_connector = GoogleContactsDataConnector::new(
            repository,
            &"contact_data_connector_test".to_string(),
            credentials,
        );

        let data = data_connector.fetch_data().await.unwrap();

        assert_eq!(data.len(), 23);
    }
}
