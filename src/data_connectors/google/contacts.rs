use std::{collections::HashMap, sync::Arc};

use crate::{
    persistence::{Repository, Text},
    DataConnector, DataConnectorError,
};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, ACCEPT, AUTHORIZATION};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Name {
    #[serde(rename = "$t")]
    value: String,
}

#[derive(Debug, Deserialize)]
struct EmailAddress {
    value: String,
}

#[derive(Debug, Deserialize)]
struct PhoneNumber {
    #[serde(rename = "$t")]
    value: String,
}

#[derive(Debug, Deserialize)]
struct Contact {
    name: Name,
    email: Vec<EmailAddress>,
    phone: Vec<PhoneNumber>,
}

impl From<Contact> for Text {
    fn from(contact: Contact) -> Self {
        let name = contact.name.value;
        let mut emails = "".to_string();
        let mut phone_numbers = "".to_string();
        for email in contact.email {
            emails = format!("{}, {}", emails, email.value);
        }
        for phone_number in contact.phone {
            phone_numbers = format!("{}, {}", phone_numbers, phone_number.value);
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
    access_token: String,
    // TODO: Implement refresh token usage when access_token expires
    _refresh_token: String,
    repository: Arc<Repository>,
    repository_name: String,
}

impl GoogleContactsDataConnector {
    // Client will initiate OAuth flow and provide the access and refresh tokens
    pub fn new(
        access_token: String,
        refresh_token: String,
        repository: Arc<Repository>,
        repository_name: &String,
    ) -> Self {
        Self {
            access_token,
            _refresh_token: refresh_token,
            repository,
            repository_name: repository_name.into(),
        }
    }

    async fn fetch_data_internal(&self) -> Result<Vec<Contact>, DataConnectorError> {
        let url = "https://www.google.com/m8/feeds/contacts/default/full?alt=json&max-results=1000";

        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, "application/json".parse().unwrap());
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {}", self.access_token).parse().unwrap(),
        );

        let client = reqwest::blocking::Client::new();

        let response = client
            .get(url)
            .headers(headers)
            .send()
            .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))
            .and_then(|res| {
                res.error_for_status()
                    .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))
            })?
            .json::<serde_json::Value>()
            .map_err(|e| DataConnectorError::GoogleApiError(e.to_string()))?;

        // Parse the JSON response and extract the contacts
        let contacts: Vec<Contact> = response["feed"]["entry"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                let name = entry.get("gd$name").and_then(|name| {
                    let name_value = name.get("gd$fullName").and_then(|name| name.get("$t"));
                    name_value
                        .and_then(|value| value.as_str())
                        .map(|value| Name {
                            value: value.to_string(),
                        })
                });

                let email = entry.get("gd$email").and_then(|email| {
                    email.as_array().map(|emails| {
                        emails
                            .iter()
                            .filter_map(|email| email.get("address"))
                            .filter_map(|address| address.as_str())
                            .map(|address| EmailAddress {
                                value: address.to_string(),
                            })
                            .collect::<Vec<EmailAddress>>()
                    })
                });

                let phone = entry.get("gd$phoneNumber").and_then(|phone| {
                    phone.as_array().map(|phones| {
                        phones
                            .iter()
                            .filter_map(|phone| phone.get("$t"))
                            .filter_map(|number| number.as_str())
                            .map(|number| PhoneNumber {
                                value: number.to_string(),
                            })
                            .collect::<Vec<PhoneNumber>>()
                    })
                });

                Contact {
                    name: name.unwrap(),
                    email: email.unwrap(),
                    phone: phone.unwrap(),
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
