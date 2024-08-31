use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::GetExtractorCoordinatesRequest;
use internal_api::ExtractResponse;

use crate::{http_api_objects::Content, coordinator_client::CoordinatorClient};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

pub struct ExtractorRouter {
    coordinator_client: Arc<CoordinatorClient>,
    client: reqwest::Client,
}

impl ExtractorRouter {
    pub fn new(coordinator_client: Arc<CoordinatorClient>) -> Result<Self> {
        let request_client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| anyhow!("unable to create request client: {}", e))?;
        Ok(Self {
            coordinator_client,
            client: request_client,
        })
    }

    pub async fn extract_content(
        &self,
        extractor_name: &str,
        content: Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<ExtractResponse, anyhow::Error> {
        let request = internal_api::ExtractRequest {
            content: internal_api::Content {
                content_type: content.content_type,
                bytes: content.bytes,
                features: vec![],
                labels: HashMap::new(),
            },
            extractor_name: extractor_name.to_string(),
            input_params,
        };

        let req = GetExtractorCoordinatesRequest {
            extractor: extractor_name.to_string(),
        };
        let resp = self
            .coordinator_client
            .get()
            .await?
            .get_extractor_coordinates(req)
            .await?
            .into_inner();
        let addresses = resp.addrs;
        if addresses.is_empty() {
            return Err(anyhow!("no extractor found"));
        }
        let extractor_addr = addresses[0].clone();
        let resp = self
            .client
            .post(&format!("http://{}/extract", extractor_addr))
            .json(&request)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("unable to embed query: {}", e))?;

        if !&resp.status().is_success() {
            return Err(anyhow!(
                "unable to extract query: status: {}, error: {}",
                resp.status(),
                resp.text().await?
            ));
        }
        let response_body = resp
            .text()
            .await
            .map_err(|e| anyhow!("unable to get response body: {}", e))?;

        let extractor_response: internal_api::ExtractResponse =
            serde_json::from_str(&response_body)
                .map_err(|e| anyhow!("unable to extract response from json: {}", e))?;

        Ok(extractor_response)
    }
}
