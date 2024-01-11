use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use crate::{
    api::Content,
    coordinator_client::CoordinatorClient,
    indexify_coordinator::GetExtractorCoordinatesRequest,
    internal_api::{self, ExtractResponse},
};

pub struct ExtractorRouter {
    coordinator_client: Arc<CoordinatorClient>,
}



impl ExtractorRouter {
    pub fn new(coordinator_client: Arc<CoordinatorClient>) -> Self {
        Self { coordinator_client }
    }

    pub async fn extract_content(
        &self,
        extractor_name: &str,
        content: Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<Vec<Content>, anyhow::Error> {
        let request = internal_api::ExtractRequest {
            content: internal_api::Content {
                mime: content.content_type,
                bytes: content.bytes,
                feature: None,
                metadata: HashMap::new(),
            },
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
        let resp = reqwest::Client::new()
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

        let extractor_response: ExtractResponse = serde_json::from_str(&response_body)
            .map_err(|e| anyhow!("unable to extract response from json: {}", e))?;

        let content_list = extractor_response
            .content
            .into_iter()
            .map(|c| c.into())
            .collect();
        Ok(content_list)
    }
}
