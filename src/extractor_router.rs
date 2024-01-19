use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::error;

use crate::{
    api::Content,
    caching::{Cache, NoOpCache},
    coordinator_client::CoordinatorClient,
    indexify_coordinator::GetExtractorCoordinatesRequest,
    internal_api::{self, ExtractResponse},
};

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExtractContentCacheKey {
    content: Arc<Content>,
    input_params: Arc<Option<serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ExtractContentCacheValue {
    content: Arc<Vec<Content>>,
}

pub struct ExtractorRouter {
    coordinator_client: Arc<CoordinatorClient>,
    cache: Arc<RwLock<Box<dyn Cache<ExtractContentCacheKey, ExtractContentCacheValue>>>>,
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
            cache: Arc::new(RwLock::new(Box::new(NoOpCache::new()))),
            client: request_client,
        })
    }

    pub fn with_cache(
        mut self,
        cache: Arc<RwLock<Box<dyn Cache<ExtractContentCacheKey, ExtractContentCacheValue>>>>,
    ) -> Self {
        self.cache = cache;
        self
    }

    pub async fn extract_content(
        &self,
        extractor_name: &str,
        content: Content,
        input_params: Option<serde_json::Value>,
    ) -> Result<Vec<Content>, anyhow::Error> {
        // check cache first
        let cache_key = ExtractContentCacheKey {
            content: content.clone().into(), /* TODO: maybe there's a better way where we don't
                                              * need to clone the content? */
            input_params: input_params.clone().into(),
        };
        match self.cache.read().await.get(&cache_key).await {
            Ok(Some(cached)) => {
                tracing::debug!("found in cache: {:?}", cache_key);
                return Ok((*cached.content).clone());
            }
            Ok(None) => {
                tracing::debug!("not found in cache: {:?}", cache_key);
            }
            Err(e) => {
                // Still return OK to the caller, even if something went wrong with the cache
                // It will show up in the logs
                error!("unable to get from cache: {:?}. Error: {}", cache_key, e);
            }
        }

        // not found in cache - proceed to extract
        let request = internal_api::ExtractRequest {
            content: internal_api::Content {
                mime: content.content_type,
                bytes: content.bytes,
                feature: None,
                labels: HashMap::new(),
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

        let extractor_response: ExtractResponse = serde_json::from_str(&response_body)
            .map_err(|e| anyhow!("unable to extract response from json: {}", e))?;

        let content_list: Vec<Content> = extractor_response
            .content
            .into_iter()
            .map(|c| c.into())
            .collect();

        // cache the result
        {
            let cache_value = ExtractContentCacheValue {
                content: content_list.clone().into(),
            };
            let mut cache = self.cache.write().await;
            match cache.insert(cache_key.clone(), cache_value).await {
                Ok(_) => {
                    tracing::debug!("inserted into cache: {:?}", cache_key);
                }
                Err(e) => {
                    // Still return OK to the caller, even if we can't cache the result
                    // It will show up in the logs
                    error!(
                        "unable to insert into cache result for: {:?}. Error: {}",
                        cache_key, e
                    );
                }
            }
        }
        Ok(content_list)
    }
}
