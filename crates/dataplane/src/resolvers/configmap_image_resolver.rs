//! Kubernetes ConfigMap-backed image resolver.
//!
//! Resolves function container images from a Kubernetes ConfigMap whose keys
//! are function names and whose values are OCI image references. A fallback
//! image is returned when no mapping is found for a given function.
//!
//! # ConfigMap format
//!
//! ```yaml
//! apiVersion: v1
//! kind: ConfigMap
//! metadata:
//!   name: indexify-function-images
//! data:
//!   my-function: "myrepo/my-function:v1.2.3"
//!   other-function: "myrepo/other-function:latest"
//! ```

use anyhow::{Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};

use super::image_resolver::ImageResolver;

pub struct ConfigMapImageResolver {
    client: Client,
    namespace: String,
    configmap_name: String,
    fallback_image: Option<String>,
}

impl ConfigMapImageResolver {
    /// Create a new resolver backed by a Kubernetes ConfigMap.
    ///
    /// # Arguments
    /// - `namespace`: the Kubernetes namespace containing the ConfigMap.
    /// - `configmap_name`: name of the ConfigMap (e.g.
    ///   `indexify-function-images`).
    /// - `fallback_image`: returned when the ConfigMap has no entry for a
    ///   function.
    pub async fn new(
        namespace: String,
        configmap_name: String,
        fallback_image: Option<String>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("Failed to create Kubernetes client for ConfigMapImageResolver")?;
        Ok(Self {
            client,
            namespace,
            configmap_name,
            fallback_image,
        })
    }

    /// Look up `key` in the ConfigMap data, returning the image reference if
    /// found. Falls back to `self.fallback_image` when the key is absent.
    async fn lookup(&self, key: &str) -> Result<Option<String>> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let cm = api.get_opt(&self.configmap_name).await.with_context(|| {
            format!(
                "Failed to read ConfigMap {}/{}",
                self.namespace, self.configmap_name
            )
        })?;

        let image = cm
            .and_then(|cm| cm.data)
            .and_then(|data| data.get(key).cloned())
            .or_else(|| self.fallback_image.clone());

        Ok(image)
    }
}

#[async_trait]
impl ImageResolver for ConfigMapImageResolver {
    async fn sandbox_image_for_pool(&self, _namespace: &str, pool_id: &str) -> Result<String> {
        self.lookup(pool_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No image found for pool '{}'", pool_id))
    }

    async fn sandbox_image(&self, _namespace: &str, sandbox_id: &str) -> Result<String> {
        self.lookup(sandbox_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No image found for sandbox '{}'", sandbox_id))
    }

    /// Returns the image mapped to `function` in the ConfigMap, or the
    /// fallback image if no explicit mapping exists.
    async fn function_image(
        &self,
        _namespace: &str,
        _app: &str,
        function: &str,
        _version: &str,
    ) -> Result<Option<String>> {
        self.lookup(function).await
    }
}
