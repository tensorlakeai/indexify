//! Kubernetes Secret-backed secrets provider.
//!
//! Resolves function secrets by looking up Kubernetes Secrets whose names
//! match the entries in `secret_names`. Each Secret's `data` entries are
//! base64-decoded and injected as environment variables into the function
//! container.
//!
//! # Usage
//!
//! In the dataplane config, set `secret_names` in the container description
//! to the names of Kubernetes Secrets that hold the function's credentials.
//! For example, if a function needs access to a database, create a Secret:
//!
//! ```yaml
//! apiVersion: v1
//! kind: Secret
//! metadata:
//!   name: my-function
//! stringData:
//!   DATABASE_URL: "postgres://user:pass@host/db"
//!   API_KEY: "secret-value"
//! ```
//!
//! Then list `my-function` in the container description's `secret_names`.
//! All key-value pairs from the Secret will be injected as env vars.

use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};

use super::secrets::SecretsResolver;

pub struct KubernetesSecretsResolver {
    client: Client,
    namespace: String,
}

impl KubernetesSecretsResolver {
    /// Create a new provider that reads Secrets from `namespace`.
    pub async fn new(namespace: String) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("Failed to create Kubernetes client for KubernetesSecretsResolver")?;
        Ok(Self { client, namespace })
    }
}

#[async_trait]
impl SecretsResolver for KubernetesSecretsResolver {
    /// For each name in `secret_names`, fetches the Kubernetes Secret with
    /// that name from the configured namespace and merges all its key-value
    /// pairs into the returned map. If a Secret does not exist it is silently
    /// skipped; other API errors are returned as failures.
    async fn fetch_secrets(
        &self,
        _executor_id: &str,
        namespace: &str,
        secret_names: &[String],
    ) -> Result<HashMap<String, String>> {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
        let mut result = HashMap::new();

        for secret_name in secret_names {
            let name = format!("{namespace}-{secret_name}");
            let secret =
                match api.get_opt(&name).await.with_context(|| {
                    format!("Failed to read Secret {}/{}", self.namespace, name,)
                })? {
                    Some(s) => s,
                    // Secret not found — skip silently; the function may not need
                    // this particular secret in this deployment.
                    None => {
                        tracing::warn!(
                            secret = %name,
                            namespace = %self.namespace,
                            "Secret not found, skipping"
                        );
                        continue;
                    }
                };

            if let Some(data) = secret.data {
                for (key, byte_string) in data {
                    match String::from_utf8(byte_string.0) {
                        Ok(value) => {
                            result.insert(key, value);
                        }
                        Err(e) => {
                            tracing::warn!(
                                secret = %secret_name,
                                key = %key,
                                error = %e,
                                "Secret key is not valid UTF-8, skipping"
                            );
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}
