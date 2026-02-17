//! Secrets provider abstraction for injecting secrets into function executor
//! processes and sandbox containers.
//!
//! The open-source dataplane ships with a [`NoopSecretsProvider`] that returns
//! no secrets. Custom main binaries (e.g. in compute-engine-internal) can
//! inject their own implementation that fetches secrets from a platform API
//! or secrets manager.

use std::collections::HashMap;

use async_trait::async_trait;

/// Provider for fetching secrets that are injected as environment variables
/// into function executor processes and sandbox containers.
#[async_trait]
pub trait SecretsProvider: Send + Sync {
    /// Fetch secrets for a given namespace and list of secret names.
    ///
    /// Returns a map of secret_name -> secret_value. Implementations should
    /// handle retries internally.
    async fn fetch_secrets(
        &self,
        executor_id: &str,
        namespace: &str,
        secret_names: &[String],
    ) -> anyhow::Result<HashMap<String, String>>;
}

/// No-op secrets provider that always returns an empty map.
///
/// Used by the open-source dataplane binary where no secrets backend is
/// configured.
pub struct NoopSecretsProvider;

impl NoopSecretsProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoopSecretsProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretsProvider for NoopSecretsProvider {
    async fn fetch_secrets(
        &self,
        _executor_id: &str,
        _namespace: &str,
        _secret_names: &[String],
    ) -> anyhow::Result<HashMap<String, String>> {
        Ok(HashMap::new())
    }
}
