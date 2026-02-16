//! Image resolution for function executor containers.

/// Resolves container images for function executors.
pub trait ImageResolver: Send + Sync {
    fn sandbox_image_for_pool(&self, namespace: &str, pool_id: &str) -> anyhow::Result<String>;
    fn sandbox_image(&self, namespace: &str, sandbox_id: &str) -> anyhow::Result<String>;
    fn function_image(
        &self,
        namespace: &str,
        app: &str,
        function: &str,
        version: &str,
    ) -> anyhow::Result<String>;
}

/// Default image resolver that uses a configured fallback image.
///
/// For function containers, this resolver returns the configured
/// `default_image`. In production, a custom `ImageResolver` will call
/// an internal image resolution API to look up per-function images.
///
/// Sandbox containers are unaffected — the server provides their image
/// via `sandbox_metadata.image`, which is checked before the resolver.
pub struct DefaultImageResolver {
    default_image: Option<String>,
}

impl DefaultImageResolver {
    pub fn new(default_image: Option<String>) -> Self {
        Self { default_image }
    }

    fn resolve_or_bail(&self, context: &str) -> anyhow::Result<String> {
        self.default_image.clone().ok_or_else(|| {
            anyhow::anyhow!(
                "No image configured for {context} — set default_function_image in config \
                 or override ImageResolver"
            )
        })
    }
}

impl Default for DefaultImageResolver {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ImageResolver for DefaultImageResolver {
    fn sandbox_image_for_pool(&self, _namespace: &str, _pool_id: &str) -> anyhow::Result<String> {
        self.resolve_or_bail("sandbox pool")
    }

    fn sandbox_image(&self, _namespace: &str, _sandbox_id: &str) -> anyhow::Result<String> {
        self.resolve_or_bail("sandbox")
    }

    fn function_image(
        &self,
        _namespace: &str,
        _app: &str,
        _function: &str,
        _version: &str,
    ) -> anyhow::Result<String> {
        self.resolve_or_bail("function")
    }
}
