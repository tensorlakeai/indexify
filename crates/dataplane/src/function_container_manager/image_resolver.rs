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

/// Default image resolver that returns errors for all methods.
///
/// The calling code in `FunctionContainerManager` first checks
/// `sandbox_metadata.image` from the server's description (primary source for
/// sandbox containers), and only falls back to the resolver if not set.
/// Custom main functions can inject their own `ImageResolver` with real logic.
pub struct DefaultImageResolver;

impl DefaultImageResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultImageResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ImageResolver for DefaultImageResolver {
    fn sandbox_image_for_pool(&self, _namespace: &str, _pool_id: &str) -> anyhow::Result<String> {
        anyhow::bail!(
            "No image configured — override ImageResolver or set sandbox_metadata.image on the description"
        )
    }

    fn sandbox_image(&self, _namespace: &str, _sandbox_id: &str) -> anyhow::Result<String> {
        anyhow::bail!("No image configured — override ImageResolver")
    }

    fn function_image(
        &self,
        _namespace: &str,
        _app: &str,
        _function: &str,
        _version: &str,
    ) -> anyhow::Result<String> {
        anyhow::bail!("No image configured — override ImageResolver")
    }
}
