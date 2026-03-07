#[cfg(feature = "kubernetes")]
pub mod configmap_image_resolver;
pub mod image_resolver;
#[cfg(feature = "kubernetes")]
pub mod kubernetes_secrets;
pub mod secrets;

#[cfg(feature = "kubernetes")]
pub use configmap_image_resolver::ConfigMapImageResolver;
pub use image_resolver::{DefaultImageResolver, ImageResolver};
#[cfg(feature = "kubernetes")]
pub use kubernetes_secrets::KubernetesSecretsResolver;
pub use secrets::{NoopSecretsResolver, SecretsResolver};
