use anyhow::Result;
use blob_store::BlobStorageConfig;
use tracing::subscriber;
use tracing_subscriber::{layer::SubscriberExt, Layer};

use crate::{config::ServerConfig, service::Service};

pub struct TestService {
    pub service: Service,
}

impl TestService {
    pub async fn new() -> Result<Self> {
        let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("trace"));
        let _ = subscriber::set_global_default(
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter)),
        );

        let temp_dir = tempfile::tempdir()?;

        let cfg = ServerConfig {
            state_store_path: temp_dir
                .path()
                .join("state_store")
                .to_str()
                .unwrap()
                .to_string(),
            blob_storage: BlobStorageConfig {
                path: format!(
                    "file://{}",
                    temp_dir.path().join("blob_store").to_str().unwrap()
                ),
            },
            ..Default::default()
        };
        let srv = Service::new(cfg).await?;

        Ok(Self { service: srv })
    }

    pub async fn process_all(&self) -> Result<()> {
        self.service.namespace_processor.process().await?;
        self.service.task_allocator.process().await?;

        Ok(())
    }

    pub async fn process_ns(&self) -> Result<()> {
        self.service.namespace_processor.process().await?;

        Ok(())
    }
}
