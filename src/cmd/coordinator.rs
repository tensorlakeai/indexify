use std::sync::Arc;

use clap::Args as ClapArgs;

use super::GlobalArgs;
use crate::{coordinator_service::CoordinatorServer, prelude::*, server_config::ServerConfig};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    config_path: String,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { config_path } = self;

        info!("starting indexify coordinator, version: {}", crate::VERSION);
        let config = ServerConfig::from_path(&config_path)
            .unwrap_or_else(|_| panic!("failed to load config for coordinator: {}", config_path));
        let coordinator = CoordinatorServer::new(Arc::new(config))
            .await
            .expect("failed to create coordinator server");
        coordinator.run().await.expect("failed to run coordinator");
    }
}
