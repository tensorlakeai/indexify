use std::sync::Arc;

use clap::Args as ClapArgs;

use super::GlobalArgs;
use crate::{coordinator_service::CoordinatorServer, prelude::*, server_config::ServerConfig};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    config_path: String,

    /// Override the node id for this coordinator.
    #[arg(long)]
    node_id: Option<u64>,

    /// Initialize the cluster with this node as the leader. This should only be
    /// run during initial bootstrap.
    #[arg(long)]
    initialize: bool,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { config_path, .. } = self;

        info!("starting indexify coordinator, version: {}", crate::VERSION);
        let mut config = ServerConfig::from_path(&config_path).unwrap_or_else(|e| {
            panic!(
                "failed to load config for coordinator: {}: {}",
                config_path, e
            )
        });

        config.initialize_raft = self.initialize;

        if let Some(node_id) = self.node_id {
            config.node_id = node_id;
        }

        let registry = Arc::new(crate::metrics::init_provider());
        let coordinator = CoordinatorServer::new(Arc::new(config), registry)
            .await
            .expect("failed to create coordinator server");
        coordinator.run().await.expect("failed to run coordinator");
    }
}
