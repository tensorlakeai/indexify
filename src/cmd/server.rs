use std::sync::Arc;

use clap::Args as ClapArgs;

use super::GlobalArgs;
use crate::{
    coordinator_service::CoordinatorServer, prelude::*, server, server_config::ServerConfig,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// path to the server config file
    #[arg(long, short = 'c')]
    config_path: String,

    #[arg(short, long)]
    dev_mode: bool,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self {
            config_path,
            dev_mode,
        } = self;

        info!("starting indexify server, version: {}", crate::VERSION);
        let config = ServerConfig::from_path(&config_path)
            .unwrap_or_else(|e| panic!("failed to load config: {}, error: {:?}", config_path, e));

        debug!("Server config is: {:?}", config);
        let server =
            server::Server::new(Arc::new(config.clone())).expect("failed to create server");

        let server_handle = tokio::spawn(async move {
            server.run().await.unwrap();
        });
        if dev_mode {
            let coordinator = CoordinatorServer::new(Arc::new(config.clone()))
                .await
                .expect("failed to create coordinator server");
            let coordinator_handle = tokio::spawn(async move {
                coordinator.run().await.unwrap();
            });
            tokio::try_join!(server_handle, coordinator_handle)
                .expect("failed to run server or coordinator server");
        } else {
            server_handle.await.expect("failed to run server");
        }
    }
}
