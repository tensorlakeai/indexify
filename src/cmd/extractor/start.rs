use std::sync::Arc;

use clap::Args as ClapArgs;

use crate::{
    cmd::GlobalArgs,
    executor_server::ExecutorServer,
    prelude::*,
    server_config::ExecutorConfig,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// address of the extractor to advertise to indexify control plane
    #[arg(long)]
    advertise_addr: Option<String>,

    /// address of the indexify server
    #[arg(long)]
    coordinator_addr: String,
}

impl Args {
    pub async fn run(self, extractor_config_path: String, _: GlobalArgs) {
        let Self {
            advertise_addr,
            coordinator_addr,
        } = self;

        info!("starting indexify executor, version: {}", crate::VERSION);
        let executor_config = Arc::new(
            ExecutorConfig::default()
                .with_advertise_addr(advertise_addr)
                .expect("unable to use the provided advertise address")
                .with_coordinator_addr(coordinator_addr),
        );
        ExecutorServer::new(&extractor_config_path, executor_config)
            .await
            .expect("failed to create executor server")
            .run()
            .await
            .expect("failed to run executor server");
    }
}
