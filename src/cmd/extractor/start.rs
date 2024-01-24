use std::sync::Arc;

use clap::Args as ClapArgs;

use crate::{
    cmd::GlobalArgs,
    executor_server::ExecutorServer,
    prelude::*,
    server_config::{ExecutorConfig, ExtractorConfig},
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// address of the extractor to advertise to indexify control plane
    #[arg(long)]
    advertise_addr: Option<String>,

    /// address of the indexify server
    #[arg(long)]
    coordinator_addr: String,

    #[arg(long)]
    ingestion_addr: String,

    #[arg(long)]
    extractor_path: Option<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self {
            advertise_addr,
            coordinator_addr,
            ingestion_addr,
            extractor_path,
        } = self;

        info!("starting indexify executor, version: {}", crate::VERSION);
        let extractor_path = match extractor_path {
            Some(path) => path,
            None => {
                ExtractorConfig::from_path("indexify.yaml")
                    .unwrap_or_else(|_| panic!("unable to load extractor config from indexify.yaml, and extractor path is not provided explicitly via --extractor-path"))
                    .path
            }
        };
        let executor_config = Arc::new(
            ExecutorConfig::default()
                .with_advertise_addr(advertise_addr)
                .expect("unable to use the provided advertise address")
                .with_coordinator_addr(coordinator_addr)
                .with_ingestion_addr(ingestion_addr)
                .with_extractor_path(extractor_path),
        );
        ExecutorServer::new(executor_config)
            .await
            .expect("failed to create executor server")
            .run()
            .await
            .expect("failed to run executor server");
    }
}
