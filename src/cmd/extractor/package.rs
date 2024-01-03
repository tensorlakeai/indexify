use clap::Args as ClapArgs;

use crate::{cmd::GlobalArgs, prelude::*};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    dev: bool,

    #[arg(long)]
    verbose: bool,
}

impl Args {
    pub async fn run(self, extractor_config_path: String, _: GlobalArgs) {
        let Self { dev, verbose } = self;

        info!("starting indexify packager, version: {}", crate::VERSION);
        crate::package::Packager::new(extractor_config_path, dev)
            .expect("failed to create packager")
            .package(verbose)
            .await
            .expect("failed to package extractor");
    }
}
