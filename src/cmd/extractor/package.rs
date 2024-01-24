use clap::Args as ClapArgs;

use crate::{cmd::GlobalArgs, prelude::*};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long)]
    dev: bool,
}

impl Args {
    pub async fn run(self, extractor_config_path: String, global_args: GlobalArgs) {
        let Self { dev } = self;

        let verbose = global_args.verbosity > 1;

        info!("starting indexify packager, version: {}", crate::VERSION);
        crate::package::Packager::new(extractor_config_path, dev)
            .expect("failed to create packager")
            .package(verbose)
            .await
            .expect("failed to package extractor");
    }
}
