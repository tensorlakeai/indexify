use std::sync::Arc;

use clap::Args as ClapArgs;

use tracing_unwrap::ResultExt;

use crate::{
    cmd::GlobalArgs,
    extractor::{py_extractors::PythonExtractor, python_path, ExtractorTS},
    prelude::*,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long)]
    dev: bool,

    #[arg(long)]
    extractor_path: String,

    #[arg(long)]
    gpu: bool,
}

impl Args {
    pub async fn run(self, _: String, global_args: GlobalArgs) {
        let Self {
            dev,
            extractor_path,
            gpu,
        } = self;

        let verbose = global_args.verbosity > 1;

        python_path::set_python_path(&extractor_path).unwrap();
        let extractor = PythonExtractor::new_from_extractor_path(&extractor_path).unwrap_or_log();
        let extractor: ExtractorTS = Arc::new(extractor);
        let description = extractor.schemas().unwrap_or_log();

        info!("starting indexify packager, version: {}", crate::VERSION);
        crate::package::Packager::new(description, dev, extractor_path, gpu)
            .expect("failed to create packager")
            .package(verbose)
            .await
            .expect("failed to package extractor");
    }
}
