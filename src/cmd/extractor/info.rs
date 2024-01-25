use std::sync::Arc;

use clap::Args as ClapArgs;

use crate::{
    cmd::GlobalArgs,
    extractor::{extractor_runner, py_extractors::PythonExtractor, python_path},
    server_config::ExtractorConfig,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long)]
    extractor_path: Option<String>,

    #[arg(long)]
    cache_dir: Option<String>,

    #[arg(short = 'n', long)]
    name: Option<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self {
            extractor_path,
            cache_dir: _,
            name: _,
        } = self;
        
        let extractor_path = match extractor_path {
            Some(path) => path,
            None => {
                ExtractorConfig::from_path("indexify.yaml")
                    .unwrap_or_else(|_| panic!("unable to load extractor config from indexify.yaml, and extractor path is not provided explicitly via --extractor-path"))
                    .path
            }
        };

        python_path::set_python_path(&extractor_path).unwrap();
        let extractor = PythonExtractor::new_from_extractor_path(&extractor_path).unwrap();
        let extractor_runner = extractor_runner::ExtractorRunner::new(Arc::new(extractor));
        let info = extractor_runner.info().unwrap();
        println!("{}", serde_json::to_string_pretty(&info).unwrap());
    }
}
