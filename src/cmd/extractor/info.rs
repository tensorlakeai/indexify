use std::sync::Arc;

use clap::Args as ClapArgs;

use crate::{
    cmd::GlobalArgs,
    extractor::{extractor_runner, py_extractors::PythonExtractor, python_path},
    server_config::ExtractorConfig,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short = 'c', long)]
    config_path: Option<String>,

    #[arg(long)]
    cache_dir: Option<String>,

    #[arg(short = 'n', long)]
    name: Option<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self {
            config_path: _,
            cache_dir: _,
            name: _,
        } = self;

        let extractor_config_path = self.config_path.unwrap_or("indexify.yaml".to_string());
        python_path::set_python_path(&extractor_config_path).unwrap();

        let extractor_config = ExtractorConfig::from_path(&extractor_config_path).unwrap();
        let extractor = PythonExtractor::new_from_extractor_path(&extractor_config.path).unwrap();
        let extractor_runner = extractor_runner::ExtractorRunner::new(Arc::new(extractor));
        let info = extractor_runner.info().unwrap();
        println!("{}", serde_json::to_string_pretty(&info).unwrap());
    }
}
