use clap::Args as ClapArgs;

use crate::{cmd::GlobalArgs, prelude::*};

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// path to create the extractor template
    #[arg(long)]
    path: Option<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { path } = self;

        let current_dir = std::env::current_dir().expect("cannot get current directory");
        let current_dir = current_dir.to_str().to_owned().unwrap();
        let path = path.unwrap_or_else(|| current_dir.to_string());
        info!("creating new extractor at: {}", path);
        crate::extractor::create_extractor_template(&path, "").unwrap()
    }
}
