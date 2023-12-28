use clap::{Args as ClapArgs, Subcommand};

use super::GlobalArgs;
use crate::prelude::*;

mod extract;
mod info;
mod new;
mod package;
mod start;

#[derive(Debug, ClapArgs)]
pub struct Args {
    /// path to the extractor config file
    #[arg(global = true, short = 'c', long, default_value = "indexify.yaml")]
    config_path: String,

    #[command(subcommand)]
    command: Command,
}

impl Args {
    pub async fn run(self, global_args: GlobalArgs) {
        let Self {
            config_path,
            command,
        } = self;

        let extractor_config_path = config_path;
        info!("using config file: {}", &extractor_config_path);
        match command {
            Command::Extract(args) => args.run(extractor_config_path, global_args).await,
            Command::New(args) => args.run(extractor_config_path, global_args).await,
            Command::Package(args) => args.run(extractor_config_path, global_args).await,
            Command::Start(args) => args.run(extractor_config_path, global_args).await,
            Command::Info(args) => args.run(extractor_config_path, global_args).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Extract(extract::Args),
    New(new::Args),
    Package(package::Args),
    Info(info::Args),
    /// join the extractor to indexify
    Start(start::Args),
}
