use clap::Args as ClapArgs;

use super::GlobalArgs;
use crate::server_config::ServerConfig;

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    config_path: String,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { config_path } = self;

        println!("Initializing config file at: {}", &config_path);

        // Initialize the error message earlier so that the String can be moved to
        // ServerConfig
        let error_message = format!("failed to generate config file at: {}", &config_path);
        ServerConfig::generate(config_path).expect(&error_message);
    }
}
