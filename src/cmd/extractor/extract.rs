use clap::Args as ClapArgs;

use crate::cmd::GlobalArgs;

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short = 'e', long)]
    extractor_path: Option<String>,

    #[arg(long)]
    cache_dir: Option<String>,

    #[arg(short = 'n', long)]
    name: Option<String>,

    #[arg(short, long)]
    text: Option<String>,

    #[arg(short, long)]
    file: Option<String>,
}

impl Args {
    pub async fn run(self, _extractor_config_path: String, _: GlobalArgs) {
        let Self {
            extractor_path,
            cache_dir,
            name,
            text,
            file,
        } = self;

        if let Some(name) = name {
            let _ = crate::extractor::run_docker_extractor(name, cache_dir, text, file)
                .await
                .expect("failed to run docker image");
        } else {
            let extracted_content =
                crate::extractor::run_local_extractor(extractor_path, text, file)
                    .expect("failed to run local extractor");
            println!(
                "{}",
                serde_json::to_string_pretty(&extracted_content)
                    .expect("unable to serialize extracted content as JSON")
            );
        }
    }
}
