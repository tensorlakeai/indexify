use std::sync::Arc;

use anyhow::anyhow;
use clap::Args as ClapArgs;
use serde_json::json;
use tracing_unwrap::ResultExt;

use crate::{
    cmd::GlobalArgs,
    extractor::{py_extractors::PythonExtractor, python_path, ExtractorTS},
    internal_api::Content,
};

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

        if extractor_path.is_none() && name.is_none() {
            panic!("either extractor path or name must be provided");
        }

        if let Some(name) = name {
            let _ = crate::extractor::run_docker_extractor(name, cache_dir, text, file)
                .await
                .unwrap_err_or_log();
        } else if let Some(extractor_path) = extractor_path {
            python_path::set_python_path(&extractor_path).unwrap();
            let extractor =
                PythonExtractor::new_from_extractor_path(&extractor_path).unwrap_or_log();
            let extractor: ExtractorTS = Arc::new(extractor);
            let py_content = match (text, file) {
                (Some(text), None) => Ok(Content {
                    content_type: "text/plain".to_string(),
                    source: text.as_bytes().to_vec(),
                    feature: None,
                }),
                (None, Some(file_path)) => {
                    let data = std::fs::read(&file_path)
                        .map_err(|e| {
                            anyhow!(format!("unable to read file: {}, error: {}", &file_path, e))
                        })
                        .unwrap_or_log();
                    let mime_type = mime_guess::from_path(&file_path).first_or_octet_stream();
                    Ok(Content {
                        content_type: mime_type.to_string(),
                        source: data,
                        feature: None,
                    })
                }
                _ => Err(anyhow!("either text or file path must be provided")),
            }
            .unwrap_or_log();
            let extracted_content = extractor
                .extract(vec![py_content], json!({}))
                .unwrap_or_log();
            println!(
                "{}",
                serde_json::to_string_pretty(&extracted_content).unwrap_or_log()
            );
        }
    }
}
