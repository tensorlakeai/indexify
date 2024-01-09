use std::{collections::HashMap, fmt::Debug, path::Path, result::Result::Ok, sync::Arc};

use anyhow::{anyhow, Result};
use bollard::{
    container::{Config, CreateContainerOptions, LogOutput, LogsOptions, StartContainerOptions},
    service::{HostConfig, Mount},
    Docker,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

pub mod extractor_runner;
pub mod py_extractors;

use crate::{api::ExtractorDescription, internal_api::Content};

pub mod python_path;
mod scaffold;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

pub trait Extractor: Debug {
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error>;

    fn extract(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<Content>>, anyhow::Error>;
}

pub trait ExtractorCli {
    fn extract(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<Content>>>;
    fn extract_from_data(&self, data: Vec<u8>, mime: &str) -> Result<Vec<Content>>;
    fn info(&self) -> Result<ExtractorDescription>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub embedding_schemas: HashMap<String, EmbeddingSchema>,
    pub input_params: serde_json::Value,
}
pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub embeddings: Vec<f32>,
}

pub async fn run_docker_extractor(
    name: String,
    cache_dir: Option<String>,
    text: Option<String>,
    file_path: Option<String>,
) -> Result<Vec<Content>, anyhow::Error> {
    let docker = Docker::connect_with_socket_defaults().unwrap();
    let options = Some(CreateContainerOptions {
        name: name.clone().replace('/', "."),
        platform: None,
    });
    let mut args = vec!["extractor".to_string(), "extract".to_string()];

    if let Some(text) = text {
        args.push("--text".to_string());
        args.push(text.escape_default().to_string());
    }

    let mut host_config: Option<HostConfig> = None;
    let mut mounts: Vec<Mount> = Vec::new();
    let mut env: Vec<String> = Vec::new();

    if let Some(file_path) = file_path {
        let file_path = Path::new(&file_path).canonicalize().unwrap();
        let file_name = file_path.file_name().unwrap().to_str().unwrap();
        args.push("--file".to_string());
        args.push(format!("./{}", file_name));
        mounts.push(Mount {
            target: Some(format!("/indexify/{}", file_name)),
            source: Some(file_path.display().to_string()),
            typ: Some(bollard::service::MountTypeEnum::BIND),
            ..Default::default()
        });
    }

    if let Some(cache_dir) = cache_dir {
        let cache_dir = Path::new(&cache_dir).canonicalize().unwrap();
        let cache_name = cache_dir.file_name().unwrap().to_str().unwrap();

        let target_path = format!("/indexify/{}", cache_name);

        mounts.push(Mount {
            target: Some(target_path.clone()),
            source: Some(cache_dir.display().to_string()),
            typ: Some(bollard::service::MountTypeEnum::BIND),
            ..Default::default()
        });

        env.push(format!("CACHE_DIR={}", target_path.clone()));
    }

    host_config.replace(HostConfig {
        mounts: Some(mounts),
        ..Default::default()
    });

    let config = Config {
        image: Some(name.clone()),
        cmd: Some(args),
        attach_stderr: Some(true),
        attach_stdout: Some(true),
        host_config,
        env: Some(env),
        ..Default::default()
    };
    let id: String = docker.create_container(options, config).await?.id;

    docker
        .start_container(&id, None::<StartContainerOptions<String>>)
        .await?;
    let options = Some(LogsOptions::<String> {
        follow: true,
        stdout: true,
        stderr: true,

        ..Default::default()
    });

    let mut log_stream = docker.logs(&id, options);
    while let Ok(log) = log_stream.next().await.ok_or(anyhow!("no logs")) {
        match log {
            Ok(log) => match &log {
                LogOutput::StdOut { .. } => print!("{log}"),
                _ => eprintln!("{log}"),
            },
            Err(err) => eprintln!("error from extractor: `{}`", err),
        }
    }
    Ok(vec![])
}

pub fn create_extractor_template(extractor_path: &str, name: &str) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(extractor_path)?;
    scaffold::render_extractor_templates(extractor_path, name)
}
