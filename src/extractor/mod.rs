use std::{collections::HashMap, path::Path, result::Result::Ok, sync::Arc};

use anyhow::anyhow;
use bollard::{
    container::{Config, CreateContainerOptions, LogOutput, LogsOptions, StartContainerOptions},
    service::{HostConfig, Mount},
    Docker,
};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_stream::StreamExt;
use tracing::info;

mod py_extractors;

use py_extractors::{PyContent, PythonExtractor};

use crate::{internal_api::Content, server_config::ExtractorConfig};

pub mod python_path;
mod scaffold;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    pub distance_metric: String,
    pub dim: usize,
}

pub trait Extractor {
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error>;

    fn extract(
        &self,
        content: Vec<Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<Content>>, anyhow::Error>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub embedding_schemas: HashMap<String, EmbeddingSchema>,
    pub input_params: serde_json::Value,
}
pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub text: String,
    pub embeddings: Vec<f32>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct AttributeData {
    pub content_id: String,
    pub text: String,
    pub json: Option<serde_json::Value>,
}

#[tracing::instrument]
pub fn create_extractor(extractor_path: &str, name: &str) -> Result<ExtractorTS, anyhow::Error> {
    let tokens: Vec<&str> = extractor_path.split(':').collect();
    if tokens.len() != 2 {
        return Err(anyhow!("invalid extractor path: {}", extractor_path));
    }
    let module_path = tokens[0];

    let module_path = Path::new(module_path);
    let parent = module_path.parent().ok_or(anyhow!(
        "couldn't find parent dir of module_path: {:?}",
        module_path
    ))?;
    let module_file_name = module_path
        .strip_prefix(parent)?
        .to_str()
        .ok_or(anyhow!("couldn't find model file name: {:?}", module_path))?;
    let module_name = module_file_name.trim_end_matches(".py");

    let class_name = tokens[1].trim();
    let extractor = PythonExtractor::new(module_name, class_name)?;
    info!(
        "extractor created: name: {}, python module: {}, class name: {}",
        name, module_name, class_name
    );
    Ok(Arc::new(extractor))
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
        let cache_name= cache_dir.file_name().unwrap().to_str().unwrap();

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

pub fn run_local_extractor(
    extractor_path: Option<String>,
    text: Option<String>,
    file_path: Option<String>,
) -> Result<Vec<Content>, anyhow::Error> {
    let extractor_path = match extractor_path {
        Some(extractor_path) => Ok::<std::string::String, anyhow::Error>(extractor_path),
        None => {
            info!("no module name provided, looking up indexify.yaml");
            let extractor_config = ExtractorConfig::from_path("indexify.yaml")?;
            Ok(extractor_config.module)
        }
    }?;
    info!("looking up extractor at path: {}", &extractor_path);
    python_path::set_python_path(&extractor_path)?;
    let extractor = create_extractor(&extractor_path, &extractor_path)?;

    match (text, file_path) {
        (Some(text), None) => {
            let content = PyContent::new(text).try_into()?;
            let extracted_content = extractor.extract(vec![content], json!({}))?;
            let content = extracted_content
                .get(0)
                .ok_or(anyhow!("no content was extracted"))?
                .to_owned();
            Ok(content)
        }
        (None, Some(file_path)) => {
            let data = std::fs::read(&file_path).map_err(|e| {
                anyhow!(format!("unable to read file: {}, error: {}", &file_path, e))
            })?;
            let mime_type = mime_guess::from_path(&file_path).first_or_octet_stream();
            let content = PyContent::from_bytes(data, mime_type).try_into()?;
            let extracted_content = extractor.extract(vec![content], json!({}))?;
            let content = extracted_content
                .get(0)
                .ok_or(anyhow!("no content was extracted"))?
                .to_owned();
            Ok(content)
        }
        _ => Err(anyhow!("either text or file path must be provided")),
    }
}

pub fn create_extractor_template(extractor_path: &str, name: &str) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(extractor_path)?;
    scaffold::render_extractor_templates(extractor_path, name)
}
