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

use indexify_internal_api as internal_api;

pub mod python_path;
mod scaffold;

/// EmbeddingSchema describes the embedding output by an extractor
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct EmbeddingSchema {
    /// distance is the distance metric used to compare embeddings
    /// i.e. dot, cosine, euclidean, etc.
    pub distance: String,

    /// dim is the dimensionality of the embedding
    pub dim: usize,
}

pub trait Extractor: Debug {
    /// Returns the extractor schema
    fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error>;

    /// Returns true if the extractor supports the content mime type
    /// Returns false if the extractor does not support the content mime type
    /// Returns an error if the extractor is unable to fetch the ExtractorSchema
    ///
    /// If the extractor input mime types include ["*/*"], then the extractor
    /// supports all mime types. This is useful for debugging, or for
    /// extractors that do not depend on the content mime type. However,
    /// this is not recommended for production use as it can lead to
    /// unexpected behavior.
    ///
    /// Otherwise, the extractor input mime types are checked to see if they
    /// include the content mime type.
    ///
    /// For example, if the extractor input mime types are ["text/plain",
    /// "application/pdf"], and the content mime type is "text/plain", then
    /// the extractor supports the content mime type. Conversely, if the
    /// extractor input mime types are ["text/plain", "application/pdf"], and
    /// the content mime type is "image/png", then the extractor does not
    /// support the content mime type and false is returned.
    ///
    /// This method can be overridden for extractors that need custom logic to
    /// determine if the extractor supports the content mime type.
    fn matches_mime_type(&self, content: &internal_api::Content) -> Result<bool, anyhow::Error> {
        let supported_mimes = self.schemas()?.input_mimes;
        // if the extractor input mime types include ["*/*"], then the extractor
        // supports all mime types.
        if supported_mimes.contains(&"*/*".to_string()) {
            return Ok(true);
        }

        // otherwise, check if the extractor supports the content mime type
        let content_mime = content.mime.clone();
        let is_match = supported_mimes.contains(&content_mime);
        Ok(is_match)
    }

    /// Extracts embeddings from content
    fn extract(
        &self,
        content: Vec<internal_api::Content>,
        input_params: serde_json::Value,
    ) -> Result<Vec<Vec<internal_api::Content>>, anyhow::Error>;
}

#[cfg(test)]
mod test_extractor {
    use super::*;

    #[derive(Debug)]
    enum TestExtractor {
        // "text/plain"
        TextPlain,

        // "*/*"
        Wildcard,
    }

    impl Extractor for TestExtractor {
        fn schemas(&self) -> Result<ExtractorSchema, anyhow::Error> {
            let schemas = match self {
                TestExtractor::TextPlain => ExtractorSchema {
                    input_mimes: vec!["text/plain".to_string()],
                    ..Default::default()
                },
                TestExtractor::Wildcard => ExtractorSchema {
                    input_mimes: vec!["*/*".to_string()],
                    ..Default::default()
                },
            };
            Ok(schemas)
        }

        fn extract(
            &self,
            content: Vec<internal_api::Content>,
            _input_params: serde_json::Value,
        ) -> Result<Vec<Vec<internal_api::Content>>, anyhow::Error> {
            Ok(vec![content])
        }
    }

    #[test]
    fn test_matches_mime_type() {
        let mimetype_matcher = |extractor: TestExtractor, content_mimetypes: Vec<(&str, bool)>| {
            for (content_mime, expected) in content_mimetypes {
                let content = internal_api::Content {
                    mime: content_mime.to_string(),
                    bytes: vec![],
                    feature: None,
                    labels: HashMap::new(),
                };
                let matches = extractor.matches_mime_type(&content).unwrap();
                assert_eq!(
                    matches, expected,
                    "content mime type {} did not match for case {:?}",
                    content_mime, extractor
                );
            }
        };

        mimetype_matcher(
            TestExtractor::TextPlain,
            vec![
                ("text/plain", true),
                ("image/png", false),
                ("application/pdf", false),
            ],
        );
        mimetype_matcher(
            TestExtractor::Wildcard,
            vec![
                ("text/plain", true),
                ("image/png", true),
                ("application/pdf", true),
            ],
        );
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub name: String,
    pub version: String,
    pub description: String,
    pub python_dependencies: Vec<String>,
    pub system_dependencies: Vec<String>,
    pub embedding_schemas: HashMap<String, EmbeddingSchema>,
    pub metadata_schemas: HashMap<String, serde_json::Value>,
    pub input_params: serde_json::Value,
    pub input_mimes: Vec<String>,
}
pub type ExtractorTS = Arc<dyn Extractor + Sync + Send>;

#[derive(Debug, Serialize, Deserialize, PartialEq, FromPyObject)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub embedding: Vec<f32>,
}

pub async fn run_docker_extractor(
    name: String,
    cache_dir: Option<String>,
    text: Option<String>,
    file_path: Option<String>,
) -> Result<Vec<internal_api::Content>, anyhow::Error> {
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
