use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Error, Result};
use askama::Template;
use bollard::{
    image::{BuildImageOptions, BuilderVersion},
    service::BuildInfoAux,
    Docker,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tracing::info;
use walkdir::WalkDir;

use crate::extractor::ExtractorSchema;

#[derive(Serialize, Deserialize)]
struct ExtractorConfig {
    path: String,
}

#[derive(Template)]
#[template(path = "Dockerfile.extractor", escape = "none")]
struct DockerfileTemplate<'a> {
    system_dependencies: &'a str,
    python_dependencies: &'a str,
    additional_pip_flags: &'a str,
    dev: bool,
}

pub struct Packager {
    extractor_path: String,
    extractor_class_name: String,
    extractor_description: ExtractorSchema,
    code_dir: PathBuf,
    dev: bool,
    gpu: bool,
}

impl Packager {
    pub fn new(
        extractor_description: ExtractorSchema,
        dev: bool,
        extractor_path: String,
        gpu: bool,
    ) -> Result<Packager> {
        let extractor_paths = extractor_path.split(':').collect_vec();
        if extractor_paths.len() != 2 {
            return Err(anyhow!(
                "specify extractor path as </path/to/extractor>:<module_name>"
            ));
        }
        let extractor_path = extractor_paths[0].to_string();
        let extractor_class_name = extractor_paths[1].to_string();
        let path_buf = PathBuf::from(extractor_path.clone())
            .canonicalize()
            .map_err(|e| anyhow!(format!("unable to use path {}", e.to_string())))?;
        let code_dir_relative_path = path_buf
            .parent()
            .ok_or(anyhow!("unable to get parent of path"))?;
        info!("packaging extractor in: {:?}", code_dir_relative_path);
        Ok(Packager {
            extractor_path,
            extractor_class_name,
            extractor_description,
            code_dir: code_dir_relative_path.into(),
            dev,
            gpu,
        })
    }

    pub async fn package(&self, verbose: bool) -> Result<()> {
        let docker_file = self.create_docker_file()?;
        info!("{}", docker_file);
        let mut header = tar::Header::new_gnu();
        header.set_path("Dockerfile").unwrap();
        header.set_size(docker_file.len() as u64);
        header.set_mode(0o755);
        header.set_cksum();
        let mut tar = tar::Builder::new(Vec::new());
        tar.append(&header, docker_file.as_bytes()).unwrap();

        self.add_directory_to_tar(&mut tar, &self.code_dir)?;

        // Add the indexify.yaml file to the tar
        let file_name = Path::new(&self.extractor_path)
            .file_name()
            .ok_or(anyhow!(
                "unable to get file name from path {}",
                self.extractor_path
            ))?
            .to_str()
            .ok_or(anyhow!(
                "unable to convert file name to string {}",
                self.extractor_path
            ))?
            .to_string();
        let indexify_extractor_config = ExtractorConfig {
            path: format!("{}:{}", file_name, self.extractor_class_name.clone()),
        };
        let config_data = serde_yaml::to_string(&indexify_extractor_config)
            .map_err(|e| anyhow!(e.to_string()))?;

        let mut indexify_header = tar::Header::new_gnu();
        indexify_header.set_path("indexify.yaml").unwrap();
        indexify_header.set_size(config_data.len() as u64);
        indexify_header.set_mode(0o755);
        indexify_header.set_cksum();
        tar.append(&indexify_header, config_data.as_bytes())
            .unwrap();

        if self.dev {
            self.add_dev_dependencies(&mut tar)?;
        }

        let uncompressed = tar.into_inner().unwrap();
        let mut c = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        c.write_all(&uncompressed).unwrap();
        let compressed = c.finish().unwrap();

        let build_image_options = BuildImageOptions {
            t: self.extractor_description.name.clone(),
            dockerfile: String::from("Dockerfile"),
            version: BuilderVersion::BuilderBuildKit,
            pull: true,
            session: Some(self.extractor_description.name.clone()),
            ..Default::default()
        };

        let docker = Docker::connect_with_local_defaults()
            .map_err(|e| anyhow!("unable to connect to docker {}", e))?;

        let mut image_build_stream =
            docker.build_image(build_image_options, None, Some(compressed.into()));

        while let Some(build_info_result) = image_build_stream.next().await {
            if let Err(err) = build_info_result {
                return Err(anyhow!("unable to connect to docker {}", err));
            }

            let build_info = build_info_result.unwrap();

            if let Some(error) = &build_info.error {
                return Err(anyhow!(error.clone()));
            }

            if let Some(status) = &build_info.stream {
                info!("build message: {}", status);
            }

            // TODO image name is being printed here but somehow missing
            if let Some(BuildInfoAux::BuildKit(status)) = &build_info.aux {
                let messages: Vec<String> = status
                    .logs
                    .clone()
                    .into_iter()
                    .map(|l| String::from_utf8(l.msg).unwrap())
                    .filter(|message| message.trim() != "")
                    .collect();
                for vertex in &status.vertexes {
                    if verbose && !vertex.name.is_empty() {
                        println!("{}", vertex.name);
                    }
                    if !vertex.error.is_empty() {
                        eprintln!("{}", vertex.error);
                        return Err(anyhow!(vertex.error.clone()));
                    }
                }
                if verbose && !messages.is_empty() {
                    println!("{}", messages.join("\n"));
                }
            }
        }

        Ok(())
    }

    fn create_docker_file(&self) -> Result<String, Error> {
        let additional_pip_flags = if self.gpu {
            ""
        } else {
            "--extra-index-url https://download.pytorch.org/whl/cpu"
        };
        let system_dependencies = self.extractor_description.system_dependencies.join(" ");
        let python_dependencies = self.extractor_description.python_dependencies.join(" ");
        let tmpl = DockerfileTemplate {
            system_dependencies: &system_dependencies,
            python_dependencies: &python_dependencies,
            additional_pip_flags,
            dev: self.dev,
        };
        tmpl.render().map_err(|e| anyhow!(e.to_string()))
    }

    fn add_directory_to_tar(
        &self,
        tar_builder: &mut tar::Builder<Vec<u8>>,
        dir_path: &PathBuf,
    ) -> Result<()> {
        for entry in WalkDir::new(dir_path) {
            let entry = entry?;
            let src_path = entry.path();
            let metadata = entry.metadata()?;
            let path_name = src_path.strip_prefix(dir_path)?;
            let path_name = Path::new(".").join(path_name);

            if metadata.is_dir() {
                tar_builder.append_dir_all(path_name, src_path)?;
            } else if metadata.is_file() {
                tar_builder.append_path_with_name(src_path, path_name)?;
            }
        }
        Ok(())
    }

    fn add_dev_dependencies(&self, tar_builder: &mut tar::Builder<Vec<u8>>) -> Result<()> {
        let dirs_to_add = vec!["setup.py", "indexify_extractor_sdk"];
        for dir in dirs_to_add {
            self.add_directory_to_tar(tar_builder, &dir.into())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_docker_file() {
        let config = ExtractorSchema {
            name: "test".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
            ..Default::default()
        };
        let packager = Packager {
            extractor_path: "foo.py".to_string(),
            extractor_class_name: "ModuleName".to_string(),
            extractor_description: config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
            gpu: false,
        };
        let docker_file = packager.create_docker_file().unwrap();
        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get -y install lsb-release ca-certificates

RUN update-ca-certificates

RUN echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y lsb-release libssl-dev apt-transport-https python3.11 python3.11-dev python3.11-pip && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input --extra-index-url https://download.pytorch.org/whl/cpu numpy pandas

WORKDIR /indexify

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk --extra-index-url https://download.pytorch.org/whl/cpu


ENTRYPOINT [ "indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_for_gpu() {
        let config = ExtractorSchema {
            name: "test".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
            ..Default::default()
        };
        let packager = Packager {
            extractor_class_name: "ModuleName".to_string(),
            extractor_path: "foo.py".to_string(),
            extractor_description: config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
            gpu: true,
        };
        let docker_file = packager.create_docker_file().unwrap();
        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get -y install lsb-release ca-certificates

RUN update-ca-certificates

RUN echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y lsb-release libssl-dev apt-transport-https python3.11 python3.11-dev python3.11-pip && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input  numpy pandas

WORKDIR /indexify

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk 


ENTRYPOINT [ "indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_no_gpu_with_pytorch() {
        // cuda version was not specified - so use latest image.
        let config = ExtractorSchema {
            name: "test".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            python_dependencies: vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "torch".to_string(),
            ],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
            ..Default::default()
        };
        let packager = Packager {
            extractor_class_name: "ModuleName".to_string(),
            extractor_path: "foo.py".to_string(),
            extractor_description: config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
            gpu: false,
        };
        let docker_file = packager.create_docker_file().unwrap();
        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get -y install lsb-release ca-certificates

RUN update-ca-certificates

RUN echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y lsb-release libssl-dev apt-transport-https python3.11 python3.11-dev python3.11-pip && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input --extra-index-url https://download.pytorch.org/whl/cpu numpy pandas torch

WORKDIR /indexify

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk --extra-index-url https://download.pytorch.org/whl/cpu


ENTRYPOINT [ "indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_no_gpu_with_pytorch_version() {
        let config = ExtractorSchema {
            name: "test".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            python_dependencies: vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "torch==2.1.2".to_string(),
            ],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
            ..Default::default()
        };
        let packager = Packager {
            extractor_class_name: "ModuleName".to_string(),
            extractor_path: "foo.py".to_string(),
            extractor_description: config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
            gpu: false,
        };
        let docker_file = packager.create_docker_file().unwrap();
        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get -y install lsb-release ca-certificates

RUN update-ca-certificates

RUN echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y lsb-release libssl-dev apt-transport-https python3.11 python3.11-dev python3.11-pip && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input --extra-index-url https://download.pytorch.org/whl/cpu numpy pandas torch==2.1.2

WORKDIR /indexify

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk --extra-index-url https://download.pytorch.org/whl/cpu


ENTRYPOINT [ "indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_for_gpu_with_pytorch_version() {
        let config = ExtractorSchema {
            name: "test".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            python_dependencies: vec![
                "numpy".to_string(),
                "pandas".to_string(),
                "torch==2.1.2".to_string(),
            ],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
            ..Default::default()
        };
        let packager = Packager {
            extractor_class_name: "ModuleName".to_string(),
            extractor_path: "foo.py".to_string(),
            extractor_description: config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
            gpu: true,
        };
        let docker_file = packager.create_docker_file().unwrap();
        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get -y install lsb-release ca-certificates

RUN update-ca-certificates

RUN echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y lsb-release libssl-dev apt-transport-https python3.11 python3.11-dev python3.11-pip && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input  numpy pandas torch==2.1.2

WORKDIR /indexify

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk 


ENTRYPOINT [ "indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }
}
