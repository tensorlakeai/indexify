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
use tokio_stream::StreamExt;
use tracing::info;
use walkdir::WalkDir;

use crate::server_config::ExtractorConfig;

#[derive(Template)]
#[template(path = "Dockerfile.extractor", escape = "none")]
struct DockerfileTemplate<'a> {
    image_name: &'a str,
    system_dependencies: &'a str,
    python_dependencies: &'a str,
    additional_pip_flags: &'a str,
    additional_dev_setup: &'a str,
}

pub struct Packager {
    config_path: String,
    config: ExtractorConfig,
    code_dir: PathBuf,
    dev: bool,
}

impl Packager {
    pub fn new(path: String, dev: bool) -> Result<Packager> {
        let path_buf = PathBuf::from(path.clone())
            .canonicalize()
            .map_err(|e| anyhow!(format!("unable to use path {}", e.to_string())))?;
        let code_dir_relative_path = path_buf
            .parent()
            .ok_or(anyhow!("unable to get parent of path"))?;
        info!("packaging extractor in: {:?}", code_dir_relative_path);

        let config = ExtractorConfig::from_path(&path)?;
        Ok(Packager {
            config_path: path,
            config,
            code_dir: code_dir_relative_path.into(),
            dev,
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

        tar.append_path_with_name(self.config_path.clone(), "indexify.yaml")?;

        if self.dev {
            self.add_dev_dependencies(&mut tar)?;
        }

        let uncompressed = tar.into_inner().unwrap();
        let mut c = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        c.write_all(&uncompressed).unwrap();
        let compressed = c.finish().unwrap();

        let build_image_options = BuildImageOptions {
            t: self.config.name.clone(),
            dockerfile: String::from("Dockerfile"),
            version: BuilderVersion::BuilderBuildKit,
            pull: true,
            session: Some(self.config.name.clone()),
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
        let (image_name, additional_pip_flags) = self.generate_base_image_name_for_matching_modify_dependencies()?;

        let system_dependencies = self.config.system_dependencies.join(" ");
        let python_dependencies = self.config.python_dependencies.join(" ");
        let additional_dev_setup = if self.dev {
            "
COPY indexify_extractor_sdk /indexify/indexify_extractor_sdk

COPY setup.py /indexify/setup.py

RUN python3 setup.py install
"
        } else {
            "
RUN pip3 install --no-input indexify_extractor_sdk
"
        };
        let tmpl = DockerfileTemplate {
            image_name: &image_name,
            system_dependencies: &system_dependencies,
            python_dependencies: &python_dependencies,
            additional_pip_flags,
            additional_dev_setup,
        };
        tmpl.render().map_err(|e| anyhow!(e.to_string()))
    }

    // TODO move image strings to config
    pub fn generate_base_image_name_for_matching_modify_dependencies(&self) -> Result<(String, &'static str), Error> {
        let pytorch = self.config.python_dependencies.iter().position(|x| x.contains("torch"));
        let gpu = self.config.gpu;

        let mut pytorch_version = None;
        match pytorch {
            Some(pos) => {
                let dep_string = &self.config.python_dependencies[pos];
                if dep_string.contains("==") {
                    pytorch_version = Option::Some(dep_string.split("==").collect::<Vec<_>>()[1]);
                } else {
                    pytorch_version = Option::Some("latest");
                }
            },
            None => {}
        }

        let mut additional_pip_flags = "";
        let image_name:String;

        match (pytorch_version, gpu) {
            (Some(version), true) => {
                if version == "latest" {
                    return Err(anyhow!("Please make sure to specify pytorch version and cuda version"));
                } else {
                    image_name = "ubuntu:22.04".to_string();
                }
            },
            (Some(_version), false) => {
                image_name = "tensorlake/indexify-extractor-base".to_string();
                additional_pip_flags = "--extra-index-url https://download.pytorch.org/whl/cpu";
            },
            (None, _) => { image_name = "tensorlake/indexify-extractor-base".to_string(); },
        }

        info!("Selecting image_name `{}`", image_name);

        Ok((image_name, additional_pip_flags))
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
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: false,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        let docker_file = packager.create_docker_file().unwrap();

        let expected_dockerfile = r#"FROM --platform=linux/amd64 tensorlake/indexify-extractor-base

RUN apt-get update -y && \
    apt-get install -y lsb-release apt-transport-https python3 python3-dev python3-pip && \
    echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input  numpy pandas

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_for_gpu() {
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: true,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        let docker_file = packager.create_docker_file().unwrap();

        let expected_dockerfile = r#"FROM --platform=linux/amd64 tensorlake/indexify-extractor-base

RUN apt-get update -y && \
    apt-get install -y lsb-release apt-transport-https python3 python3-dev python3-pip && \
    echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input  numpy pandas

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_for_gpu_with_pytorch() {
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: true,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string(), "torch".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        assert!(packager.create_docker_file().is_err());
    }

    #[test]
    fn test_create_docker_file_no_gpu_with_pytorch() {
        // cuda version was not specified - so use latest image.
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: false,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string(), "torch".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        let docker_file = packager.create_docker_file().unwrap();

        let expected_dockerfile = r#"FROM --platform=linux/amd64 tensorlake/indexify-extractor-base

RUN apt-get update -y && \
    apt-get install -y lsb-release apt-transport-https python3 python3-dev python3-pip && \
    echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input --index-url https://download.pytorch.org/whl/cpu numpy pandas torch

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_no_gpu_with_pytorch_version() {
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: false,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string(), "torch==2.1.2".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        let docker_file = packager.create_docker_file().unwrap();

        let expected_dockerfile = r#"FROM --platform=linux/amd64 tensorlake/indexify-extractor-base

RUN apt-get update -y && \
    apt-get install -y lsb-release apt-transport-https python3 python3-dev python3-pip && \
    echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input --index-url https://download.pytorch.org/whl/cpu numpy pandas torch==2.1.2

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }

    #[test]
    fn test_create_docker_file_for_gpu_with_pytorch_version() {
        let config = ExtractorConfig {
            name: "test".to_string(),
            module: "foo.py/ModuleName".to_string(),
            description: "test_description".into(),
            version: "0.1.0".to_string(),
            gpu: true,
            python_dependencies: vec!["numpy".to_string(), "pandas".to_string(), "torch==2.1.2".to_string()],
            system_dependencies: vec!["libpq-dev".to_string(), "libssl-dev".to_string()],
        };
        let packager = Packager {
            config_path: "test".to_string(),
            config,
            code_dir: PathBuf::from("/tmp"),
            dev: false,
        };
        let docker_file = packager.create_docker_file().unwrap();

        let expected_dockerfile = r#"FROM --platform=linux/amd64 ubuntu:22.04

RUN apt-get update -y && \
    apt-get install -y lsb-release apt-transport-https python3 python3-dev python3-pip && \
    echo "deb [trusted=yes] https://cf-repo.diptanu-6d5.workers.dev/repo $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/indexify-repo.list && \
    apt-get update -y && \
    apt-get install -y indexify && \
    apt-get install -y  libpq-dev libssl-dev && \
    apt-get -y clean

RUN pip3 install --no-input  numpy pandas torch==2.1.2

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }
}
