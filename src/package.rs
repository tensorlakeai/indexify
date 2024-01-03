use std::{
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
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
    system_dependencies: &'a str,
    python_dependencies: &'a str,
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

        let docker = Docker::connect_with_local_defaults().map_err(|e| anyhow!("unable to connect to docker {}", e))?;

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
                        print!("{}", vertex.name);
                    }
                    if !vertex.error.is_empty() {
                        eprint!("{}", vertex.error);
                        return Err(anyhow!(vertex.error.clone()));
                    }
                }
                if verbose && !messages.is_empty() {
                    print!("{}", messages.join("\n"));
                }
            }
        }

        Ok(())
    }

    fn create_docker_file(&self) -> Result<String> {
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
            system_dependencies: &system_dependencies,
            python_dependencies: &python_dependencies,
            additional_dev_setup,
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

        let expected_dockerfile = r#"FROM tensorlake/indexify-extractor-base

RUN apt-get install -y  libpq-dev libssl-dev

RUN pip3 install --no-input numpy pandas

COPY . /indexify/

COPY indexify.yaml indexify.yaml


RUN pip3 install --no-input indexify_extractor_sdk


ENTRYPOINT [ "/indexify/indexify" ]"#;
        assert_eq!(docker_file, expected_dockerfile);
    }
}
