use askama::Template;
use clap::Args as ClapArgs;

use super::GlobalArgs;

struct TemplateExtractorValue {
    docker_image: String,
    service_name: String,
}

#[derive(Template)]
#[template(path = "docker_compose.yaml.jinja2")]
struct DockerComposeTemplate {
    extractors: Vec<TemplateExtractorValue>,
}

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    path: Option<String>,
    #[arg(long)]
    extractor: Vec<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { path, extractor } = self;
        let path = path.unwrap_or_else(|| "./docker-compose.yaml".to_string());
        println!("Initializing docker compose file at: {}", &path);
        let path = std::path::Path::new(&path);
        let prefix = path.parent().expect("failed to get parent directory");
        std::fs::create_dir_all(prefix).expect("failed to create parent directory");

        let extractors = extractor
            .into_iter()
            .map(|extractor| TemplateExtractorValue {
                service_name: extractor.replace('/', "-"),
                docker_image: extractor,
            })
            .collect();

        let template = DockerComposeTemplate { extractors };
        let compose_file = template.render().unwrap();
        std::fs::write(path, compose_file).expect("failed to write docker-compose file");
    }
}
