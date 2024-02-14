use askama::Template;
use clap::Args as ClapArgs;

use super::GlobalArgs;

#[derive(Template)]
#[template(path = "docker_compose.yaml.jinja2")]
struct DockerComposeTemplate {
    // Add fields here when we templatize this
}

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(short, long)]
    path: Option<String>,
}

impl Args {
    pub async fn run(self, _: GlobalArgs) {
        let Self { path } = self;
        let path = path.unwrap_or_else(|| "./docker-compose.yaml".to_string());
        println!("Initializing docker compose file at: {}", &path);
        let path = std::path::Path::new(&path);
        let prefix = path.parent().expect("failed to get parent directory");
        std::fs::create_dir_all(prefix).expect("failed to create parent directory");

        let template = DockerComposeTemplate {};
        let compose_file = template.render().unwrap();
        std::fs::write(&path, compose_file).expect("failed to write docker-compose file");
    }
}
