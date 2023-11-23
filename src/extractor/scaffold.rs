use askama::Template;

#[derive(Template)]
#[template(path = "indexify.yaml", escape = "none")]
struct ExtractorConfigTemplate {
    name: String,
}

#[derive(Template)]
#[template(path = "custom_extractor.py", escape = "none")]
struct ExtractorTemplate {}

pub fn render_extractor_templates(path: &str, name: &str) -> Result<(), anyhow::Error> {
    let config_template = ExtractorConfigTemplate {
        name: name.to_owned(),
    };

    let extractor_template = ExtractorTemplate {};
    let extractor_code = extractor_template.render()?;
    let config = config_template.render()?;
    std::fs::write(format!("{}/indexify.yaml", path), config)?;
    std::fs::write(format!("{}/custom_extractor.py", path), extractor_code)?;
    Ok(())
}
