use askama::Template;

#[derive(Template)]
#[template(path = "custom_extractor.py", escape = "none")]
struct ExtractorTemplate {}

pub fn render_extractor_templates(path: &str, _name: &str) -> Result<(), anyhow::Error> {
    let extractor_template = ExtractorTemplate {};
    let extractor_code = extractor_template.render()?;
    std::fs::write(format!("{}/custom_extractor.py", path), extractor_code)?;
    Ok(())
}
