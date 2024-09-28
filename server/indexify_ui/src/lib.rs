use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../ui/build"]
pub struct Assets;
