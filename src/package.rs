use crate::ExtractorPackageConfig;
use anyhow::Result;

pub struct Packager {
    config: ExtractorPackageConfig,
}

impl Packager {
    pub fn from(config: ExtractorPackageConfig) -> Result<Packager> {
        Ok(Packager { config })
    }

    pub fn package(&self) -> Result<()> {
        Ok(())
    }
}
