use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::error;

use crate::{
    index::IndexManager,
    persistence::{ExtractorType, Repository},
};

pub struct ExtractorRunner {
    repository: Arc<Repository>,
    index_manager: Arc<IndexManager>,
}

impl ExtractorRunner {
    pub fn new(repository: Arc<Repository>, index_manager: Arc<IndexManager>) -> Self {
        Self {
            repository,
            index_manager,
        }
    }

    pub async fn sync_repo(&self, repository_name: &str) {
        if let Err(e) = self._sync_repo(repository_name).await {
            error!("Error syncing repo: {}", e);
        }
    }

    pub async fn _sync_repo(&self, repository_name: &str) -> Result<(), anyhow::Error> {
        let repository = self
            .repository
            .repository_by_name(repository_name)
            .await
            .map_err(|e| anyhow!(e.to_string()))?;

        for extractor in repository.extractors {
            if let ExtractorType::Embedding { .. } = extractor.extractor_type {
                let index_name: String = format!("{}/{}", repository_name, extractor.name);
                let index = self.index_manager.load(&index_name).await?;
                let content_by_repo = self
                    .repository
                    .content_with_unapplied_extractor(
                        repository_name,
                        &extractor.name,
                        extractor.content_type,
                    )
                    .await?;
                for content in content_by_repo {
                    index.add_to_index(content).await?;
                }
            }
        }
        Ok(())
    }
}
