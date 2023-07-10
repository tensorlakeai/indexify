use crate::{
    extractors::{EmbeddingExtractor, Extractor},
    index::IndexManager,
    persistence::{ExtractorType, Repository},
    persistence::{Work, WorkState},
    Executor, ExecutorState,
};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::error;
use tracing::info;

pub struct ExtractorExecutor {
    repository: Arc<Repository>,
    embedding_extractor: Arc<EmbeddingExtractor>,
    node_state: Option<Arc<ExecutorState>>,
}

impl ExtractorExecutor {
    pub fn new(
        repository: Arc<Repository>,
        index_manager: Arc<IndexManager>,
        node_state: Option<Arc<ExecutorState>>,
    ) -> Self {
        let embedding_extractor = Arc::new(EmbeddingExtractor::new(index_manager));
        Self {
            repository,
            embedding_extractor,
            node_state,
        }
    }

    pub async fn register_local_worker(&self) -> Result<(), anyhow::Error> {
        let _ = self
            .node_state
            .as_ref()
            .unwrap()
            .record_node(Executor {
                id: "local".to_string(),
            })
            .await;
        Ok(())
    }

    pub async fn get_work_local(&self) -> Result<Vec<Work>, anyhow::Error> {
        let node_state = self.node_state.as_ref().unwrap();
        let worker_id = "local";
        info!("getting work for worker: {}", worker_id);
        node_state.get_work_for_worker(worker_id).await
    }

    pub async fn update_work_status(&self, work: &Work) -> Result<(), anyhow::Error> {
        self.node_state
            .as_ref()
            .unwrap()
            .update_work_state(vec![work.to_owned()], "local")
            .await
    }

    pub async fn sync_repo(&self, _repository_name: &str) -> Result<u64, anyhow::Error> {
        let _ = self.register_local_worker().await;
        let _ = self.node_state.as_ref().unwrap().distribute_work().await;
        let work_list = self.get_work_local().await;
        if let Err(err) = &work_list {
            error!("unable to get work: {:?}", err);
            return Err(anyhow!("unable to get work: {:?}", err));
        }
        if let Err(err) = self
            .perform_work(work_list.as_ref().unwrap().to_vec())
            .await
        {
            error!("unable perform work: {:?}", err);
            return Err(anyhow!("unable perform work: {:?}", err));
        }
        let mut num_work_perfomed = 0;
        for mut work in work_list.unwrap() {
            work.work_state = WorkState::Completed;
            info!(
                "updating work status: content_id: {}, extractor: {}",
                &work.content_id, &work.extractor
            );
            if let Err(err) = self.update_work_status(&work).await {
                error!("unable update work status: {:?}", err);
                return Err(anyhow!("unable update work status: {:?}", err));
            }
            num_work_perfomed += 1;
        }

        Ok(num_work_perfomed)
    }

    pub async fn perform_work(&self, work_list: Vec<Work>) -> Result<(), anyhow::Error> {
        for work in work_list {
            info!("performing work: {}", &work.id);
            let extractor = self
                .repository
                .get_extractor(&work.repository_id, &work.extractor)
                .await?;
            let content = self
                .repository
                .content_from_repo(&work.content_id, &work.repository_id)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            if let ExtractorType::Embedding { .. } = extractor.extractor_type {
                let index_name: String = format!("{}/{}", work.repository_id, extractor.name);
                info!(
                    "extracting embedding - extractor: {}, index: {}, content id: {}",
                    &extractor.name, &index_name, &content.id
                );
                self.embedding_extractor
                    .extract_and_store(content, &index_name)
                    .await?;
            }
        }
        Ok(())
    }
}
