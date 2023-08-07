use anyhow::Result;
use dashmap::DashMap;

use crate::{
    extractors::{create_extractor, ExtractedEmbeddings, ExtractorTS},
    index::IndexError,
    persistence::{Chunk, ExtractorConfig, ExtractorType, Repository, Text},
    vectordbs::{CreateIndexParams, VectorChunk, VectorDBTS},
    ServerConfig,
};
use std::sync::Arc;
use tracing::error;

pub struct VectorIndexManager {
    repository: Arc<Repository>,
    vector_db: VectorDBTS,

    embedding_extractors: DashMap<String, ExtractorTS>,
}

impl VectorIndexManager {
    pub fn new(
        server_config: Arc<ServerConfig>,
        repository: Arc<Repository>,
        vector_db: VectorDBTS,
    ) -> Self {
        let extractor_index = DashMap::new();
        for extractor in server_config.extractors.iter() {
            let extractor = create_extractor(extractor.clone()).unwrap();
            if let ExtractorType::Embedding { .. } = extractor.info().unwrap().extractor_type {
                extractor_index.insert(extractor.info().unwrap().name, extractor);
            }
        }
        Self {
            repository,
            vector_db,
            embedding_extractors: extractor_index,
        }
    }

    pub async fn create_index(
        &self,
        repository: &str,
        index_name: &str,
        extractor_config: ExtractorConfig,
    ) -> Result<()> {
        let mut index_params: Option<CreateIndexParams> = None;
        if let ExtractorType::Embedding { dim, distance } = &extractor_config.extractor_type {
            let vector_index_name = format!("{}/{}", repository, index_name);
            let create_index_params = CreateIndexParams {
                vectordb_index_name: vector_index_name,
                vector_dim: *dim as u64,
                distance: distance.clone(),
                unique_params: None,
            };
            index_params.replace(create_index_params);
        }
        self.repository
            .create_vector_index(
                repository,
                &extractor_config.name,
                index_name,
                index_params.unwrap(),
                self.vector_db.clone(),
            )
            .await
            .unwrap();
        Ok(())
    }

    pub async fn add_embedding(
        &self,
        _repository: &str,
        index: &str,
        embeddings: Vec<ExtractedEmbeddings>,
    ) -> Result<()> {
        let index_info = self.repository.get_index(index, _repository).await?;
        let vector_index_name = index_info.vector_index_name.clone().unwrap();
        let mut vector_chunks = Vec::new();
        let mut chunks = Vec::new();
        embeddings.iter().for_each(|embedding| {
            let chunk = Chunk::new(embedding.text.clone(), embedding.content_id.clone());
            let vector_chunk = VectorChunk::new(
                chunk.chunk_id.clone(),
                chunk.text.clone(),
                embedding.embeddings.clone(),
            );
            chunks.push(chunk);
            vector_chunks.push(vector_chunk);
        });
        self.repository.create_chunks(chunks, index).await?;
        self.vector_db
            .add_embedding(&vector_index_name, vector_chunks)
            .await?;
        Ok(())
    }

    pub async fn search(
        &self,
        repository: &str,
        index: &str,
        query: &str,
        k: usize,
    ) -> Result<Vec<Text>, IndexError> {
        let index_info = self.repository.get_index(index, repository).await?;
        let vector_index_name = index_info.vector_index_name.clone().unwrap();
        let embeddings = self
            .embedding_extractors
            .get(index_info.extractor_name.as_str())
            .unwrap()
            .value()
            .extract_embedding_query(query)
            .unwrap();
        let results = self
            .vector_db
            .search(vector_index_name, embeddings, k as u64)
            .await?;
        let mut index_search_results = Vec::new();
        for result in results {
            let chunk = self.repository.chunk_with_id(&result.chunk_id).await;
            if chunk.as_ref().is_err() {
                error!("Chunk with id {} not found", result.chunk_id);
                continue;
            }
            let search_result = Text {
                id: chunk.as_ref().unwrap().content_id.clone(),
                text: chunk.as_ref().unwrap().text.clone(),
                metadata: chunk.as_ref().unwrap().metadata.clone(),
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::env;

    use crate::data_repository_manager::DataRepositoryManager;
    use crate::persistence::{self, DataRepository, ExtractorBinding, Text};
    use crate::test_util;
    use crate::test_util::db_utils::{
        create_index_manager, DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_index_search_basic() {
        env::set_var("RUST_LOG", "debug");
        let db = test_util::db_utils::create_db().await.unwrap();
        let (index_manager, extractor_executor, coordinator) =
            create_index_manager(db.clone()).await;
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), index_manager.clone());
        let _ = repository_manager
            .create(&DataRepository {
                name: DEFAULT_TEST_REPOSITORY.into(),
                data_connectors: vec![],
                metadata: HashMap::new(),
                extractor_bindings: vec![ExtractorBinding {
                    extractor_name: DEFAULT_TEST_EXTRACTOR.into(),
                    index_name: DEFAULT_TEST_EXTRACTOR.into(),
                    filter: persistence::ExtractorFilter::ContentType {
                        content_type: persistence::ContentType::Text,
                    },
                    input_params: serde_json::json!({}),
                }],
            })
            .await;

        repository_manager
            .add_texts(
                DEFAULT_TEST_REPOSITORY,
                vec![
                    Text::from_text(DEFAULT_TEST_REPOSITORY, "hello world", None, HashMap::new()),
                    Text::from_text(DEFAULT_TEST_REPOSITORY, "hello pipe", None, HashMap::new()),
                    Text::from_text(DEFAULT_TEST_REPOSITORY, "nba", None, HashMap::new()),
                ],
                None,
            )
            .await
            .unwrap();
        repository_manager
            .add_texts(
                DEFAULT_TEST_REPOSITORY,
                vec![Text::from_text(
                    DEFAULT_TEST_REPOSITORY,
                    "hello world",
                    None,
                    HashMap::new(),
                )],
                None,
            )
            .await
            .unwrap();

        coordinator.process_and_distribute_work().await.unwrap();
        let executor_id = extractor_executor.get_executor_info().id;
        let work_list = coordinator.get_work_for_worker(&executor_id).await.unwrap();

        extractor_executor.sync_repo_test(work_list).await.unwrap();
        let result = index_manager
            .search(DEFAULT_TEST_REPOSITORY, DEFAULT_TEST_EXTRACTOR, "pipe", 1)
            .await
            .unwrap();
        assert_eq!(1, result.len())
    }
}
