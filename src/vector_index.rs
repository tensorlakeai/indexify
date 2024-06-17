use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::future::join_all;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::Index;
use internal_api::ExtractedEmbeddings;
use tracing::info;

use crate::{
    api,
    blob_storage::ContentReader,
    coordinator_client::CoordinatorClient,
    extractor_router::ExtractorRouter,
    metrics::{vector_storage::Metrics, Timer},
    vectordbs::{CreateIndexParams, Filter, IndexDistance, SearchResult, VectorChunk, VectorDBTS},
};

pub struct VectorIndexManager {
    vector_db: VectorDBTS,
    extractor_router: ExtractorRouter,
    content_reader: Arc<ContentReader>,
    metrics: Metrics,
}

impl fmt::Debug for VectorIndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIndexManager").finish()
    }
}

pub struct ScoredText {
    pub text: String,
    pub content_id: String,
    pub mime_type: String,
    pub labels: HashMap<String, serde_json::Value>,
    pub confidence_score: f32,
    pub root_content_metadata: Option<internal_api::ContentMetadata>,
    pub content_metadata: internal_api::ContentMetadata,
}

impl VectorIndexManager {
    pub fn new(coordinator_client: Arc<CoordinatorClient>, vector_db: VectorDBTS) -> Result<Self> {
        let extractor_router = ExtractorRouter::new(coordinator_client.clone())?;
        let content_reader = Arc::new(ContentReader::new(coordinator_client.config.clone()));
        Ok(Self {
            vector_db,
            extractor_router,
            content_reader,
            metrics: Metrics::new(),
        })
    }

    pub async fn create_index(
        &self,
        index_name: &str,
        schema: internal_api::EmbeddingSchema,
    ) -> Result<String> {
        let create_index_params = CreateIndexParams {
            vectordb_index_name: index_name.to_string(),
            vector_dim: schema.dim as u64,
            distance: IndexDistance::from_str(schema.distance.as_str())?,
            unique_params: None,
        };
        info!("Creating index: {:?}", create_index_params);
        self.vector_db.create_index(create_index_params).await?;
        Ok(index_name.to_string())
    }

    pub async fn drop_index(&self, index_name: &str) -> Result<()> {
        self.vector_db.drop_index(index_name).await
    }

    pub async fn add_embedding(
        &self,
        vector_index_name: &str,
        embeddings: Vec<ExtractedEmbeddings>,
    ) -> Result<()> {
        let _timer = Timer::start(&self.metrics.vector_upsert);
        let mut vector_chunks = Vec::new();
        embeddings.iter().for_each(|embedding| {
            let vector_chunk = VectorChunk::new(
                embedding.content_id.clone(),
                embedding.embedding.clone(),
                embedding.metadata.clone(),
                embedding.root_content_metadata.clone(),
                &embedding.content_metadata,
            );
            vector_chunks.push(vector_chunk);
        });
        self.vector_db
            .add_embedding(vector_index_name, vector_chunks)
            .await?;
        Ok(())
    }

    pub async fn remove_embedding(&self, vector_index_name: &str, content_id: &str) -> Result<()> {
        let _timer = Timer::start(&self.metrics.vector_delete);
        self.vector_db
            .remove_embedding(vector_index_name, content_id)
            .await?;
        Ok(())
    }

    pub async fn get_points(
        &self,
        index: &str,
        content_ids: Vec<String>,
    ) -> Result<Vec<VectorChunk>> {
        self.vector_db.get_points(index, content_ids).await
    }

    pub async fn update_metadata(
        &self,
        index: &str,
        content_id: String,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Result<()> {
        let _timer = Timer::start(&self.metrics.vector_metadata_update);
        self.vector_db
            .update_metadata(index, content_id, metadata)
            .await
    }

    pub async fn search(
        &self,
        index: Index,
        query: &str,
        k: usize,
        filters: Vec<String>,
        include_content: bool,
    ) -> Result<Vec<ScoredText>> {
        let _timer = Timer::start(&self.metrics.vector_search);

        let content = api::Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: query.as_bytes().into(),
            features: vec![],
            labels: HashMap::new(),
        };
        info!("Extracting searching from index {:?}", index);
        let filters = filters
            .into_iter()
            .map(|f| Filter::from_str(f.as_str()))
            .collect::<Result<Vec<Filter>>>()?;

        let embedding = self.generate_embedding(&index.extractor, content).await?;

        let search_result = self
            .search_vector_db(index.table_name, embedding.values, k as u64, filters)
            .await?;

        let mut content_byte_map = HashMap::new();
        if include_content {
            content_byte_map = self.retrieve_content_blob(&search_result).await?;
        }

        let mut index_search_results = Vec::new();
        for result in search_result {
            let content = content_byte_map.get(result.content_id.as_str());
            // Only skip specified to include content but content is not found.
            if content.is_none() && include_content {
                continue;
            }
            let text =
                if result.content_metadata.content_type.starts_with("text/") && content.is_some() {
                    let content = content.unwrap().clone();
                    String::from_utf8(content.to_vec()).unwrap()
                } else {
                    String::from("")
                };
            let mut labels = HashMap::new();
            labels.extend(result.content_metadata.labels.clone());
            for (k, v) in result.metadata {
                labels.insert(k, v);
            }
            let search_result = ScoredText {
                text,
                content_id: result.content_id.clone(),
                mime_type: result.content_metadata.content_type.clone(),
                labels,
                confidence_score: result.confidence_score,
                root_content_metadata: result.root_content_metadata,
                content_metadata: result.content_metadata.clone(),
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }

    async fn generate_embedding(
        &self,
        extractor: &str,
        content: api::Content,
    ) -> Result<internal_api::Embedding> {
        let _timer = Timer::start(&self.metrics.vector_search_extract_embeddings);
        let feature = self
            .extractor_router
            .extract_content(extractor, content, None)
            .await
            .map_err(|e| anyhow!("unable to extract embedding: {}", e.to_string()))?
            .features
            .pop()
            .ok_or(anyhow!("No embeddings were extracted"))?;

        let embedding =
            serde_json::from_value(feature.data.clone()).map_err(|e| anyhow!(e.to_string()))?;

        Ok(embedding)
    }

    async fn search_vector_db(
        &self,
        index: String,
        embedding: Vec<f32>,
        k: u64,
        filters: Vec<Filter>,
    ) -> Result<Vec<SearchResult>> {
        let _timer = Timer::start(&self.metrics.vector_search_db);
        let search_result = self.vector_db.search(index, embedding, k, filters).await?;
        Ok(search_result)
    }

    async fn retrieve_content_blob(
        &self,
        search_results: &Vec<SearchResult>,
    ) -> Result<HashMap<String, Bytes>> {
        let _timer = Timer::start(&self.metrics.vector_search_retrieve_blob);
        let mut content_bytes_list = Vec::new();
        let mut content_ids = Vec::new();

        for search_result in search_results {
            let content = self
                .content_reader
                .bytes(&search_result.content_metadata.storage_url);
            content_bytes_list.push(content);
            content_ids.push(search_result.content_metadata.id.id.clone());
        }

        let bytes = join_all(content_bytes_list).await;
        let mut content_byte_map = HashMap::new();

        for (id, content) in content_ids.iter().zip(bytes) {
            let bytes = content.map_err(|e| {
                let msg = e.to_string();
                anyhow!("unable to read content bytes for id: {id}, {msg}")
            })?;
            content_byte_map.insert(id.clone(), bytes);
        }

        Ok(content_byte_map)
    }
}
