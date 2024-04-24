use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};
use futures::future::join_all;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{self, Index};
use internal_api::ExtractedEmbeddings;
use itertools::Itertools;
use tracing::info;

use crate::{
    api,
    blob_storage::ContentReader,
    coordinator_client::CoordinatorClient,
    extractor_router::ExtractorRouter,
    metrics::{vector_storage::Metrics, Timer},
    vectordbs::{CreateIndexParams, Filter, IndexDistance, VectorChunk, VectorDBTS},
};

pub struct VectorIndexManager {
    vector_db: VectorDBTS,
    extractor_router: ExtractorRouter,
    coordinator_client: Arc<CoordinatorClient>,
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
    pub labels: HashMap<String, String>,
    pub confidence_score: f32,
}

impl VectorIndexManager {
    pub fn new(coordinator_client: Arc<CoordinatorClient>, vector_db: VectorDBTS) -> Result<Self> {
        let extractor_router = ExtractorRouter::new(coordinator_client.clone())?;
        let content_reader = Arc::new(ContentReader::new());
        Ok(Self {
            vector_db,
            extractor_router,
            coordinator_client: coordinator_client.clone(),
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
        metadata: serde_json::Value,
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
    ) -> Result<Vec<ScoredText>> {
        let content = api::Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: query.as_bytes().into(),
            features: vec![],
            labels: HashMap::new(),
        };
        info!("Extracting searching from index {:?}", index);
        let feature = self
            .extractor_router
            .extract_content(&index.extractor, content, None)
            .await
            .map_err(|e| anyhow!("unable to extract embedding: {}", e.to_string()))?
            .features
            .pop()
            .ok_or(anyhow!("No embeddings were extracted"))?;
        let filters = filters
            .into_iter()
            .map(|f| Filter::from_str(f.as_str()))
            .collect::<Result<Vec<Filter>>>()?;
        let embedding: internal_api::Embedding =
            serde_json::from_value(feature.data.clone()).map_err(|e| anyhow!(e.to_string()))?;
        let search_result = self
            .vector_db
            .search(index.table_name, embedding.values, k as u64, filters)
            .await?;
        let content_ids = search_result
            .iter()
            .map(|r| r.content_id.clone())
            .collect_vec();
        let req = indexify_coordinator::GetContentMetadataRequest {
            content_list: content_ids.clone(),
        };
        let content_metadata_list = self
            .coordinator_client
            .get()
            .await?
            .get_content_metadata(req)
            .await?
            .into_inner()
            .content_list;
        if content_ids.len() != content_metadata_list.len() {
            return Err(anyhow!(
                "Unable to get metadata for all content ids: {:?}, retreived content ids: {:?}",
                &content_ids,
                content_metadata_list.values(),
            ));
        }

        let mut content_bytes_list = Vec::new();
        let mut content_ids = Vec::new();
        for (id, content_meta) in &content_metadata_list {
            let content = self.content_reader.bytes(&content_meta.storage_url);
            content_bytes_list.push(content);
            content_ids.push(id.clone());
        }
        let bytes = join_all(content_bytes_list).await;
        let mut content_byte_map = HashMap::new();
        for (id, content) in content_ids.iter().zip(bytes) {
            let bytes = content.map_err(|e| {
                anyhow!(
                    "unable to read content bytes for id: {}, {}",
                    id,
                    e.to_string()
                )
            })?;
            content_byte_map.insert(id.clone(), bytes);
        }

        let mut index_search_results = Vec::new();
        for result in search_result {
            let content = content_byte_map.get(result.content_id.as_str());
            if content.is_none() {
                continue;
            }
            let content = content.unwrap().clone();
            let mime_type = content_metadata_list
                .get(&result.content_id)
                .unwrap()
                .mime
                .to_string();
            let labels = content_metadata_list
                .get(&result.content_id)
                .unwrap()
                .labels
                .clone();
            let text = if mime_type.starts_with("text/") {
                String::from_utf8(content.to_vec()).unwrap()
            } else {
                String::from("")
            };
            let search_result = ScoredText {
                text,
                content_id: result.content_id.clone(),
                mime_type,
                labels,
                confidence_score: result.confidence_score,
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}
