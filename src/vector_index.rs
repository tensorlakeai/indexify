use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};
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
    vectordbs::{CreateIndexParams, IndexDistance, VectorChunk, VectorDBTS},
};

pub struct VectorIndexManager {
    vector_db: VectorDBTS,
    extractor_router: ExtractorRouter,
    coordinator_client: Arc<CoordinatorClient>,
    content_reader: Arc<ContentReader>,
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

    pub async fn add_embedding(
        &self,
        vector_index_name: &str,
        embeddings: Vec<ExtractedEmbeddings>,
    ) -> Result<()> {
        let mut vector_chunks = Vec::new();
        embeddings.iter().for_each(|embedding| {
            let vector_chunk =
                VectorChunk::new(embedding.content_id.clone(), embedding.embedding.clone());
            vector_chunks.push(vector_chunk);
        });
        self.vector_db
            .add_embedding(vector_index_name, vector_chunks)
            .await?;
        Ok(())
    }

    pub async fn search(&self, index: Index, query: &str, k: usize) -> Result<Vec<ScoredText>> {
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
        let embedding: internal_api::Embedding =
            serde_json::from_value(feature.data.clone()).map_err(|e| anyhow!(e.to_string()))?;
        let search_result = self
            .vector_db
            .search(index.table_name, embedding.values, k as u64)
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
            .into_inner();
        if content_ids.len() != content_metadata_list.content_list.len() {
            return Err(anyhow!(
                "Unable to get metadata for all content ids: {:?}, retreived content ids: {:?}",
                &content_ids,
                content_metadata_list.content_list,
            ));
        }

        let mut content_byte_map = HashMap::new();
        let mut content_mime_map = HashMap::new();
        for content_meta in content_metadata_list.content_list {
            let content = self
                .content_reader
                .bytes(&content_meta.storage_url)
                .await
                .map_err(|e| anyhow!("unable to get content: {}", e.to_string()))?;
            content_byte_map.insert(content_meta.id.clone(), content);
            content_mime_map.insert(content_meta.id.clone(), content_meta.mime);
        }

        let mut index_search_results = Vec::new();
        for result in search_result {
            let content = content_byte_map.get(result.content_id.as_str());
            if content.is_none() {
                continue;
            }
            let content = content.unwrap().clone();
            let mime_type = content_mime_map
                .get(&result.content_id)
                .unwrap()
                .to_string();
            let text = if mime_type.starts_with("text/") {
                String::from_utf8(content.to_vec()).unwrap()
            } else {
                String::from("")
            };
            let search_result = ScoredText {
                text,
                content_id: result.content_id.clone(),
                mime_type,
                labels: HashMap::new(),
                confidence_score: result.confidence_score,
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}
