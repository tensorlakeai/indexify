use std::{collections::HashMap, fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, Error, Result};
use base64::{engine::general_purpose, Engine as _};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{self, Index};
use internal_api::ExtractedEmbeddings;
use itertools::Itertools;
use tracing::info;

use crate::{
    api::{self},
    blob_storage::{BlobStorage, BlobStorageReader},
    coordinator_client::CoordinatorClient,
    extractor_router::ExtractorRouter,
    vectordbs::{CreateIndexParams, IndexDistance, VectorChunk, VectorDBTS},
};

pub struct VectorIndexManager {
    vector_db: VectorDBTS,
    extractor_router: ExtractorRouter,
    coordinator_client: Arc<CoordinatorClient>,
}

impl fmt::Debug for VectorIndexManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIndexManager").finish()
    }
}

pub struct ScoredText {
    pub text: String,
    pub content_id: String,
    pub labels: HashMap<String, String>,
    pub confidence_score: f32,
}

impl VectorIndexManager {
    pub fn new(coordinator_client: Arc<CoordinatorClient>, vector_db: VectorDBTS) -> Result<Self> {
        let extractor_router = ExtractorRouter::new(coordinator_client.clone())?;
        Ok(Self {
            vector_db,
            extractor_router,
            coordinator_client: coordinator_client.clone(),
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
        info!("Adding embeddings to index: {}", vector_index_name);
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
        let content = self
            .extractor_router
            .extract_content(&index.extractor, content, None)
            .await
            .map_err(|e| anyhow!("unable to extract embedding: {}", e.to_string()))?
            .pop()
            .ok_or(anyhow!("No content was extracted"))?;
        let feature = content
            .features
            .first()
            .ok_or(anyhow!("No features were extracted"))?;
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

        let content_id_to_blob = BlobStorage::new()
            .get(
                &content_metadata_list
                    .content_list
                    .iter()
                    .map(|item| item.storage_url.as_str())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .zip(content_metadata_list.content_list.into_iter())
            .map(|(data, content)| {
                let text = match content.mime.as_str() {
                    "text/plain" => String::from_utf8(data)?,
                    "application/json" => {
                        let json: serde_json::Value = serde_json::from_slice(&data)?;
                        json.to_string()
                    }
                    _ => general_purpose::STANDARD.encode(&data),
                };
                Ok::<_, Error>((content.id, text))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<HashMap<_, _>>();

        let mut index_search_results = Vec::new();
        for result in search_result {
            let chunk = content_id_to_blob.get(&result.content_id);
            if chunk.is_none() {
                continue;
            }
            let search_result = ScoredText {
                text: chunk.unwrap().to_string(),
                content_id: result.content_id.clone(),
                labels: HashMap::new(),
                confidence_score: result.confidence_score,
            };
            index_search_results.push(search_result);
        }
        Ok(index_search_results)
    }
}
