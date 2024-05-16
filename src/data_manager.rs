use std::{
    collections::HashMap,
    fmt,
    hash::{DefaultHasher, Hash, Hasher},
    str::FromStr,
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{self, CreateContentStatus};
use itertools::Itertools;
use mime::Mime;
use nanoid::nanoid;
use sha2::{Digest, Sha256};
use tracing::{error, info};

use crate::{
    api::{self, BeginExtractedContentIngest, ExtractionGraphRequest},
    blob_storage::{BlobStorage, BlobStorageWriter, PutResult, StoragePartWriter},
    coordinator_client::CoordinatorClient,
    grpc_helper::GrpcHelper,
    metadata_storage::{
        query_engine::{run_query, StructuredDataRow},
        ExtractedMetadata,
        MetadataReaderTS,
        MetadataStorageTS,
    },
    vector_index::{ScoredText, VectorIndexManager},
};

fn index_in_features(
    output_index_map: &HashMap<String, String>,
    features: &[api::Feature],
    index_name: &str,
) -> bool {
    features.iter().any(|f| match f.feature_type {
        api::FeatureType::Embedding => output_index_map
            .get(&f.name)
            .map_or(false, |index| index == index_name),
        _ => false,
    })
}

pub struct DataManager {
    pub vector_index_manager: Arc<VectorIndexManager>,
    metadata_index_manager: MetadataStorageTS,
    metadata_reader: MetadataReaderTS,
    blob_storage: Arc<BlobStorage>,
    coordinator_client: Arc<CoordinatorClient>,
}

impl fmt::Debug for DataManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataManager").finish()
    }
}

impl DataManager {
    pub fn new(
        vector_index_manager: Arc<VectorIndexManager>,
        metadata_index_manager: MetadataStorageTS,
        metadata_reader: MetadataReaderTS,
        blob_storage: Arc<BlobStorage>,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Self {
        DataManager {
            vector_index_manager,
            metadata_index_manager,
            metadata_reader,
            blob_storage,
            coordinator_client,
        }
    }

    #[tracing::instrument]
    pub async fn list_namespaces(&self) -> Result<Vec<api::DataNamespace>> {
        let req = indexify_coordinator::ListNamespaceRequest {};
        let response = self.coordinator_client.get().await?.list_ns(req).await?;
        let namespaces = response.into_inner().namespaces;
        let data_namespaces = namespaces
            .into_iter()
            .map(|r| api::DataNamespace {
                name: r.name,
                extraction_graphs: r.extraction_graphs.into_iter().map(Into::into).collect(),
            })
            .collect();
        Ok(data_namespaces)
    }

    #[tracing::instrument]
    pub async fn create_namespace(&self, namespace: &api::DataNamespace) -> Result<()> {
        info!("creating data namespace: {}", namespace.name);
        let request = indexify_coordinator::CreateNamespaceRequest {
            name: namespace.name.clone(),
        };
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_ns(request)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get(&self, name: &str) -> Result<api::DataNamespace> {
        let req = indexify_coordinator::GetNamespaceRequest {
            name: name.to_string(),
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .get_ns(req)
            .await?
            .into_inner();
        let namespace = response.namespace.ok_or(anyhow!("namespace not found"))?;
        Ok(namespace.into())
    }

    pub async fn get_extraction_policy(&self, id: &str) -> Result<api::ExtractionPolicy> {
        let req = indexify_coordinator::GetExtractionPolicyRequest {
            extraction_policy_id: id.to_string(),
        };
        let resp = self
            .coordinator_client
            .get()
            .await?
            .get_extraction_policy(req)
            .await?
            .into_inner();
        let policy = resp
            .policy
            .ok_or_else(|| anyhow!("extraction policy not found"))?;
        Ok(policy.into())
    }

    pub async fn create_extraction_graph(
        &self,
        namespace: &str,
        req: ExtractionGraphRequest,
    ) -> Result<Vec<internal_api::IndexName>> {
        let mut extraction_policies = Vec::new();
        for ep in req.extraction_policies {
            let input_params_serialized = serde_json::to_string(&ep.input_params)
                .map_err(|e| anyhow!("unable to serialize input params to str {}", e))?;
            let req = indexify_coordinator::ExtractionPolicyRequest {
                namespace: namespace.to_string(),
                extractor: ep.extractor.clone(),
                name: ep.name.clone(),
                filters: ep.filters_eq.clone().unwrap_or_default(),
                input_params: input_params_serialized,
                content_source: ep.content_source.clone().unwrap_or_default(),
                created_at: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs() as i64,
            };
            extraction_policies.push(req);
        }
        let req = indexify_coordinator::CreateExtractionGraphRequest {
            namespace: namespace.to_string(),
            name: req.name,
            policies: extraction_policies,
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .create_extraction_graph(req)
            .await?
            .into_inner();
        for (_, policy) in response.policies {
            let extractor = response
                .extractors
                .get(policy.extractor.as_str())
                .ok_or(anyhow!(format!(
                    "extractor {} not found in response",
                    policy.extractor
                ),))?;
            for (name, output_schema) in &extractor.embedding_schemas {
                let embedding_schema: internal_api::EmbeddingSchema =
                    serde_json::from_str(output_schema)?;
                let table_name = policy.output_table_mapping.get(name).unwrap();
                let _ = self
                    .vector_index_manager
                    .create_index(table_name, embedding_schema.clone())
                    .await?;
            }

            // Create metadata table for the namespace if it doesn't exist
            let _ = self
                .metadata_index_manager
                .create_metadata_table(&namespace)
                .await?;
        }
        let req = indexify_coordinator::UpdateIndexesStateRequest {
            indexes: response.indexes.clone(),
        };
        self.coordinator_client
            .get()
            .await?
            .update_indexes_state(req)
            .await?;

        let index_names = response
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect();
        Ok(index_names)
    }

    pub async fn list_content(
        &self,
        namespace: &str,
        source_filter: &str,
        parent_id_filter: &str,
        labels_eq_filter: Option<&HashMap<String, String>>,
    ) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::ListContentRequest {
            namespace: namespace.to_string(),
            source: source_filter.to_string(),
            parent_id: parent_id_filter.to_string(),
            labels_eq: labels_eq_filter.unwrap_or(&HashMap::new()).clone(),
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_content(req)
            .await?;
        let content_list = response
            .into_inner()
            .content_list
            .into_iter()
            .map(|c| c.into())
            .collect_vec();
        Ok(content_list)
    }

    #[tracing::instrument(skip(self, content_list))]
    pub async fn add_texts(
        &self,
        namespace: &str,
        content_list: Vec<api::ContentWithId>,
        extraction_graph_names: Vec<internal_api::ExtractionGraphName>,
    ) -> Result<()> {
        for content_with_id in content_list {
            let text = content_with_id.content;
            let stream = futures::stream::once(async { Ok(Bytes::from(text.bytes)) });
            let content_metadata = self
                .write_content_bytes(
                    namespace,
                    Box::pin(stream),
                    text.labels,
                    text.content_type,
                    None,
                    "",
                    Some(&content_with_id.id),
                    &extraction_graph_names,
                )
                .await?;

            let req = indexify_coordinator::CreateContentRequest {
                content: Some(content_metadata),
            };
            self.coordinator_client
                .get()
                .await?
                .create_content(GrpcHelper::into_req(req))
                .await
                .map_err(|e| {
                    anyhow!(
                        "unable to write content metadata to coordinator {}",
                        e.to_string()
                    )
                })?;
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn delete_content(&self, gc_task: &indexify_coordinator::GcTask) -> Result<()> {
        //  Remove content from blob storage
        self.blob_storage.delete(&gc_task.blob_store_path).await?;

        //  Remove features and embeddings from vector stores
        for table in &gc_task.output_tables {
            self.vector_index_manager
                .remove_embedding(table, &gc_task.content_id)
                .await?;
        }

        //  Remove any metadata
        self.metadata_index_manager
            .remove_metadata(&gc_task.namespace, &gc_task.content_id)
            .await?;

        Ok(())
    }

    #[tracing::instrument]
    pub async fn delete_file(&self, path: &str) -> Result<()> {
        self.blob_storage.delete(path).await
    }

    pub async fn ingest_remote_file(
        &self,
        namespace: &str,
        id: Option<String>,
        file: &str,
        mime: &str,
        labels: HashMap<String, String>,
        extraction_graph_names: &Vec<internal_api::ExtractionGraphName>,
    ) -> Result<String> {
        if !(["https://", "http://", "s3://", "file://"]
            .iter()
            .any(|s| file.starts_with(*s)))
        {
            return Err(anyhow!("invalid file path, must be a url, s3 or file path"));
        }
        let _ = mime::Mime::from_str(mime).map_err(|e| anyhow!("invalid mime type {}", e))?;
        let current_ts_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let id = id.unwrap_or(nanoid!(16));
        let content_metadata = indexify_coordinator::ContentMetadata {
            id: id.clone(),
            file_name: file.to_string(),
            storage_url: file.to_string(),
            parent_id: "".to_string(),
            created_at: current_ts_secs as i64,
            mime: mime.to_string(),
            namespace: namespace.to_string(),
            labels,
            source: "".to_string(),
            size_bytes: 0,
            hash: "".to_string(),
            extraction_policy_ids: HashMap::new(),
            root_content_id: "".to_string(),
            extraction_graph_names: extraction_graph_names.clone(),
        };
        let req: indexify_coordinator::CreateContentRequest =
            indexify_coordinator::CreateContentRequest {
                content: Some(content_metadata),
            };
        self.coordinator_client
            .get()
            .await?
            .create_content(GrpcHelper::into_req(req))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to write content metadata to coordinator {}",
                    e.to_string()
                )
            })?;
        Ok(id)
    }

    pub async fn get_content_metadata(
        &self,
        _namespace: &str,
        content_ids: Vec<String>,
    ) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::GetContentMetadataRequest {
            content_list: content_ids,
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .get_content_metadata(req)
            .await?
            .into_inner();
        Ok(response.content_list.into_iter().map(Into::into).collect())
    }

    pub async fn get_content_tree_metadata(
        &self,
        _namespace: &str,
        content_id: String,
    ) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::GetContentTreeMetadataRequest { content_id };
        let response = self
            .coordinator_client
            .get()
            .await?
            .get_content_tree_metadata(req)
            .await?;
        let content_list: Vec<api::ContentMetadata> = response
            .into_inner()
            .content_list
            .into_iter()
            .map(|content| content.into())
            .collect();
        Ok(content_list)
    }

    #[tracing::instrument(skip(self, data))]
    pub async fn upload_file(
        &self,
        namespace: &str,
        data: impl Stream<Item = Result<Bytes>> + Send + Unpin,
        name: &str,
        mime_type: Mime,
        labels: HashMap<String, String>,
        original_content_id: Option<&str>,
        extraction_graph_names: Vec<internal_api::ExtractionGraphName>,
    ) -> Result<indexify_coordinator::ContentMetadata> {
        let content_metadata = self
            .write_content_bytes(
                namespace,
                data,
                labels,
                mime_type.to_string(),
                Some(name),
                "",
                original_content_id,
                &extraction_graph_names,
            )
            .await
            .map_err(|e| anyhow!("unable to write content to blob store: {}", e))?;
        Ok(content_metadata)
    }

    pub async fn create_content_metadata(
        &self,
        content_metadata: indexify_coordinator::ContentMetadata,
    ) -> Result<()> {
        let req = indexify_coordinator::CreateContentRequest {
            content: Some(content_metadata),
        };
        self.coordinator_client
            .get()
            .await?
            .create_content(GrpcHelper::into_req(req))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to write content metadata to coordinator {}",
                    e.to_string()
                )
            })?;
        Ok(())
    }

    pub fn make_file_name(file_name: Option<&str>) -> String {
        file_name.map(|f| f.to_string()).unwrap_or(nanoid!())
    }

    pub fn make_id() -> String {
        let mut s = DefaultHasher::new();
        let id = nanoid!(16);
        id.hash(&mut s);
        format!("{:x}", s.finish())
    }

    /// Checks if the given string is a valid hexadecimal.
    pub fn is_hex_string(s: &str) -> bool {
        s.chars().all(|c| c.is_ascii_hexdigit())
    }

    async fn write_content_bytes(
        &self,
        namespace: &str,
        data: impl Stream<Item = Result<Bytes>> + Send + Unpin,
        labels: HashMap<String, String>,
        content_type: String,
        file_name: Option<&str>,
        source: &str,
        original_content_id: Option<&str>,
        extraction_graph_names: &Vec<internal_api::ExtractionGraphName>,
    ) -> Result<indexify_coordinator::ContentMetadata> {
        let current_ts_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let file_name = DataManager::make_file_name(file_name);

        let mut hasher = Sha256::new();
        let hashed_stream = data.map(|item| match item {
            Ok(bytes) => {
                hasher.update(&bytes);
                Ok(bytes)
            }
            Err(e) => Err(e),
        });

        let res = self
            .write_to_blob_store(namespace, &file_name, hashed_stream)
            .await
            .map_err(|e| anyhow!("unable to write text to blob store: {}", e))?;

        let hash_result = hasher.finalize();
        let content_hash = format!("{:x}", hash_result);

        let mut id = DataManager::make_id();
        if original_content_id.is_some() {
            id = original_content_id.unwrap().to_string();
        }
        Ok(indexify_coordinator::ContentMetadata {
            id: id.clone(),
            file_name,
            storage_url: res.url,
            parent_id: "".to_string(),
            root_content_id: "".to_string(),
            created_at: current_ts_secs as i64,
            mime: content_type,
            namespace: namespace.to_string(),
            labels,
            source: source.to_string(),
            size_bytes: res.size_bytes,
            hash: content_hash,
            extraction_policy_ids: HashMap::new(),
            extraction_graph_names: extraction_graph_names.to_vec(),
        })
    }

    pub async fn finish_extracted_content_write(
        &self,
        begin_ingest: BeginExtractedContentIngest,
    ) -> Result<()> {
        let outcome: indexify_coordinator::TaskOutcome = begin_ingest.task_outcome.into();

        let req = indexify_coordinator::UpdateTaskRequest {
            executor_id: begin_ingest.executor_id,
            task_id: begin_ingest.task_id,
            outcome: outcome as i32,
        };
        let res = self.coordinator_client.get().await?.update_task(req).await;
        if let Err(err) = res {
            error!("unable to update task: {}", err.to_string());
        }
        Ok(())
    }

    pub async fn write_extracted_embedding(
        &self,
        name: &str,
        embedding: &[f32],
        content_id: &str,
        output_index_map: &HashMap<String, String>,
        metadata: HashMap<String, serde_json::Value>,
        root_content_metadata: Option<internal_api::ContentMetadata>,
        content_metadata: internal_api::ContentMetadata,
    ) -> Result<()> {
        let embeddings = internal_api::ExtractedEmbeddings {
            content_id: content_id.to_string(),
            embedding: embedding.to_vec(),
            metadata,
            root_content_metadata,
            content_metadata,
        };
        let index_table = output_index_map
            .get(name)
            .ok_or(anyhow!("index table not {} found", name))?;
        self.vector_index_manager
            .add_embedding(index_table, vec![embeddings])
            .await
            .map_err(|e| anyhow!("unable to add embedding to vector index {}", e))?;
        Ok(())
    }

    // Combine metadata from existing metadata and new features into single json
    // object
    fn combine_metadata(
        metadata: Vec<ExtractedMetadata>,
        features: &[api::Feature],
        labels: HashMap<String, serde_json::Value>,
    ) -> HashMap<String, serde_json::Value> {
        let mut combined_metadata = HashMap::new();
        for m in metadata {
            for (k, v) in m.metadata.as_object().unwrap() {
                combined_metadata.insert(k.clone(), v.clone());
            }
        }
        for feature in features {
            if let api::FeatureType::Metadata = feature.feature_type {
                if let serde_json::Value::Object(data) = &feature.data {
                    for (k, v) in data {
                        combined_metadata.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        for (k, v) in labels {
            combined_metadata.insert(k, v);
        }
        combined_metadata
    }

    pub async fn write_existing_content_features(
        &self,
        extractor: &str,
        extraction_graph_name: &str,
        content_metadata: &indexify_coordinator::ContentMetadata,
        root_content_metadata: Option<indexify_internal_api::ContentMetadata>,
        features: Vec<api::Feature>,
        output_index_mapping: &HashMap<String, String>,
        index_tables: &[String],
    ) -> Result<()> {
        let metadata_updated = features
            .iter()
            .any(|feature| matches!(feature.feature_type, api::FeatureType::Metadata));
        let existing_metadata = self
            .metadata_index_manager
            .get_metadata_for_content(&content_metadata.namespace, &content_metadata.id)
            .await?;
        let content_metadata_labels = content_metadata
            .labels
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone())),
                )
            })
            .collect();
        let new_metadata =
            Self::combine_metadata(existing_metadata, &features, content_metadata_labels);
        self.write_extracted_features(
            extractor,
            extraction_graph_name,
            content_metadata.clone(),
            root_content_metadata,
            features.clone(),
            new_metadata.clone(),
            output_index_mapping,
        )
        .await?;
        if metadata_updated && !new_metadata.is_empty() {
            // For all embeddings not updated with new values, update their metadata
            for index in index_tables {
                if !index_in_features(output_index_mapping, &features, index) {
                    info!(
                        "updating metadata for content {} index {}",
                        content_metadata.id.clone(),
                        index
                    );
                    self.vector_index_manager
                        .update_metadata(index, content_metadata.id.clone(), new_metadata.clone())
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn write_extracted_features(
        &self,
        extractor: &str,
        extraction_graph_name: &str,
        content_metadata: indexify_coordinator::ContentMetadata,
        root_content_metadata: Option<indexify_internal_api::ContentMetadata>,
        features: Vec<api::Feature>,
        metadata: HashMap<String, serde_json::Value>,
        output_index_map: &HashMap<String, String>,
    ) -> Result<()> {
        let content_metadata: internal_api::ContentMetadata = content_metadata.try_into()?;
        for feature in &features {
            match feature.feature_type {
                api::FeatureType::Embedding => {
                    let embedding_payload: internal_api::Embedding =
                        serde_json::from_value(feature.data.clone()).map_err(|e| {
                            anyhow!("unable to get embedding from extracted data {}", e)
                        })?;
                    self.write_extracted_embedding(
                        &feature.name,
                        &embedding_payload.values,
                        &content_metadata.id.id,
                        output_index_map,
                        metadata.clone(),
                        root_content_metadata.clone(),
                        content_metadata.clone(),
                    )
                    .await?;
                }
                api::FeatureType::Metadata => {
                    let extracted_attributes = ExtractedMetadata::new(
                        &content_metadata.id.id,
                        &content_metadata
                            .parent_id
                            .clone()
                            .map(|id| id.id)
                            .unwrap_or_default(),
                        &content_metadata.source.to_string(),
                        feature.data.clone(),
                        extractor,
                        extraction_graph_name,
                    );
                    info!("adding metadata to index {}", feature.data.to_string());
                    self.metadata_index_manager
                        .add_metadata(&content_metadata.namespace, extracted_attributes)
                        .await?;
                }
                _ => {
                    error!("unsupported feature type: {:?}", feature.feature_type);
                }
            }
        }
        Ok(())
    }

    pub async fn create_content_and_write_features(
        &self,
        content_metadata: &indexify_coordinator::ContentMetadata,
        root_content_metadata: Option<indexify_internal_api::ContentMetadata>,
        extractor: &str,
        extraction_graph_name: &str,
        features: Vec<api::Feature>,
        output_index_map: &HashMap<String, String>,
    ) -> Result<()> {
        let req = indexify_coordinator::CreateContentRequest {
            content: Some(content_metadata.clone()),
        };
        let res = self
            .coordinator_client
            .get()
            .await?
            .create_content(GrpcHelper::into_req(req))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to write content metadata to coordinator {}",
                    e.to_string()
                )
            })?
            .into_inner();
        if res.status() == CreateContentStatus::Duplicate {
            if let Err(e) = self.delete_file(&content_metadata.storage_url).await {
                tracing::warn!(
                    "unable to delete duplicate file for {:?}: {}",
                    content_metadata.id,
                    e
                );
            }
            return Ok(());
        }
        let content_metadata_labels = content_metadata
            .labels
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone())),
                )
            })
            .collect();
        let metadata = Self::combine_metadata(Vec::new(), &features, content_metadata_labels);
        self.write_extracted_features(
            extractor,
            extraction_graph_name,
            content_metadata.clone(),
            root_content_metadata,
            features,
            metadata,
            output_index_map,
        )
        .await
    }

    pub async fn query_content_source(
        &self,
        namespace: &str,
        query: &str,
    ) -> Result<Vec<StructuredDataRow>> {
        let schemas = self
            .coordinator_client
            .get_structured_schemas(namespace)
            .await?;
        let metadata_reader = self.metadata_reader.clone();
        let namespace = namespace.to_string();
        let query = query.to_string();
        tokio::task::spawn_blocking(move || {
            futures::executor::block_on(async move {
                run_query(query, metadata_reader, schemas, namespace).await
            })
        })
        .await?
    }

    #[tracing::instrument]
    pub async fn list_indexes(&self, namespace: &str) -> Result<Vec<api::Index>> {
        let req = indexify_coordinator::ListIndexesRequest {
            namespace: namespace.to_string(),
        };
        let resp = self
            .coordinator_client
            .get()
            .await?
            .list_indexes(req)
            .await?;
        let mut api_indexes = Vec::new();
        for index in resp.into_inner().indexes {
            let api_index = index.try_into()?;
            api_indexes.push(api_index);
        }
        Ok(api_indexes)
    }

    #[tracing::instrument]
    pub async fn search(
        &self,
        namespace: &str,
        index_name: &str,
        query: &str,
        k: u64,
        filters: Vec<String>,
        include_content: bool,
    ) -> Result<Vec<ScoredText>> {
        let req = indexify_coordinator::GetIndexRequest {
            namespace: namespace.to_string(),
            name: index_name.to_string(),
        };
        let index = self
            .coordinator_client
            .get()
            .await?
            .get_index(req)
            .await?
            .into_inner()
            .index
            .ok_or(anyhow!("Index not found"))?;
        self.vector_index_manager
            .search(index, query, k as usize, filters, include_content)
            .await
    }

    #[tracing::instrument]
    pub async fn metadata_lookup(
        &self,
        namespace: &str,
        content_id: &str,
    ) -> Result<Vec<ExtractedMetadata>, anyhow::Error> {
        self.metadata_index_manager
            .get_metadata_for_content(namespace, content_id)
            .await
    }

    #[tracing::instrument]
    pub async fn list_extractors(&self) -> Result<Vec<api::ExtractorDescription>> {
        let req = indexify_coordinator::ListExtractorsRequest {};
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_extractors(req)
            .await?
            .into_inner();

        let extractors = response
            .extractors
            .into_iter()
            .map(|e| e.try_into())
            .collect::<Result<Vec<api::ExtractorDescription>>>()?;
        Ok(extractors)
    }

    #[tracing::instrument(skip(file))]
    pub async fn write_to_blob_store(
        &self,
        namespace: &str,
        name: &str,
        file: impl Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult> {
        self.blob_storage.put(name, file).await
    }

    pub async fn blob_store_writer(&self, namespace: &str, key: &str) -> Result<StoragePartWriter> {
        self.blob_storage.writer(namespace, key).await
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_combine_metadata() {
        let _features = vec![
            api::Feature {
                name: String::from(""),
                feature_type: api::FeatureType::Metadata,
                data: json!({"key1": "value1"}),
            },
            api::Feature {
                name: String::from(""),
                feature_type: api::FeatureType::Metadata,
                data: json!({"key2": "value2"}),
            },
            api::Feature {
                name: String::from(""),
                feature_type: api::FeatureType::Embedding,
                data: json!({"values": "[0.1, 0.2, 0.3]"}),
            },
            api::Feature {
                name: String::from(""),
                feature_type: api::FeatureType::Metadata,
                data: json!({"key3": "value3"}),
            },
        ];

        let _labels = HashMap::from([("label1".to_string(), "value1".to_string())]);

        //let combined = DataManager::combine_metadata(Vec::new(), &features, labels);
        let _expected = json!({
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "label1": "value1",
        });

        //assert_eq!(combined, expected);
    }
}
