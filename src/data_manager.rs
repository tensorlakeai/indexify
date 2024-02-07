use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    fmt,
    hash::{Hash, Hasher},
    path::Path,
    sync::Arc,
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator;
use nanoid::nanoid;
use tracing::{error, info};

use crate::{
    api::{self, Content, EmbeddingSchema},
    blob_storage::{BlobStorage, BlobStorageConfig, BlobStorageReader, BlobStorageWriter},
    coordinator_client::CoordinatorClient,
    extractor::ExtractedEmbeddings,
    grpc_helper::GrpcHelper,
    metadata_index::{ExtractedMetadata, MetadataIndexManager},
    vector_index::{ScoredText, VectorIndexManager},
};

pub struct DataManager {
    vector_index_manager: Arc<VectorIndexManager>,
    metadata_index_manager: Arc<MetadataIndexManager>,
    blob_storage: BlobStorage,
    coordinator_client: Arc<CoordinatorClient>,
}

impl fmt::Debug for DataManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataManager").finish()
    }
}

impl DataManager {
    pub async fn new(
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<MetadataIndexManager>,
        blob_storage_config: BlobStorageConfig,
        coordinator_client: Arc<CoordinatorClient>,
    ) -> Result<Self> {
        let blob_storage = BlobStorage::new_with_config(blob_storage_config);
        Ok(Self {
            vector_index_manager,
            metadata_index_manager: attribute_index_manager,
            blob_storage,
            coordinator_client,
        })
    }

    #[tracing::instrument]
    pub async fn list_namespaces(&self) -> Result<Vec<api::DataNamespace>> {
        let req = indexify_coordinator::ListRepositoriesRequest {};
        let response = self
            .coordinator_client
            .get()
            .await?
            .list_repositories(req)
            .await?;
        let namespaces = response.into_inner().repositories;
        let data_namespaces = namespaces
            .into_iter()
            .map(|r| api::DataNamespace {
                name: r.name,
                extractor_bindings: Vec::new(),
            })
            .collect();
        Ok(data_namespaces)
    }

    #[tracing::instrument]
    pub async fn create(&self, namespace: &api::DataNamespace) -> Result<()> {
        info!("creating data namespace: {}", namespace.name);
        let bindings = namespace
            .extractor_bindings
            .clone()
            .into_iter()
            .map(|b| b.into())
            .collect();
        let request = indexify_coordinator::CreateRepositoryRequest {
            name: namespace.name.clone(),
            bindings,
        };
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_repository(request)
            .await?;
        Ok(())
    }

    #[tracing::instrument]
    pub async fn get(&self, name: &str) -> Result<api::DataNamespace> {
        let req = indexify_coordinator::GetRepositoryRequest {
            name: name.to_string(),
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .get_repository(req)
            .await?
            .into_inner();
        let namespace = response.repository.ok_or(anyhow!("namespace not found"))?;
        namespace.try_into()
    }

    pub async fn add_extractor_binding(
        &self,
        namespace: &str,
        extractor_binding: &api::ExtractorBinding,
    ) -> Result<Vec<String>> {
        info!(
            "adding extractor bindings namespace: {}, extractor: {}, binding: {}",
            namespace, extractor_binding.extractor, extractor_binding.name,
        );
        let req = indexify_coordinator::ExtractorBindRequest {
            repository: namespace.to_string(),
            binding: Some(extractor_binding.clone().into()),
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs() as i64,
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .create_binding(req)
            .await?
            .into_inner();
        let mut index_names = Vec::new();
        let extractor = response.extractor.ok_or(anyhow!(
            "extractor {:?} not found",
            extractor_binding.extractor
        ))?;
        for (name, output_schema) in &extractor.outputs {
            let output_schema: internal_api::OutputSchema = serde_json::from_str(output_schema)?;
            let index_name = response.output_index_name_mapping.get(name).unwrap();
            let table_name = response.index_name_table_mapping.get(index_name).unwrap();
            index_names.push(index_name.clone());
            match output_schema {
                internal_api::OutputSchema::Embedding(embedding_schema) => {
                    let _ = self
                        .vector_index_manager
                        .create_index(table_name, embedding_schema.clone())
                        .await?;
                    self.create_index_metadata(
                        namespace,
                        index_name,
                        table_name,
                        serde_json::to_value(embedding_schema)?,
                        &extractor_binding.name,
                        &extractor.name,
                    )
                    .await?;
                }
                internal_api::OutputSchema::Attributes(schema) => {
                    let _ = self
                        .metadata_index_manager
                        .create_index(
                            namespace,
                            &index_name,
                            table_name,
                            &extractor.name,
                            &extractor_binding.name,
                            schema,
                        )
                        .await?;
                }
            }
        }

        Ok(index_names)
    }

    async fn create_index_metadata(
        &self,
        namespace: &str,
        index_name: &str,
        table_name: &str,
        schema: serde_json::Value,
        binding: &str,
        extractor: &str,
    ) -> Result<()> {
        let index = indexify_coordinator::CreateIndexRequest {
            index: Some(indexify_coordinator::Index {
                name: index_name.to_string(),
                table_name: table_name.to_string(),
                repository: namespace.to_string(),
                schema: serde_json::to_value(schema).unwrap().to_string(),
                extractor_binding: binding.to_string(),
                extractor: extractor.to_string(),
            }),
        };
        let req = GrpcHelper::into_req(index);
        let _resp = self
            .coordinator_client
            .get()
            .await?
            .create_index(req)
            .await?;
        Ok(())
    }

    pub async fn list_content(
        &self,
        namespace: &str,
        source_filter: &str,
        parent_id_filter: &str,
        labels_eq_filter: Option<&HashMap<String, String>>,
    ) -> Result<Vec<api::ContentMetadata>> {
        let req = indexify_coordinator::ListContentRequest {
            repository: namespace.to_string(),
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
        let content_list = response.into_inner().content_list;
        let mut content = Vec::new();

        for c in content_list {
            content.push(api::ContentMetadata {
                id: c.id,
                name: c.file_name,
                storage_url: c.storage_url,
                parent_id: c.parent_id,
                created_at: c.created_at,
                content_type: c.mime,
                namespace: c.repository,
                labels: c.labels,
                source: c.source,
            })
        }
        Ok(content)
    }

    #[tracing::instrument]
    pub async fn add_texts(&self, namespace: &str, content_list: Vec<api::Content>) -> Result<()> {
        for text in content_list {
            let content_metadata = self
                .write_content(namespace, text, None, None, "ingestion")
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

    pub async fn read_content(
        &self,
        _namespace: &str,
        content_ids: Vec<String>,
    ) -> Result<Vec<Content>> {
        let req = indexify_coordinator::GetContentMetadataRequest {
            content_list: content_ids,
        };
        let response = self
            .coordinator_client
            .get()
            .await?
            .get_content_metadata(req)
            .await?;
        let content_metadata_list = response.into_inner().content_list;
        let content_list = self
            .blob_storage
            .get(
                &content_metadata_list
                    .iter()
                    .map(|item| item.storage_url.as_str())
                    .collect::<Vec<_>>(),
            )
            .await?
            .into_iter()
            .zip(content_metadata_list.into_iter())
            .map(|(bytes, content)| Content {
                content_type: content.mime,
                bytes,
                labels: content.labels.clone(),
                features: vec![],
            })
            .collect();
        Ok(content_list)
    }

    #[tracing::instrument(skip(self, data))]
    pub async fn upload_file(&self, namespace: &str, data: Bytes, name: &str) -> Result<()> {
        let ext = Path::new(name)
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();
        let content_mime = mime_guess::from_ext(ext).first_or_octet_stream();
        let content = api::Content {
            content_type: content_mime.to_string(),
            bytes: data.to_vec(),
            labels: HashMap::new(),
            features: vec![],
        };
        let content_metadata = self
            .write_content(namespace, content, Some(name), None, "ingestion")
            .await
            .map_err(|e| anyhow!("unable to write content to blob store: {}", e))?;
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

    async fn write_content(
        &self,
        namespace: &str,
        content: api::Content,
        file_name: Option<&str>,
        parent_id: Option<String>,
        source: &str,
    ) -> Result<indexify_coordinator::ContentMetadata> {
        let current_ts_secs = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();
        let mut s = DefaultHasher::new();
        namespace.hash(&mut s);
        content.bytes.hash(&mut s);
        if let Some(f) = file_name {
            f.hash(&mut s);
        }
        let file_name = file_name.map(|f| f.to_string()).unwrap_or(nanoid!());
        if let Some(parent_id) = &parent_id {
            parent_id.hash(&mut s);
        }
        let id = format!("{:x}", s.finish());
        let storage_url = self
            .write_to_blob_store(namespace, &file_name, Bytes::from(content.bytes.clone()))
            .await
            .map_err(|e| anyhow!("unable to write text to blob store: {}", e))?;
        let labels = content
            .labels
            .clone()
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();
        Ok(indexify_coordinator::ContentMetadata {
            id,
            file_name,
            storage_url,
            parent_id: parent_id.unwrap_or_default(),
            created_at: current_ts_secs as i64,
            mime: content.content_type,
            repository: namespace.to_string(),
            labels,
            source: source.to_string(),
        })
    }

    pub async fn write_extracted_content(
        &self,
        extracted_content: api::WriteExtractedContent,
    ) -> Result<()> {
        let namespace = extracted_content.namespace.clone();
        let mut new_content_metadata = Vec::new();
        for content in extracted_content.content_list {
            let content: api::Content = content.into();
            let content_metadata = self
                .write_content(
                    extracted_content.namespace.as_str(),
                    content.clone(),
                    None,
                    Some(extracted_content.parent_content_id.to_string()),
                    &extracted_content.extractor_binding,
                )
                .await?;
            new_content_metadata.push(content_metadata.clone());
            for feature in content.features {
                let index_table_name = extracted_content
                    .output_to_index_table_mapping
                    .get(&feature.name);
                if index_table_name.is_none() {
                    error!(
                        "unable to find index table name for feature {}",
                        feature.name
                    );
                    continue;
                }
                let index_table_name = index_table_name.unwrap();
                match feature.feature_type {
                    api::FeatureType::Embedding => {
                        let embedding_payload: internal_api::Embedding =
                            serde_json::from_value(feature.data).map_err(|e| {
                                anyhow!("unable to get embedding from extracted data {}", e)
                            })?;
                        let embeddings = ExtractedEmbeddings {
                            content_id: content_metadata.id.to_string(),
                            embedding: embedding_payload.values,
                        };
                        self.vector_index_manager
                            .add_embedding(&index_table_name.clone(), vec![embeddings])
                            .await
                            .map_err(|e| {
                                anyhow!("unable to add embedding to vector index {}", e)
                            })?;
                    }
                    api::FeatureType::Metadata => {
                        let extracted_attributes = ExtractedMetadata::new(
                            &content_metadata.id,
                            &content_metadata.parent_id,
                            feature.data.clone(),
                            "extractor_name",
                            &namespace,
                        );
                        info!("adding metadata to index {}", feature.data.to_string());
                        self.metadata_index_manager
                            .add_metadata(
                                &namespace,
                                &index_table_name.clone(),
                                extracted_attributes,
                            )
                            .await?;
                    }
                    _ => {}
                }
            }
        }

        let outcome: indexify_coordinator::TaskOutcome = extracted_content.task_outcome.into();

        let req = indexify_coordinator::UpdateTaskRequest {
            executor_id: extracted_content.executor_id,
            task_id: extracted_content.task_id,
            outcome: outcome as i32,
            content_list: new_content_metadata,
        };
        let res = self.coordinator_client.get().await?.update_task(req).await;
        if let Err(err) = res {
            error!("unable to update task: {}", err.to_string());
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn list_indexes(&self, namespace: &str) -> Result<Vec<api::Index>> {
        let req = indexify_coordinator::ListIndexesRequest {
            repository: namespace.to_string(),
        };
        let resp = self
            .coordinator_client
            .get()
            .await?
            .list_indexes(req)
            .await?;
        let indexes = resp
            .into_inner()
            .indexes
            .into_iter()
            .map(|i| api::Index {
                name: i.name,
                schema: api::ExtractorOutputSchema::Embedding(EmbeddingSchema {
                    dim: 384,
                    distance: api::IndexDistance::Cosine,
                }),
            })
            .collect();
        Ok(indexes)
    }

    #[tracing::instrument]
    pub async fn search(
        &self,
        namespace: &str,
        index_name: &str,
        query: &str,
        k: u64,
    ) -> Result<Vec<ScoredText>> {
        let req = indexify_coordinator::GetIndexRequest {
            repository: namespace.to_string(),
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
            .search(index, query, k as usize)
            .await
    }

    #[tracing::instrument]
    pub async fn metadata_lookup(
        &self,
        namespace: &str,
        index_name: &str,
        content_id: Option<&String>,
    ) -> Result<Vec<ExtractedMetadata>, anyhow::Error> {
        let req = indexify_coordinator::GetIndexRequest {
            repository: namespace.to_string(),
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

        self.metadata_index_manager
            .get_attributes(namespace, &index.table_name, content_id)
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

    #[tracing::instrument]
    async fn write_to_blob_store(
        &self,
        namespace: &str,
        name: &str,
        file: Bytes,
    ) -> Result<String, anyhow::Error> {
        let stored_file_path = self.blob_storage.put(name, file).await?;
        Ok(stored_file_path)
    }
}
