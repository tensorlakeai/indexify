use std::collections::HashMap;

use anyhow::{anyhow, Result};
use axum_typed_websockets::{Message, WebSocket};
use indexify_proto::indexify_coordinator;
use sha2::{
    digest::{
        consts::{B0, B1},
        core_api::{CoreWrapper, CtVariableCoreWrapper},
        typenum::{UInt, UTerm},
    },
    Digest,
    OidSha256,
    Sha256,
    Sha256VarCore,
};
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::{
    api::*,
    blob_storage::StoragePartWriter,
    data_manager::DataManager,
    server::NamespaceEndpointState,
};

#[derive(Debug)]
struct Writing {
    created_at: i64,
    file_name: String,
    file_size: u64,
    writer: StoragePartWriter,
    hasher: CoreWrapper<
        CtVariableCoreWrapper<
            Sha256VarCore,
            UInt<UInt<UInt<UInt<UInt<UInt<UTerm, B1>, B0>, B0>, B0>, B0>, B0>,
            OidSha256,
        >,
    >,
    frame_count: u64,
}

#[derive(Debug)]
enum FrameState {
    New,
    Writing(Writing),
}

pub struct IngestExtractedContentState {
    ingest_metadata: Option<BeginExtractedContentIngest>,
    content_metadata: Option<indexify_coordinator::ContentMetadata>,
    state: NamespaceEndpointState,
    frame_state: FrameState,
}

impl IngestExtractedContentState {
    pub fn new(state: NamespaceEndpointState) -> Self {
        Self {
            ingest_metadata: None,
            content_metadata: None,
            state,
            frame_state: FrameState::New,
        }
    }

    fn begin(&mut self, payload: BeginExtractedContentIngest) {
        info!(
            "beginning extraction ingest for task: {} index_tables: {}",
            payload.task_id,
            payload.index_tables.join(",")
        );
        self.ingest_metadata.replace(payload);
    }

    async fn write_content(&mut self, payload: ExtractedContent) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received extracted content without header metadata"
            ));
        }
        self.state
            .data_manager
            .write_extracted_content(self.ingest_metadata.clone().unwrap(), payload)
            .await
    }

    async fn start_content(&mut self) -> Result<()> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let file_name = DataManager::make_file_name(None);
        let metadata = self.ingest_metadata.as_ref().unwrap();
        let writer = self
            .state
            .data_manager
            .blob_store_writer(&metadata.namespace, &file_name)
            .await?;
        self.frame_state = FrameState::Writing(Writing {
            created_at: ts as i64,
            file_name,
            file_size: 0,
            writer,
            hasher: Sha256::new(),
            frame_count: 0,
        });
        Ok(())
    }

    async fn begin_multipart_content(&mut self) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!("received begin content without header metadata"));
        }
        info!(
            "beginning multipart content ingest for task: {}",
            self.ingest_metadata.as_ref().unwrap().task_id
        );
        match &self.frame_state {
            FrameState::New => {
                self.start_content().await?;
            }
            FrameState::Writing(_) => {
                return Err(anyhow!(
                    "received begin content without finishing previous content"
                ));
            }
        }
        Ok(())
    }

    async fn write_content_frame(&mut self, payload: ContentFrame) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received finished extraction ingest without header metadata"
            ));
        }
        match &mut self.frame_state {
            FrameState::New => Err(anyhow!(
                "received content frame without starting multipart content"
            )),
            FrameState::Writing(frame_state) => {
                let frame_index_bytes = frame_state.frame_count.to_le_bytes(); //  NOTE: doing this so hash is consistent independent of platform
                frame_state.file_size += payload.bytes.len() as u64;
                frame_state.hasher.update(frame_index_bytes);
                frame_state.hasher.update(&payload.bytes);
                frame_state.frame_count += 1;
                frame_state
                    .writer
                    .writer
                    .write_all(&payload.bytes)
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "unable to write extracted content frame to blob store: {}",
                            e
                        )
                    })
            }
        }
    }

    async fn finish_content(&mut self, payload: FinishContent) -> Result<String> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received finished extraction ingest without header metadata"
            ));
        }
        info!(
            "received finish multipart content for task: {}",
            self.ingest_metadata.as_ref().unwrap().task_id
        );
        // let mut ret_id = None;
        match &mut self.frame_state {
            FrameState::New => Err(anyhow!(
                "received finish content without any content frames"
            )),
            FrameState::Writing(frame_state) => {
                frame_state.writer.writer.shutdown().await?;
                let metadata = self.ingest_metadata.as_ref().unwrap();
                let hash_result = frame_state.hasher.clone().finalize();
                let content_hash = format!("{:x}", hash_result);
                let id = DataManager::make_id(
                    &metadata.namespace,
                    &Some(metadata.parent_content_id.clone()),
                    &content_hash,
                );
                let content_metadata = indexify_coordinator::ContentMetadata {
                    id: id.clone(),
                    file_name: frame_state.file_name.clone(),
                    parent_id: metadata.parent_content_id.clone(),
                    namespace: metadata.namespace.clone(),
                    mime: payload.content_type,
                    size_bytes: frame_state.file_size,
                    storage_url: frame_state.writer.url.clone(),
                    labels: payload.labels,
                    source: metadata.extraction_policy.clone(),
                    created_at: frame_state.created_at,
                    hash: content_hash,
                    extraction_policy_ids: HashMap::new(),
                };
                self.state
                    .data_manager
                    .create_content_and_write_features(
                        &content_metadata,
                        self.ingest_metadata.as_ref().unwrap(),
                        payload.features,
                    )
                    .await?;
                self.state.metrics.node_content_extracted.add(1, &[]);
                self.state
                    .metrics
                    .node_content_bytes_extracted
                    .add(frame_state.file_size, &[]);
                self.frame_state = FrameState::New;
                Ok(id)
            }
        }
        // Ok(ret_id)
    }

    async fn ensure_has_content_metadata(
        &mut self,
        content_id: String,
    ) -> Result<indexify_coordinator::ContentMetadata> {
        if self.content_metadata.is_none() {
            let content_metas = self
                .state
                .coordinator_client
                .get()
                .await?
                .get_content_metadata(indexify_coordinator::GetContentMetadataRequest {
                    content_list: vec![content_id.clone()],
                })
                .await?
                .into_inner()
                .content_list;

            let content_meta = content_metas
                .get(&content_id)
                .ok_or(anyhow!("No content metadata found"))?;
            self.content_metadata.replace(content_meta.clone());
        }
        Ok(self.content_metadata.clone().unwrap())
    }

    async fn write_features(&mut self, payload: ExtractedFeatures) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received extracted features without header metadata"
            ));
        }
        let content_meta = self
            .ensure_has_content_metadata(payload.content_id.clone())
            .await?;
        self.state
            .data_manager
            .write_existing_content_features(
                &self.ingest_metadata.clone().unwrap().extractor,
                &self.ingest_metadata.clone().unwrap().extraction_policy,
                &content_meta,
                payload.features,
                &self
                    .ingest_metadata
                    .as_ref()
                    .unwrap()
                    .output_to_index_table_mapping,
                &self.ingest_metadata.as_ref().unwrap().index_tables,
            )
            .await
    }

    async fn finish(&mut self) -> Result<()> {
        if self.ingest_metadata.is_none() {
            tracing::error!("received finished extraction ingest without header metadata");
            return Err(anyhow!(
                "received finished extraction ingest without header metadata"
            ));
        }
        self.state
            .data_manager
            .finish_extracted_content_write(self.ingest_metadata.clone().unwrap())
            .await?;
        Ok(())
    }

    pub async fn run(
        mut self,
        mut socket: WebSocket<IngestExtractedContentResponse, IngestExtractedContent>,
    ) {
        let _ = socket.send(Message::Ping(vec![])).await;
        while let Some(msg) = socket.recv().await {
            if let Err(err) = &msg {
                tracing::error!("error receiving message: {:?}", err);
                return;
            }
            if let Ok(Message::Item(msg)) = msg {
                match msg {
                    IngestExtractedContent::BeginExtractedContentIngest(payload) => {
                        self.begin(payload);
                    }
                    IngestExtractedContent::ExtractedContent(payload) => {
                        if let Err(e) = self.write_content(payload).await {
                            tracing::error!("Error handling extracted content: {}", e);
                            return;
                        }
                    }
                    IngestExtractedContent::BeginMultipartContent(_) => {
                        if let Err(e) = self.begin_multipart_content().await {
                            tracing::error!("Error beginning multipart content: {}", e);
                            return;
                        }
                    }
                    IngestExtractedContent::MultipartContentFrame(payload) => {
                        if let Err(e) = self.write_content_frame(payload).await {
                            tracing::error!("Error handling content frame: {}", e);
                            return;
                        }
                    }
                    IngestExtractedContent::FinishMultipartContent(payload) => {
                        if let Err(e) = self.finish_content(payload).await {
                            tracing::error!("Error finishing extacted content: {}", e);
                            return;
                        }
                    }
                    IngestExtractedContent::ExtractedFeatures(payload) => {
                        if let Err(e) = self.write_features(payload).await {
                            tracing::error!("Error handling extracted features: {}", e);
                            return;
                        }
                    }
                    IngestExtractedContent::FinishExtractedContentIngest(_payload) => {
                        if let Err(e) = self.finish().await {
                            tracing::error!("Error finishing extraction ingest: {}", e);
                            return;
                        }
                        return;
                    }
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use indexify_internal_api::TaskOutcome;
    use serde_json::json;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        blob_storage::{BlobStorage, ContentReader},
        coordinator_client::CoordinatorClient,
        data_manager::DataManager,
        metadata_storage::{self, MetadataReaderTS, MetadataStorageTS},
        metrics,
        server::NamespaceEndpointState,
        server_config::{IndexStoreKind, ServerConfig},
        vector_index::VectorIndexManager,
        vectordbs,
    };

    fn make_test_config() -> ServerConfig {
        let mut config = ServerConfig::default();
        config.coordinator_port += 100;
        config.coordinator_addr = format!("localhost:{}", config.coordinator_port);
        config.listen_port += 100;
        config.index_config.index_store = IndexStoreKind::Qdrant;
        config.index_config.qdrant_config = Some(Default::default());
        config.blob_storage = crate::blob_storage::BlobStorageConfig {
            s3: None,
            disk: Some(crate::blob_storage::DiskStorageConfig {
                path: "/tmp/indexify-test".to_string(),
            }),
        };
        config
    }

    struct TestCoordinator {
        handle: JoinHandle<()>,
    }

    impl TestCoordinator {
        async fn stop(self) {
            self.handle.abort();
            let _ = self.handle.await;
        }

        async fn new() -> TestCoordinator {
            let config = make_test_config();
            let coordinator =
                crate::coordinator_service::CoordinatorServer::new(Arc::new(config.clone()))
                    .await
                    .expect("failed to create coordinator server");
            let handle = tokio::spawn(async move {
                coordinator.run().await.unwrap();
            });
            // wait until able to connect to coordinator
            loop {
                if let Ok(_) = CoordinatorClient::new(&config.coordinator_addr).get().await {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            TestCoordinator { handle }
        }
    }

    async fn new_endpoint_state() -> Result<NamespaceEndpointState> {
        let config = make_test_config();
        let vector_db = vectordbs::create_vectordb(config.index_config.clone()).await?;
        let coordinator_client = Arc::new(CoordinatorClient::new(&config.coordinator_addr));
        let vector_index_manager = Arc::new(
            VectorIndexManager::new(coordinator_client.clone(), vector_db.clone())
                .map_err(|e| anyhow!("unable to create vector index {}", e))?,
        );
        let metadata_index_manager: MetadataStorageTS =
            metadata_storage::from_config(&config.metadata_storage)?;
        let metadata_reader: MetadataReaderTS =
            metadata_storage::from_config_reader(&config.metadata_storage)?;
        let blob_storage = Arc::new(BlobStorage::new_with_config(config.blob_storage.clone()));
        let data_manager = Arc::new(DataManager::new(
            vector_index_manager,
            metadata_index_manager,
            metadata_reader,
            blob_storage,
            coordinator_client.clone(),
        ));
        let namespace_endpoint_state = NamespaceEndpointState {
            data_manager: data_manager.clone(),
            coordinator_client: coordinator_client.clone(),
            content_reader: Arc::new(ContentReader::new()),
            metrics: Arc::new(metrics::server::Metrics::new()),
        };
        Ok(namespace_endpoint_state)
    }

    #[tokio::test]
    async fn test_new() {
        let state = new_endpoint_state().await.unwrap();
        let ingest_state = IngestExtractedContentState::new(state);
        assert!(ingest_state.ingest_metadata.is_none());
        assert!(ingest_state.content_metadata.is_none());
        match ingest_state.frame_state {
            FrameState::New => (),
            _ => panic!("frame_state should be New"),
        }
    }

    fn set_tracing() {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    #[tokio::test]
    async fn test_begin() {
        set_tracing();

        let state = new_endpoint_state().await.unwrap();
        let coordinator = TestCoordinator::new().await;

        let mut ingest_state = IngestExtractedContentState::new(state);
        let payload = BeginExtractedContentIngest {
            task_id: "test".to_string(),
            namespace: "test".to_string(),
            parent_content_id: "".to_string(),
            extraction_policy: "test".to_string(),
            extractor: "test".to_string(),
            output_to_index_table_mapping: HashMap::new(),
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
            index_tables: vec!["test".to_string()],
        };
        ingest_state.begin(payload.clone());
        let new_payload = ingest_state.ingest_metadata.clone().unwrap();
        assert_eq!(new_payload.task_id, payload.task_id);
        assert_eq!(new_payload.namespace, payload.namespace);
        assert_eq!(new_payload.parent_content_id, payload.parent_content_id);
        assert_eq!(new_payload.extraction_policy, payload.extraction_policy);
        assert_eq!(new_payload.extractor, payload.extractor);
        assert_eq!(
            new_payload.output_to_index_table_mapping,
            payload.output_to_index_table_mapping
        );
        assert_eq!(new_payload.executor_id, payload.executor_id);
        assert_eq!(new_payload.task_outcome, payload.task_outcome);

        ingest_state.begin_multipart_content().await.unwrap();

        let url = if let FrameState::Writing(s) = &ingest_state.frame_state {
            s.writer.url.clone()
        } else {
            panic!("frame_state should be Writing");
        };

        let payload = ContentFrame {
            bytes: vec![1, 2, 3],
        };
        ingest_state.write_content_frame(payload).await.unwrap();
        let payload = ContentFrame {
            bytes: vec![4, 5, 6],
        };
        ingest_state.write_content_frame(payload).await.unwrap();
        let payload = ContentFrame {
            bytes: vec![7, 8, 9],
        };
        ingest_state.write_content_frame(payload).await.unwrap();

        if let FrameState::Writing(s) = &ingest_state.frame_state {
            assert_eq!(s.file_size, 9);
        } else {
            panic!("frame_state should be Writing");
        }

        let payload = FinishContent {
            content_type: "test".to_string(),
            features: Vec::new(),
            labels: HashMap::new(),
        };

        ingest_state.finish_content(payload).await.unwrap();
        if let FrameState::Writing(_) = ingest_state.frame_state {
            panic!("frame_state should be New");
        }

        // compare file content with written content
        let content = ingest_state.state.content_reader.bytes(&url).await.unwrap();
        assert_eq!(content, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        coordinator.stop().await;
    }

    #[tokio::test]
    async fn test_embedding_metadata() {
        set_tracing();

        let state = new_endpoint_state().await.unwrap();
        let coordinator = TestCoordinator::new().await;

        let mut ingest_state = IngestExtractedContentState::new(state);
        let output_mappings: HashMap<String, String> = vec![
            ("name1".to_string(), "test_index1".to_string()),
            ("name2".to_string(), "test_index2".to_string()),
        ]
        .into_iter()
        .collect();

        let schema = indexify_internal_api::EmbeddingSchema {
            dim: 3,
            distance: "cosine".to_string(),
        };

        let _ = ingest_state
            .state
            .data_manager
            .vector_index_manager
            .drop_index("test_index1")
            .await;

        ingest_state
            .state
            .data_manager
            .vector_index_manager
            .create_index("test_index1", schema)
            .await
            .unwrap();

        let payload = BeginExtractedContentIngest {
            task_id: "test".to_string(),
            namespace: "test".to_string(),
            parent_content_id: "".to_string(),
            extraction_policy: "test".to_string(),
            extractor: "test".to_string(),
            output_to_index_table_mapping: output_mappings,
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
            index_tables: vec!["test_index1".to_string()],
        };

        ingest_state.begin(payload.clone());

        ingest_state.begin_multipart_content().await.unwrap();

        let mut payload = FinishContent {
            content_type: "test".to_string(),
            features: Vec::new(),
            labels: HashMap::new(),
        };

        payload.features.push(Feature {
            feature_type: FeatureType::Embedding,
            name: "name1".to_string(),
            data: json!({"values" : [1.0, 2.0, 3.0],
        "distance" : "cosine"}),
        });

        let metadata1 = json!({"key1" : "value1", "key2" : "value2"});

        payload.features.push(Feature {
            feature_type: FeatureType::Metadata,
            name: "name1".to_string(),
            data: metadata1.clone(),
        });

        let id = ingest_state.finish_content(payload).await.unwrap();
        if let FrameState::Writing(_) = ingest_state.frame_state {
            panic!("frame_state should be New");
        }

        // read entry for id from vector index
        let points = ingest_state
            .state
            .data_manager
            .vector_index_manager
            .get_points("test_index1", vec![id.clone()])
            .await
            .unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].content_id, id);
        assert_eq!(points[0].metadata, metadata1);

        // update metadata for content_id
        let metadata2 = json!({"key1" : "value3", "key2" : "value4"});
        let payload = ExtractedFeatures {
            content_id: id.clone(),
            features: vec![Feature {
                feature_type: FeatureType::Metadata,
                name: "name1".to_string(),
                data: metadata2.clone(),
            }],
        };

        ingest_state.write_features(payload).await.unwrap();

        // read entry for id from vector index
        let points = ingest_state
            .state
            .data_manager
            .vector_index_manager
            .get_points("test_index1", vec![id.clone()])
            .await
            .unwrap();

        // metadata should be replaced with new values
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].content_id, id);
        assert_eq!(points[0].metadata, metadata2);

        coordinator.stop().await;
    }

    // create content with metadata only then add embedding for it
    #[tokio::test]
    async fn test_embedding_existing_metadata() {
        set_tracing();

        let state = new_endpoint_state().await.unwrap();
        let coordinator = TestCoordinator::new().await;

        let mut ingest_state = IngestExtractedContentState::new(state);
        let output_mappings: HashMap<String, String> = vec![
            ("name1".to_string(), "test_index1".to_string()),
            ("name2".to_string(), "test_index2".to_string()),
        ]
        .into_iter()
        .collect();

        let schema = indexify_internal_api::EmbeddingSchema {
            dim: 3,
            distance: "cosine".to_string(),
        };

        let _ = ingest_state
            .state
            .data_manager
            .vector_index_manager
            .drop_index("test_index1")
            .await;

        ingest_state
            .state
            .data_manager
            .vector_index_manager
            .create_index("test_index1", schema)
            .await
            .unwrap();

        let payload = BeginExtractedContentIngest {
            task_id: "test".to_string(),
            namespace: "test".to_string(),
            parent_content_id: "".to_string(),
            extraction_policy: "test".to_string(),
            extractor: "test".to_string(),
            output_to_index_table_mapping: output_mappings,
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
            index_tables: vec!["test_index1".to_string()],
        };

        ingest_state.begin(payload.clone());

        ingest_state.begin_multipart_content().await.unwrap();

        let mut payload = FinishContent {
            content_type: "test".to_string(),
            features: Vec::new(),
            labels: HashMap::new(),
        };

        let metadata1 = json!({"key1" : "value1", "key2" : "value2"});

        // Add metadata only without embedding
        payload.features.push(Feature {
            feature_type: FeatureType::Metadata,
            name: "name1".to_string(),
            data: metadata1.clone(),
        });

        let id = ingest_state.finish_content(payload).await.unwrap();
        if let FrameState::Writing(_) = ingest_state.frame_state {
            panic!("frame_state should be New");
        }

        // add embedding for content_id
        let payload = ExtractedFeatures {
            content_id: id.clone(),
            features: vec![Feature {
                feature_type: FeatureType::Embedding,
                name: "name1".to_string(),
                data: json!({"values" : [1.0, 2.0, 3.0], "distance" :
    "cosine"}),
            }],
        };

        ingest_state.write_features(payload).await.unwrap();

        // read entry for id from vector index
        let points = ingest_state
            .state
            .data_manager
            .vector_index_manager
            .get_points("test_index1", vec![id.clone()])
            .await
            .unwrap();

        // embedding should be created with existing metadata
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].content_id, id);
        assert_eq!(points[0].metadata, metadata1);

        coordinator.stop().await;
    }
}
