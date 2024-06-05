use std::collections::HashMap;

use anyhow::{anyhow, Result};
use axum::extract::ws;
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

// Web socket status codes start with 1000, 1000 and 1001 is success.
const WS_PROTOCOL_ERROR: u16 = 1002;

fn msg_type_str(msg: &IngestExtractedContent) -> &'static str {
    match msg {
        IngestExtractedContent::BeginExtractedContentIngest(_) => "BeginExtractedContentIngest",
        IngestExtractedContent::BeginMultipartContent(_) => "BeginMultipartContent",
        IngestExtractedContent::MultipartContentFrame(_) => "MultipartContentFrame",
        IngestExtractedContent::FinishMultipartContent(_) => "FinishMultipartContent",
        IngestExtractedContent::ExtractedFeatures(_) => "ExtractedFeatures",
        IngestExtractedContent::FinishExtractedContentIngest(_) => "FinishExtractedContentIngest",
    }
}

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
}

#[derive(Debug)]
enum FrameState {
    New,
    Writing(Writing),
}

struct ContentStateWriting {
    ingest_metadata: BeginExtractedContentIngest,
    task: indexify_coordinator::Task,
    root_content_metadata: Option<indexify_internal_api::ContentMetadata>,
    frame_state: FrameState,
}

impl ContentStateWriting {
    fn new(
        ingest_metadata: BeginExtractedContentIngest,
        task: indexify_coordinator::Task,
        root_content: Option<indexify_coordinator::ContentMetadata>,
    ) -> Result<Self> {
        if task.content_metadata.is_none() {
            return Err(anyhow!("task does not have content metadata"));
        }
        let root_content = root_content.map(|c| c.into());
        Ok(Self {
            ingest_metadata,
            task,
            root_content_metadata: root_content,
            frame_state: FrameState::New,
        })
    }

    fn content_metadata(&self) -> &indexify_coordinator::ContentMetadata {
        self.task.content_metadata.as_ref().unwrap()
    }

    async fn start_content(&mut self, state: &NamespaceEndpointState) -> Result<()> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let file_name = DataManager::make_file_name(None);
        let writer = DataManager::blob_store_writer(
            &state.data_manager,
            &self.content_metadata().namespace,
            &file_name,
        )
        .await?;
        self.frame_state = FrameState::Writing(Writing {
            created_at: ts as i64,
            file_name,
            file_size: 0,
            writer,
            hasher: Sha256::new(),
        });
        Ok(())
    }

    async fn begin_multipart_content(&mut self, state: &NamespaceEndpointState) -> Result<()> {
        match &self.frame_state {
            FrameState::New => self.start_content(state).await,
            FrameState::Writing(_) => Err(anyhow!(
                "received begin content without finishing previous content"
            )),
        }
    }

    async fn write_content_frame(&mut self, payload: ContentFrame) -> Result<()> {
        match &mut self.frame_state {
            FrameState::New => Err(anyhow!(
                "received content frame without starting multipart content"
            )),
            FrameState::Writing(frame_state) => {
                frame_state.file_size += payload.bytes.len() as u64;
                frame_state.hasher.update(&payload.bytes);
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

    async fn finish_content(
        &mut self,
        state: &NamespaceEndpointState,
        payload: FinishContent,
    ) -> Result<String> {
        let mut labels = self.content_metadata().labels.clone();
        let parent_id = self.content_metadata().id.clone();
        match &mut self.frame_state {
            FrameState::New => Err(anyhow!(
                "received finish content without any content frames"
            )),
            FrameState::Writing(frame_state) => {
                frame_state.writer.writer.shutdown().await?;
                labels.extend(payload.labels);
                let hash_result = frame_state.hasher.clone().finalize();
                let content_hash = format!("{:x}", hash_result);
                let id = DataManager::make_id();
                let root_content_metadata = self
                    .root_content_metadata
                    .clone()
                    .unwrap_or(self.task.content_metadata.clone().unwrap().into());
                let extraction_policy = state
                    .data_manager
                    .get_extraction_policy(&self.task.extraction_policy_id)
                    .await?;
                let content_metadata = indexify_coordinator::ContentMetadata {
                    id: id.clone(),
                    file_name: frame_state.file_name.clone(),
                    parent_id,
                    root_content_id: root_content_metadata.id.id.clone(),
                    namespace: self.task.namespace.clone(),
                    mime: payload.content_type,
                    size_bytes: frame_state.file_size,
                    storage_url: frame_state.writer.url.clone(),
                    labels,
                    source: extraction_policy.name,
                    created_at: frame_state.created_at,
                    hash: content_hash,
                    extraction_policy_ids: HashMap::new(),
                    extraction_graph_names: vec![extraction_policy.graph_name],
                };
                state
                    .data_manager
                    .create_content_and_write_features(
                        &content_metadata,
                        Some(root_content_metadata.clone()),
                        &self.task.extractor,
                        &self.task.extraction_graph_name,
                        payload.features,
                        &self.task.output_index_mapping,
                    )
                    .await?;
                state.metrics.node_content_extracted.add(1, &[]);
                state
                    .metrics
                    .node_content_bytes_extracted
                    .add(frame_state.file_size, &[]);
                self.frame_state = FrameState::New;
                Ok(id)
            }
        }
    }

    async fn write_features(
        &mut self,
        state: &NamespaceEndpointState,
        payload: ExtractedFeatures,
    ) -> Result<()> {
        state
            .data_manager
            .write_existing_content_features(
                &self.task.extractor,
                &self.task.extraction_graph_name,
                self.content_metadata(),
                self.root_content_metadata.clone(),
                payload.features,
                &self.task.output_index_mapping,
                &self.task.index_tables,
            )
            .await
    }
}

enum ContentState {
    Init,
    Writing(ContentStateWriting),
}

pub struct IngestExtractedContentState {
    state: NamespaceEndpointState,
    content_state: ContentState,
}

impl IngestExtractedContentState {
    pub fn new(state: NamespaceEndpointState) -> Self {
        Self {
            state,
            content_state: ContentState::Init,
        }
    }

    async fn begin(&mut self, payload: BeginExtractedContentIngest) -> Result<()> {
        info!("beginning extraction ingest for task: {}", payload.task_id);
        let (task, root_content) = self
            .state
            .coordinator_client
            .get_metadata_for_ingestion(&payload.task_id)
            .await?;
        let task = task.ok_or_else(|| anyhow!("task {} not found", payload.task_id))?;

        self.content_state =
            ContentState::Writing(ContentStateWriting::new(payload, task, root_content)?);
        Ok(())
    }

    async fn begin_multipart_content(&mut self) -> Result<()> {
        match &mut self.content_state {
            ContentState::Writing(s) => s.begin_multipart_content(&self.state).await,
            ContentState::Init => Err(anyhow!("received begin content without header metadata")),
        }
    }

    async fn write_content_frame(&mut self, payload: ContentFrame) -> Result<()> {
        match &mut self.content_state {
            ContentState::Writing(s) => s.write_content_frame(payload).await,
            ContentState::Init => Err(anyhow!("received content frame without header metadata")),
        }
    }

    async fn finish_content(&mut self, payload: FinishContent) -> Result<String> {
        match &mut self.content_state {
            ContentState::Writing(s) => s.finish_content(&self.state, payload).await,
            ContentState::Init => Err(anyhow!("received finish content without header metadata")),
        }
    }

    async fn write_features(&mut self, payload: ExtractedFeatures) -> Result<()> {
        match &mut self.content_state {
            ContentState::Writing(s) => s.write_features(&self.state, payload).await,
            ContentState::Init => Err(anyhow!(
                "received extracted features without header metadata"
            )),
        }
    }

    async fn finish(&mut self) -> Result<()> {
        match &mut self.content_state {
            ContentState::Writing(s) => {
                self.state
                    .data_manager
                    .finish_extracted_content_write(s.ingest_metadata.clone())
                    .await?;
                self.content_state = ContentState::Init;
                Ok(())
            }
            ContentState::Init => Err(anyhow!(
                "received finished extraction ingest without header metadata"
            )),
        }
    }

    pub async fn run(
        mut self,
        mut socket: WebSocket<IngestExtractedContentResponse, IngestExtractedContent>,
    ) {
        while let Some(msg) = socket.recv().await {
            match msg {
                Ok(Message::Item(msg)) => {
                    let msg_type = msg_type_str(&msg);
                    let res = match msg {
                        IngestExtractedContent::BeginExtractedContentIngest(payload) => {
                            self.begin(payload).await
                        }
                        IngestExtractedContent::BeginMultipartContent(_) => {
                            self.begin_multipart_content().await
                        }
                        IngestExtractedContent::MultipartContentFrame(payload) => {
                            self.write_content_frame(payload).await
                        }
                        IngestExtractedContent::FinishMultipartContent(payload) => {
                            match self.finish_content(payload).await {
                                Ok(_) => Ok(()),
                                Err(e) => Err(e),
                            }
                        }
                        IngestExtractedContent::ExtractedFeatures(payload) => {
                            self.write_features(payload).await
                        }
                        IngestExtractedContent::FinishExtractedContentIngest(_) => {
                            let res = self.finish().await;
                            let msg = match res {
                                Ok(_) => IngestExtractedContentResponse::Success,
                                Err(e) => IngestExtractedContentResponse::Error(e.to_string()),
                            };
                            let _ = socket.send(Message::Item(msg)).await;
                            break;
                        }
                    };
                    if let Err(e) = res {
                        tracing::error!("Error handling message {:?} {:?}", msg_type, e);
                        let _ = socket
                            .send(Message::Close(Some(ws::CloseFrame {
                                code: WS_PROTOCOL_ERROR,
                                reason: e.to_string().into(),
                            })))
                            .await;
                        break;
                    }
                }
                Ok(Message::Close(_)) => {
                    break;
                }
                Ok(Message::Ping(data)) => {
                    let _ = socket.send(Message::Pong(data)).await;
                }
                Ok(Message::Pong(_)) => {}
                Err(err) => {
                    tracing::error!("error receiving message: {:?}", err);
                    let _ = socket
                        .send(Message::Close(Some(ws::CloseFrame {
                            code: WS_PROTOCOL_ERROR,
                            reason: "invalid message".into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use anyhow::Result;
    use indexify_internal_api::{
        ContentMetadata,
        ExtractedEmbeddings,
        ExtractionGraph,
        ExtractionPolicy,
        ExtractionPolicyContentSource,
        ExtractorDescription,
        StructuredDataSchema,
        Task,
        TaskOutcome,
    };
    use serde_json::json;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::{
        blob_storage::{BlobStorage, ContentReader},
        coordinator::Coordinator,
        coordinator_client::CoordinatorClient,
        data_manager::DataManager,
        metadata_storage::{self, MetadataReaderTS, MetadataStorageTS},
        metrics,
        server::{NamespaceEndpointState, Server},
        server_config::{IndexStoreKind, ServerConfig},
        test_util::db_utils::{
            create_metadata,
            create_test_extraction_graph,
            create_test_extraction_graph_with_children,
            mock_extractor,
            perform_all_tasks,
            test_mock_content_metadata,
            wait_changes_processed,
            wait_gc_tasks_completed,
            Parent::{Child, Root},
            DEFAULT_TEST_NAMESPACE,
        },
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

    fn make_test_task(
        task_id: &str,
        content_metadata: &ContentMetadata,
        extraction_policy: ExtractionPolicy,
    ) -> Task {
        let mut task = Task::new(task_id, content_metadata, extraction_policy);
        task.output_index_table_mapping = vec![
            ("name1".to_string(), "test_index1".to_string()),
            ("name2".to_string(), "test_index2".to_string()),
        ]
        .into_iter()
        .collect();
        task.extractor = "test".to_string();
        task.index_tables = vec!["test_index1".to_string()];
        task
    }

    struct TestCoordinator {
        coordinator: Arc<Coordinator>,
        handle: JoinHandle<()>,
    }

    impl TestCoordinator {
        async fn stop(self) {
            self.handle.abort();
            let _ = self.handle.await;
        }

        async fn new() -> TestCoordinator {
            let config = make_test_config();
            let _ = std::fs::remove_dir_all(config.state_store.clone().path.unwrap());
            let registry = Arc::new(crate::metrics::init_provider());
            let coordinator_server = crate::coordinator_service::CoordinatorServer::new(
                Arc::new(config.clone()),
                registry,
            )
            .await
            .expect("failed to create coordinator server");

            let coordinator = coordinator_server.get_coordinator();

            let handle = tokio::spawn(async move {
                coordinator_server.run().await.unwrap();
            });
            // wait until able to connect to coordinator
            loop {
                if let Ok(_) = CoordinatorClient::new(Arc::new(config.clone())).get().await {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            let test_coordinator = TestCoordinator {
                handle,
                coordinator,
            };
            let extractor = mock_extractor();
            test_coordinator
                .create_extractor(extractor.clone())
                .await
                .unwrap();
            let eg = create_test_extraction_graph(
                "extraction_graph_name",
                vec!["extraction_policy_name"],
            );
            test_coordinator
                .create_extraction_graph(eg.clone())
                .await
                .unwrap();
            let content_metadata = test_mock_content_metadata("1", "1", &eg.name);
            test_coordinator
                .create_content(content_metadata.clone().into())
                .await
                .unwrap();
            let internal_content_metadata = test_coordinator
                .get_internal_content(content_metadata.id.id)
                .await;
            test_coordinator
                .create_task(make_test_task(
                    "test",
                    &internal_content_metadata,
                    eg.extraction_policies[0].clone(),
                ))
                .await
                .unwrap();
            test_coordinator
        }

        pub async fn create_extractor(&self, extractor: ExtractorDescription) -> Result<()> {
            self.coordinator
                .shared_state
                .register_executor("localhost:8950", "executor_id", vec![extractor])
                .await?;
            Ok(())
        }

        pub async fn create_extraction_graph(
            &self,
            extraction_graph: ExtractionGraph,
        ) -> Result<()> {
            self.coordinator
                .shared_state
                .create_extraction_graph(
                    extraction_graph,
                    StructuredDataSchema::default(),
                    Vec::new(),
                )
                .await?;
            Ok(())
        }

        pub async fn create_content(
            &self,
            content: indexify_coordinator::ContentMetadata,
        ) -> Result<()> {
            self.coordinator
                .create_content_metadata(vec![content.into()])
                .await
                .unwrap();

            Ok(())
        }

        pub async fn get_internal_content(
            &self,
            id: String,
        ) -> indexify_internal_api::ContentMetadata {
            self.coordinator
                .shared_state
                .get_content_metadata_batch(vec![id])
                .await
                .unwrap()
                .first()
                .unwrap()
                .clone()
        }

        pub async fn create_task(&self, task: Task) -> Result<()> {
            let mut watcher = self.coordinator.shared_state.get_state_change_watcher();
            let state_change_id = watcher.borrow_and_update();

            self.coordinator
                .shared_state
                .create_tasks(vec![task], *state_change_id)
                .await
                .unwrap();
            Ok(())
        }
    }

    async fn new_endpoint_state() -> Result<NamespaceEndpointState> {
        let config = make_test_config();
        let vector_db = vectordbs::create_vectordb(config.index_config.clone()).await?;
        let coordinator_client = Arc::new(CoordinatorClient::new(Arc::new(config.clone())));
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
            content_reader: Arc::new(ContentReader::new(Arc::new(config.clone()))),
            registry: Arc::new(metrics::init_provider()),
            metrics: Arc::new(metrics::server::Metrics::new()),
        };
        Ok(namespace_endpoint_state)
    }

    #[tokio::test]
    async fn test_new() {
        let state = new_endpoint_state().await.unwrap();
        let ingest_state = IngestExtractedContentState::new(state);
        assert!(matches!(ingest_state.content_state, ContentState::Init));
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
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
        };
        ingest_state.begin(payload.clone()).await.unwrap();
        let new_payload = if let ContentState::Writing(s) = &ingest_state.content_state {
            s.ingest_metadata.clone()
        } else {
            panic!("content_state should be Writing");
        };
        assert_eq!(new_payload.task_id, payload.task_id);
        assert_eq!(new_payload.executor_id, payload.executor_id);
        assert_eq!(new_payload.task_outcome, payload.task_outcome);

        ingest_state.begin_multipart_content().await.unwrap();
        let url = if let ContentState::Writing(s) = &ingest_state.content_state {
            if let FrameState::Writing(w) = &s.frame_state {
                w.writer.url.clone()
            } else {
                panic!("frame_state should be Writing");
            }
        } else {
            panic!("content_state should be Writing");
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

        if let ContentState::Writing(s) = &ingest_state.content_state {
            if let FrameState::Writing(s) = &s.frame_state {
                assert_eq!(s.file_size, 9);
            } else {
                panic!("frame_state should be Writing");
            }
        } else {
            panic!("content_state should be Writing");
        };

        let payload = FinishContent {
            content_type: "test".to_string(),
            features: Vec::new(),
            labels: HashMap::new(),
        };

        ingest_state.finish_content(payload).await.unwrap();
        if let ContentState::Writing(s) = &ingest_state.content_state {
            if !matches!(s.frame_state, FrameState::New) {
                panic!("frame_state should be New");
            }
        } else {
            panic!("content_state should be Writing");
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

        let mut ingest_state = IngestExtractedContentState::new(state.clone());

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
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
        };

        ingest_state.begin(payload.clone()).await.unwrap();

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
        let metadata1_out = create_metadata(vec![("key1", "value1"), ("key2", "value2")]);

        payload.features.push(Feature {
            feature_type: FeatureType::Metadata,
            name: "name1".to_string(),
            data: metadata1.clone(),
        });

        let id = ingest_state.finish_content(payload).await.unwrap();

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
        assert_eq!(points[0].metadata, metadata1_out);

        let extraction_policy = ExtractionPolicy {
            id: "extraction_policy_id".to_string(),
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            name: "extraction_policy_name".to_string(),
            extractor: "extractor_name".to_string(),
            graph_name: "extraction_graph_id".to_string(),
            filters: HashMap::new(),
            content_source: ExtractionPolicyContentSource::Ingestion,
            output_table_mapping: vec![("test_output".to_string(), "test_table".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let content_metadata = coordinator
            .coordinator
            .shared_state
            .get_content_metadata_batch(vec![id.clone()])
            .await
            .unwrap();
        coordinator
            .create_task(make_test_task(
                "test_1",
                content_metadata.first().unwrap(),
                extraction_policy,
            ))
            .await
            .unwrap();
        assert_eq!(
            content_metadata.first().unwrap().root_content_id,
            Some("1".to_string())
        );

        let payload = BeginExtractedContentIngest {
            task_id: "test_1".to_string(),
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
        };

        let mut ingest_state = IngestExtractedContentState::new(state.clone());
        ingest_state.begin(payload.clone()).await.unwrap();

        // update metadata for content_id
        let metadata2 = json!({"key1" : "value3", "key2" : "value4"});
        let metadata2_out = create_metadata(vec![("key1", "value3"), ("key2", "value4")]);
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
        assert_eq!(points[0].metadata, metadata2_out);

        coordinator.stop().await;
    }

    // create content with metadata only then add embedding for it
    #[tokio::test]
    async fn test_embedding_existing_metadata() {
        set_tracing();

        let state = new_endpoint_state().await.unwrap();
        let coordinator = TestCoordinator::new().await;

        let mut ingest_state = IngestExtractedContentState::new(state.clone());

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
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
        };

        ingest_state.begin(payload.clone()).await.unwrap();

        ingest_state.begin_multipart_content().await.unwrap();

        let mut payload = FinishContent {
            content_type: "test".to_string(),
            features: Vec::new(),
            labels: HashMap::new(),
        };

        let metadata1 = json!({"key1" : "value1", "key2" : "value2"});
        let metadata1_out = create_metadata(vec![("key1", "value1"), ("key2", "value2")]);

        // Add metadata only without embedding
        payload.features.push(Feature {
            feature_type: FeatureType::Metadata,
            name: "name1".to_string(),
            data: metadata1.clone(),
        });

        let id = ingest_state.finish_content(payload).await.unwrap();

        let extraction_policy = ExtractionPolicy {
            id: "extraction_policy_id".to_string(),
            namespace: "test".to_string(),
            name: "extraction_policy_name".to_string(),
            extractor: "extractor_name".to_string(),
            graph_name: "extraction_graph_id".to_string(),
            filters: HashMap::new(),
            content_source: ExtractionPolicyContentSource::Ingestion,
            output_table_mapping: vec![("test_output".to_string(), "test_table".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let content_metadata = coordinator
            .coordinator
            .shared_state
            .get_content_metadata_batch(vec![id.clone()])
            .await
            .unwrap();
        coordinator
            .create_task(make_test_task(
                "test_1",
                content_metadata.first().unwrap(),
                extraction_policy,
            ))
            .await
            .unwrap();

        let payload = BeginExtractedContentIngest {
            task_id: "test_1".to_string(),
            executor_id: "test".to_string(),
            task_outcome: TaskOutcome::Success,
        };

        let mut ingest_state = IngestExtractedContentState::new(state.clone());
        ingest_state.begin(payload.clone()).await.unwrap();

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
        assert_eq!(points[0].metadata, metadata1_out);

        coordinator.stop().await;
    }

    #[tokio::test]
    async fn test_labels_update() -> Result<()> {
        set_tracing();
        let state = new_endpoint_state().await.unwrap();

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = Server::new(Arc::new(make_test_config()))?;
        let server_id = "1";

        let test_coordinator = TestCoordinator::new().await;
        let coordinator = &test_coordinator.coordinator;

        server.start_gc_tasks_stream(
            state.coordinator_client.clone(),
            server_id,
            state.data_manager.clone(),
            shutdown_rx,
        );

        let _ = state
            .data_manager
            .vector_index_manager
            .drop_index("test_table")
            .await;

        let schema = indexify_internal_api::EmbeddingSchema {
            dim: 3,
            distance: "cosine".to_string(),
        };
        state
            .data_manager
            .vector_index_manager
            .create_index("test_table", schema)
            .await?;

        let _executor_id_1 = "test_executor_id_1";
        let extractor_1 = mock_extractor();
        coordinator
            .register_executor(
                "localhost:8956",
                "test_executor_id",
                vec![extractor_1.clone()],
            )
            .await?;

        //  Create an extraction graph
        let eg = create_test_extraction_graph_with_children(
            "test_extraction_graph",
            vec![
                "test_extraction_policy_1",
                "test_extraction_policy_2",
                "test_extraction_policy_3",
                "test_extraction_policy_4",
                "test_extraction_policy_5",
                "test_extraction_policy_6",
            ],
            &[Root, Child(0), Child(0), Child(1), Child(3), Child(3)],
        );
        coordinator.create_extraction_graph(eg.clone()).await?;
        coordinator.run_scheduler().await?;

        let parent_content = test_mock_content_metadata("200", "", &eg.name);
        let create_res = coordinator
            .create_content_metadata(vec![parent_content.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        coordinator.run_scheduler().await?;

        let mut child_id = 100;
        perform_all_tasks(&coordinator, "test_executor_id_1", &mut child_id).await?;

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        for content in &tree {
            if content.extraction_policy_ids.is_empty() {
                continue;
            }
            let embedding = ExtractedEmbeddings {
                content_id: content.id.id.clone(),
                embedding: vec![1.0, 2.0, 3.0],
                metadata: HashMap::new(),
                content_metadata: content.clone(),
                root_content_metadata: None,
            };
            state
                .data_manager
                .vector_index_manager
                .add_embedding("test_table", vec![embedding])
                .await?;
        }

        let labels: HashMap<_, _> = [("key1", "value1"), ("key2", "value2")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        state
            .data_manager
            .update_labels(DEFAULT_TEST_NAMESPACE, "200", labels.clone())
            .await?;

        let content = state
            .data_manager
            .get_content_metadata(DEFAULT_TEST_NAMESPACE, vec!["200".to_string()])
            .await?
            .first()
            .cloned()
            .unwrap();

        assert_eq!(content.labels, labels);

        let content = state
            .data_manager
            .get_content_metadata(DEFAULT_TEST_NAMESPACE, vec!["101".to_string()])
            .await?
            .first()
            .cloned()
            .unwrap();

        assert_eq!(content.labels, labels);

        wait_changes_processed(coordinator).await?;
        wait_gc_tasks_completed(coordinator).await?;

        let points = state
            .data_manager
            .vector_index_manager
            .get_points("test_table", vec!["200".to_string(), "101".to_string()])
            .await?;

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].metadata, create_metadata(&labels));
        assert_eq!(points[1].metadata, create_metadata(&labels));

        let tree = coordinator
            .shared_state
            .get_content_tree_metadata(&parent_content.id.id)?;
        assert_eq!(tree.len(), 7);

        // Update labels again and check if values change.
        let labels: HashMap<_, _> = [("key1", "value3"), ("key2", "value4")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        state
            .data_manager
            .update_labels(DEFAULT_TEST_NAMESPACE, "200", labels.clone())
            .await?;

        let content = state
            .data_manager
            .get_content_metadata(DEFAULT_TEST_NAMESPACE, vec!["200".to_string()])
            .await?
            .first()
            .cloned()
            .unwrap();

        assert_eq!(content.labels, labels);

        let content = state
            .data_manager
            .get_content_metadata(DEFAULT_TEST_NAMESPACE, vec!["101".to_string()])
            .await?
            .first()
            .cloned()
            .unwrap();

        assert_eq!(content.labels, labels);

        wait_changes_processed(coordinator).await?;
        wait_gc_tasks_completed(coordinator).await?;

        let points = state
            .data_manager
            .vector_index_manager
            .get_points("test_table", vec!["200".to_string(), "101".to_string()])
            .await?;

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].metadata, create_metadata(&labels));
        assert_eq!(points[1].metadata, create_metadata(&labels));

        shutdown_tx.send(true)?;

        test_coordinator.stop().await;

        Ok(())
    }
}
