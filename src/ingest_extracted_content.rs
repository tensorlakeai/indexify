use axum_typed_websockets::{Message, WebSocket};
use crate::blob_storage::StoragePartWriter;
use crate::api::{*};
use indexify_proto::indexify_coordinator;
use crate::server::NamespaceEndpointState;
use tracing::info;
use anyhow::{anyhow, Result};
use crate::data_manager::DataManager;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
struct Writing {
    created_at: i64,
    file_name: String,
    file_size: u64,
    id: String, // DataManager ID
    writer: StoragePartWriter,
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
        info!("beginning extraction ingest for task: {}", payload.task_id);
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
        let id = DataManager::make_id(
            &metadata.namespace,
            &file_name,
            &Some(metadata.parent_content_id.clone()),
        );
        let writer = self
            .state
            .data_manager
            .blob_store_writer(&metadata.namespace, &file_name)
            .await?;
        self.frame_state = FrameState::Writing(Writing {
            created_at: ts as i64,
            file_name,
            file_size: 0,
            id,
            writer,
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
            FrameState::New => {
                return Err(anyhow!(
                    "received content frame without starting multipart content"
                ));
            }
            FrameState::Writing(frame_state) => {
                frame_state.file_size += payload.bytes.len() as u64;
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

    async fn add_content_feature(&mut self, payload: AddContentFeature) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received finished extraction ingest without header metadata"
            ));
        }
        info!(
            "received content feature for task: {}",
            self.ingest_metadata.as_ref().unwrap().task_id
        );
        match &self.frame_state {
            FrameState::New => {
                return Err(anyhow!(
                    "received content feature without starting multipart content"
                ));
            }
            FrameState::Writing(frame_state) => {
                self.state
                    .data_manager
                    .write_extracted_embedding(
                        &payload.name,
                        &payload.values,
                        &frame_state.id,
                        &self
                            .ingest_metadata
                            .as_ref()
                            .unwrap()
                            .output_to_index_table_mapping,
                    )
                    .await
            }
        }
    }

    async fn finish_content(&mut self, payload: FinishContent) -> Result<()> {
        if self.ingest_metadata.is_none() {
            return Err(anyhow!(
                "received finished extraction ingest without header metadata"
            ));
        }
        info!(
            "received finish multipart content for task: {}",
            self.ingest_metadata.as_ref().unwrap().task_id
        );
        match &mut self.frame_state {
            FrameState::New => {
                return Err(anyhow!(
                    "received finish content without any content frames"
                ));
            }
            FrameState::Writing(frame_state) => {
                frame_state.writer.writer.shutdown().await?;

                let metadata = self.ingest_metadata.as_ref().unwrap();
                let content_metadata = indexify_coordinator::ContentMetadata {
                    id: frame_state.id.clone(),
                    file_name: frame_state.file_name.clone(),
                    parent_id: metadata.parent_content_id.clone(),
                    namespace: metadata.namespace.clone(),
                    mime: payload.content_type,
                    size_bytes: frame_state.file_size,
                    storage_url: frame_state.writer.url.clone(),
                    labels: payload.labels,
                    source: metadata.extraction_policy.clone(),
                    created_at: frame_state.created_at,
                };
                self.state
                    .data_manager
                    .create_content_and_write_features(
                        &content_metadata,
                        self.ingest_metadata.as_ref().unwrap(),
                        payload.features,
                    )
                    .await?;
                self.frame_state = FrameState::New;
            }
        }
        Ok(())
    }

    async fn ensure_has_content_metadata(
        &mut self,
        content_id: String,
    ) -> Result<indexify_coordinator::ContentMetadata> {
        if self.content_metadata.is_none() {
            self.content_metadata = Some(
                self.state
                    .coordinator_client
                    .get()
                    .await?
                    .get_content_metadata(indexify_coordinator::GetContentMetadataRequest {
                        content_list: vec![content_id],
                    })
                    .await?
                    .into_inner()
                    .content_list
                    .first()
                    .ok_or(anyhow!("No content metadata found"))?
                    .clone(),
            );
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
            .write_extracted_features(
                &self.ingest_metadata.clone().unwrap().extractor,
                &self.ingest_metadata.clone().unwrap().extraction_policy,
                &content_meta,
                payload.features,
                &self
                    .ingest_metadata
                    .as_ref()
                    .unwrap()
                    .output_to_index_table_mapping,
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
                    IngestExtractedContent::MultipartContentFeature(payload) => {
                        if let Err(e) = self.add_content_feature(payload).await {
                            tracing::error!("Error handling content feature: {}", e);
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
                    }
                };
            }
        }
    }
}
