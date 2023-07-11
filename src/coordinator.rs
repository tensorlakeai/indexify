use anyhow::anyhow;
use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use dashmap::{DashMap, DashSet};
use serde::{Deserialize, Serialize};
use tokio::signal;
use tracing::{error, info};

use crate::{
    api::IndexifyAPIError,
    persistence::{ExtractionEventPayload, Repository, Work, WorkState},
    ServerConfig,
};
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::SystemTime};

pub struct Executor {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SyncWorker {
    worker_id: String,
    available_models: Vec<String>,
    work_status: Vec<Work>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SyncWorkerResponse {
    content_to_process: Vec<Work>,
}
pub struct ExecutorState {
    // Executor ID -> Last Seen Timestamp
    executor_healthchecks: DashMap<String, u64>,

    // List of known executors
    executors: DashSet<String>,

    repository: Arc<Repository>,
}

impl ExecutorState {
    pub fn new(repository: Arc<Repository>) -> Self {
        Self {
            executor_healthchecks: DashMap::new(),
            executors: DashSet::new(),
            repository,
        }
    }

    pub async fn record_node(&self, worker: Executor) -> Result<(), anyhow::Error> {
        self.executor_healthchecks.insert(
            worker.id.clone(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        // Add the worker to our list of active workers
        self.executors.insert(worker.id);
        Ok(())
    }

    pub async fn distribute_work(&self) -> Result<(), anyhow::Error> {
        let events = self.repository.unprocessed_extraction_events().await?;
        let mut processed_events_for_repository = HashSet::new();
        for event in &events {
            info!("processing extraction event: {}", event.id);
            if processed_events_for_repository.contains(&event.repository_id) {
                if let Err(err) = self
                    .repository
                    .mark_extraction_event_as_processed(&event.id)
                    .await
                {
                    error!(
                        "unable to mark extraction event as processed: {}",
                        &err.to_string()
                    );
                    return Err(err);
                }
                continue;
            }
            let mut content: Option<&str> = None;
            match &event.payload {
                ExtractionEventPayload::SyncRepository { memory_session: _ } => {
                    processed_events_for_repository.insert(&event.repository_id);
                }
                ExtractionEventPayload::CreateContent { content_id } => {
                    content.replace(content_id.as_str());
                }
            }
            if let Err(err) = self.create_work(&event.repository_id, content).await {
                error!("unable to create work: {}", &err.to_string());
                return Err(err);
            }
            self.repository
                .mark_extraction_event_as_processed(&event.id)
                .await?;
        }
        Ok(())
    }

    pub async fn create_work(
        &self,
        repository_id: &str,
        content_id: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let worker_id = self
            .executors
            .iter()
            .next()
            .ok_or(anyhow!("no workers are available"))?;
        let extractors = self
            .repository
            .repository_by_name(repository_id)
            .await?
            .extractors;
        for extractor in &extractors {
            let content_list = self
                .repository
                .content_with_unapplied_extractor(repository_id, extractor, content_id)
                .await?;
            for content in content_list {
                info!("Creating work for content {}", &content.id);
                let work = Work::new(
                    &content.id,
                    repository_id,
                    &extractor.name,
                    Some(&worker_id),
                );
                self.repository.insert_work(&work).await?;
                self.repository
                    .mark_content_as_processed(&work.content_id, &work.extractor)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn update_work_state(
        &self,
        work_list: Vec<Work>,
        worker_id: &str,
    ) -> Result<(), anyhow::Error> {
        info!("updating work status by worker: {}", worker_id);
        for work in work_list.iter() {
            match work.work_state {
                WorkState::Completed | WorkState::Failed => {
                    if let Err(err) = self
                        .repository
                        .update_work_state(&work.id, work.work_state.clone())
                        .await
                    {
                        error!("unable to update work state: {}", err.to_string());
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn get_work_for_worker(&self, worker_id: &str) -> Result<Vec<Work>, anyhow::Error> {
        let work_list = self.repository.work_for_worker(worker_id).await?;

        Ok(work_list)
    }
}

pub struct CoordinatorWorker {
    addr: SocketAddr,
    node_state: Arc<ExecutorState>,
}

impl CoordinatorWorker {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_addr.parse()?;
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let node_state = Arc::new(ExecutorState::new(repository));
        info!("Coordinator listening on: {}", &config.coordinator_addr);
        Ok(Self { addr, node_state })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let app = Router::new().route("/", get(root)).route(
            "/sync_worker",
            post(sync_worker).with_state(self.node_state.clone()),
        );
        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }

    pub async fn run_extractors(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

async fn root() -> &'static str {
    "Indexify Coordinator"
}

#[axum_macros::debug_handler]
async fn sync_worker(
    State(node_state): State<Arc<ExecutorState>>,
    Json(worker): Json<SyncWorker>,
) -> Result<Json<SyncWorkerResponse>, IndexifyAPIError> {
    // Record the health check of the worker
    let worker_id = worker.worker_id.clone();
    let _ = node_state
        .record_node(Executor {
            id: worker_id.clone(),
        })
        .await;
    // Record the outcome of any work the worker has done
    node_state
        .update_work_state(worker.work_status, &worker_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Find more work for the worker
    let queued_work = node_state
        .get_work_for_worker(&worker.worker_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Respond
    Ok(Json(SyncWorkerResponse {
        content_to_process: queued_work,
    }))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
        },
        _ = terminate => {
        },
    }
    info!("signal received, shutting down server gracefully");
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        persistence::{self, DataRepository, ExtractorConfig, Text},
        test_util::db_utils::create_db,
        vectordbs::IndexDistance,
    };

    use super::*;

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_work() -> Result<(), anyhow::Error> {
        let db = create_db().await?;
        let repository = Arc::new(Repository::new_with_db(db));
        let node_state = ExecutorState::new(repository.clone());
        let repository_name = "test";

        // Create a repository
        repository
            .upsert_repository(DataRepository {
                name: repository_name.into(),
                data_connectors: vec![],
                metadata: HashMap::new(),
                extractors: vec![ExtractorConfig {
                    name: repository_name.into(),
                    extractor_type: persistence::ExtractorType::Embedding {
                        model: "model".into(),
                        text_splitter: crate::text_splitters::TextSplitterKind::NewLine,
                        distance: IndexDistance::Cosine,
                    },
                    filter: persistence::ExtractorFilter::ContentType {
                        content_type: persistence::ContentType::Text,
                    },
                }],
            })
            .await?;

        repository
            .add_content(
                repository_name,
                vec![
                    Text::from_text(repository_name, "hello", None, HashMap::new()),
                    Text::from_text(repository_name, "world", None, HashMap::new()),
                ],
                None,
            )
            .await?;

        // Insert a new worker and then create work
        node_state.executors.insert("test_worker".into());
        node_state.create_work(repository_name, None).await?;

        let work_list = node_state.get_work_for_worker("test_worker").await?;

        // Check amount of work queued for the worker
        assert_eq!(work_list.len(), 2);
        Ok(())
    }
}
