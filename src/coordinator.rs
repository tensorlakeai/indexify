use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::{
    signal,
    sync::mpsc::{self, Receiver, Sender},
};
use tracing::{error, info};

use crate::{
    api::IndexifyAPIError,
    persistence::{ExtractionEventPayload, ExtractorConfig, Repository, Work, WorkState},
    ServerConfig,
};
use indexmap::{IndexMap, IndexSet};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::SystemTime,
};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ExecutorInfo {
    pub id: String,
    pub last_seen: u64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SyncWorker {
    pub worker_id: String,
    pub available_extractors: Vec<ExtractorConfig>,
    pub work_status: Vec<Work>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ListExecutors {
    pub executors: Vec<ExecutorInfo>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ListExtractors {
    pub extractors: Vec<ExtractorConfig>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SyncWorkerResponse {
    pub content_to_process: Vec<Work>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CreateWork {
    pub repository_name: String,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateWorkResponse {}

pub struct Coordinator {
    // Executor ID -> Last Seen Timestamp
    executor_health_checks: Arc<RwLock<IndexMap<String, u64>>>,

    // List of known executors
    executors: Arc<RwLock<IndexSet<String>>>,

    repository: Arc<Repository>,

    tx: Sender<CreateWork>,
}

impl Coordinator {
    pub fn new(repository: Arc<Repository>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(32);

        let coordinator = Arc::new(Self {
            executor_health_checks: Arc::new(RwLock::new(IndexMap::new())),
            executors: Arc::new(RwLock::new(IndexSet::new())),
            repository,
            tx,
        });
        let coordinator_clone = coordinator.clone();
        tokio::spawn(async move {
            coordinator_clone.loop_for_work(rx).await.unwrap();
        });
        coordinator
    }

    pub async fn record_node(&self, worker: ExecutorInfo) -> Result<(), anyhow::Error> {
        self.executor_health_checks
            .write()
            .unwrap()
            .insert(worker.id.clone(), worker.last_seen);

        // Add the worker to our list of active workers
        self.executors.write().unwrap().insert(worker.id);
        Ok(())
    }

    pub async fn process_extraction_events(&self) -> Result<(), anyhow::Error> {
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
            let content = match &event.payload {
                ExtractionEventPayload::SyncRepository { memory_session: _ } => {
                    processed_events_for_repository.insert(&event.repository_id);
                    None
                }
                ExtractionEventPayload::CreateContent { content_id } => Some(content_id.as_str()),
            };
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

    pub async fn distribute_work(&self) -> Result<(), anyhow::Error> {
        let unallocated_work = self.repository.unallocated_work().await?;

        // work_id -> executor_id
        let mut work_assignment = HashMap::new();
        for work in unallocated_work {
            let rand_index = rand::random::<usize>() % self.executors.read().unwrap().len();
            let executor = self
                .executors
                .read()
                .unwrap()
                .get_index(rand_index)
                .cloned();
            if let Some(executor) = executor {
                info!("assigning work {} to executor {}", work.id, executor);
                work_assignment.insert(work.id.clone(), executor.clone());
            }
        }
        self.repository.assign_work(work_assignment).await?;
        Ok(())
    }

    pub async fn create_work(
        &self,
        repository_id: &str,
        content_id: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let extractor_bindings = self
            .repository
            .repository_by_name(repository_id)
            .await?
            .extractor_bindings;
        for extractor_binding in &extractor_bindings {
            let content_list = self
                .repository
                .content_with_unapplied_extractor(repository_id, extractor_binding, content_id)
                .await?;
            for content in content_list {
                info!("Creating work for content {}", &content.id);
                let work = Work::new(
                    &content.id,
                    repository_id,
                    &extractor_binding.index_name,
                    &extractor_binding.extractor_name,
                    &extractor_binding.input_params,
                    None,
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
        for work in work_list.iter() {
            info!(
                "updating work status by worker: {}, work id: {}, status: {}",
                worker_id, &work.id, &work.work_state
            );
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

    pub async fn record_extractors(
        &self,
        extractors: Vec<ExtractorConfig>,
    ) -> Result<(), anyhow::Error> {
        self.repository.record_extractors(extractors).await?;
        Ok(())
    }

    pub async fn get_work_for_worker(&self, worker_id: &str) -> Result<Vec<Work>, anyhow::Error> {
        let work_list = self.repository.work_for_worker(worker_id).await?;

        Ok(work_list)
    }

    async fn loop_for_work(&self, mut rx: Receiver<CreateWork>) -> Result<(), anyhow::Error> {
        info!("starting work distribution loop");
        loop {
            if (rx.recv().await).is_none() {
                info!("no work to process");
                return Ok(());
            }
            if let Err(err) = self.process_and_distribute_work().await {
                error!("unable to process and distribute work: {}", err.to_string());
            }
        }
    }

    pub async fn process_and_distribute_work(&self) -> Result<(), anyhow::Error> {
        info!("received work request, processing extraction events");
        self.process_extraction_events().await?;

        info!("doing distribution of work");
        self.distribute_work().await?;
        Ok(())
    }
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
}

impl CoordinatorServer {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_addr.parse()?;
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let coordinator = Coordinator::new(repository);
        info!("Coordinator listening on: {}", &config.coordinator_addr);
        Ok(Self { addr, coordinator })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let app = Router::new()
            .route("/", get(root))
            .route(
                "/sync_executor",
                post(sync_executor).with_state(self.coordinator.clone()),
            )
            .route(
                "/executors",
                get(list_executors).with_state(self.coordinator.clone()),
            )
            .route(
                "/create_work",
                post(create_work).with_state(self.coordinator.clone()),
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
async fn list_executors(
    State(coordinator): State<Arc<Coordinator>>,
) -> Result<Json<ListExecutors>, IndexifyAPIError> {
    let executors = coordinator.executor_health_checks.read().unwrap().clone();
    let executors = executors
        .into_iter()
        .map(|(id, last_seen)| ExecutorInfo { id, last_seen })
        .collect();
    Ok(Json(ListExecutors { executors }))
}

#[axum_macros::debug_handler]
async fn sync_executor(
    State(coordinator): State<Arc<Coordinator>>,
    Json(worker): Json<SyncWorker>,
) -> Result<Json<SyncWorkerResponse>, IndexifyAPIError> {
    // Record the health check of the worker
    let worker_id = worker.worker_id.clone();
    let _ = coordinator
        .record_node(ExecutorInfo {
            id: worker_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        })
        .await;
    // Record the outcome of any work the worker has done
    coordinator
        .update_work_state(worker.work_status, &worker_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Record the extractors available on the executor
    coordinator
        .record_extractors(worker.available_extractors)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Find more work for the worker
    let queued_work = coordinator
        .get_work_for_worker(&worker.worker_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Respond
    Ok(Json(SyncWorkerResponse {
        content_to_process: queued_work,
    }))
}

#[axum_macros::debug_handler]
async fn create_work(
    State(coordinator): State<Arc<Coordinator>>,
    Json(create_work): Json<CreateWork>,
) -> Result<Json<CreateWorkResponse>, IndexifyAPIError> {
    if let Err(err) = coordinator.tx.try_send(create_work) {
        error!("unable to send create work request: {}", err.to_string());
    }
    Ok(Json(CreateWorkResponse {}))
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
    use crate::{
        persistence::ExtractorBinding,
        test_util::{
            self,
            db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
        },
    };
    use std::collections::HashMap;

    use crate::{
        data_repository_manager::DataRepositoryManager,
        persistence::{self, DataRepository, Text},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_work() -> Result<(), anyhow::Error> {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (vector_index_manager, extractor_executor, coordinator) =
            test_util::db_utils::create_index_manager(db.clone()).await;
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), vector_index_manager);

        // Create a repository
        repository_manager
            .create(&DataRepository {
                name: DEFAULT_TEST_REPOSITORY.into(),
                data_connectors: vec![],
                metadata: HashMap::new(),
                extractor_bindings: vec![ExtractorBinding {
                    extractor_name: DEFAULT_TEST_EXTRACTOR.into(),
                    index_name: DEFAULT_TEST_EXTRACTOR.into(),
                    filter: persistence::ExtractorFilter::ContentType {
                        content_type: persistence::ContentType::Text,
                    },
                    input_params: serde_json::json!({}),
                }],
            })
            .await?;

        repository_manager
            .add_texts(
                DEFAULT_TEST_REPOSITORY,
                vec![
                    Text::from_text(DEFAULT_TEST_REPOSITORY, "hello", None, HashMap::new()),
                    Text::from_text(DEFAULT_TEST_REPOSITORY, "world", None, HashMap::new()),
                ],
                None,
            )
            .await?;

        // Insert a new worker and then create work
        coordinator.process_and_distribute_work().await.unwrap();

        let work_list = coordinator
            .get_work_for_worker(&extractor_executor.get_executor_info().id)
            .await?;

        // Check amount of work queued for the worker
        assert_eq!(work_list.len(), 2);
        Ok(())
    }
}
