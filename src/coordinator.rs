use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};
use tokio::{
    signal,
    sync::mpsc::{self, Receiver, Sender},
};
use tracing::{error, info};

use crate::{
    api::IndexifyAPIError,
    internal_api::{
        CreateWork, CreateWorkResponse, EmbedQueryRequest, EmbedQueryResponse, ExecutorInfo,
        ListExecutors, SyncExecutor, SyncWorkerResponse,
    },
    persistence::{
        ExtractionEventPayload, ExtractorBinding, ExtractorConfig, Repository, Work, WorkState,
    },
    server_config::CoordinatorConfig,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::SystemTime,
};

#[derive(Debug)]
pub struct Coordinator {
    // Executor ID -> Last Seen Timestamp
    executor_health_checks: Arc<RwLock<HashMap<String, u64>>>,

    executors: Arc<RwLock<HashMap<String, ExecutorInfo>>>,

    // Extractor Name -> [Executor ID]
    extractors_table: Arc<RwLock<HashMap<String, Vec<String>>>>,

    repository: Arc<Repository>,

    tx: Sender<CreateWork>,
}

impl Coordinator {
    pub fn new(repository: Arc<Repository>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(32);

        let coordinator = Arc::new(Self {
            executor_health_checks: Arc::new(RwLock::new(HashMap::new())),
            executors: Arc::new(RwLock::new(HashMap::new())),
            extractors_table: Arc::new(RwLock::new(HashMap::new())),
            repository,
            tx,
        });
        let coordinator_clone = coordinator.clone();
        tokio::spawn(async move {
            coordinator_clone.loop_for_work(rx).await.unwrap();
        });
        coordinator
    }

    #[tracing::instrument(skip(self))]
    pub async fn record_executor(&self, worker: ExecutorInfo) -> Result<(), anyhow::Error> {
        // First see if the executor is already in the table
        let is_new_executor = self
            .executor_health_checks
            .read()
            .unwrap()
            .get(&worker.id)
            .is_none();
        self.executor_health_checks
            .write()
            .unwrap()
            .insert(worker.id.clone(), worker.last_seen);
        if is_new_executor {
            info!("recording new executor: {}", &worker.id);
            self.executors
                .write()
                .unwrap()
                .insert(worker.id.clone(), worker.clone());
            let mut extractors_table = self.extractors_table.write().unwrap();
            let executors = extractors_table
                .entry(worker.extractor.name.clone())
                .or_default();
            executors.push(worker.id.clone());
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn process_extraction_events(&self) -> Result<(), anyhow::Error> {
        let events = self.repository.unprocessed_extraction_events().await?;
        for event in &events {
            info!("processing extraction event: {}", event.id);
            match &event.payload {
                ExtractionEventPayload::ExtractorBindingAdded { repository, id } => {
                    let binding = self.repository.binding_by_id(repository, id).await?;
                    self.generate_work_for_extractor_bindings(repository, &binding)
                        .await?;
                }
                ExtractionEventPayload::CreateContent { content_id } => {
                    if let Err(err) = self
                        .create_work(&event.repository_id, Some(content_id))
                        .await
                    {
                        error!("unable to create work: {}", &err.to_string());
                        return Err(err);
                    }
                }
            };

            self.repository
                .mark_extraction_event_as_processed(&event.id)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn generate_work_for_extractor_bindings(
        &self,
        repository: &str,
        extractor_binding: &ExtractorBinding,
    ) -> Result<(), anyhow::Error> {
        let content_list = self
            .repository
            .content_with_unapplied_extractor(repository, extractor_binding, None)
            .await?;
        for content in content_list {
            self.create_work(repository, Some(&content.id)).await?;
        }
        Ok(())
    }

    #[tracing::instrument]
    pub async fn distribute_work(&self) -> Result<(), anyhow::Error> {
        let unallocated_work = self.repository.unallocated_work().await?;

        // work_id -> executor_id
        let mut work_assignment = HashMap::new();
        for work in unallocated_work {
            let extractor_table = self.extractors_table.read().unwrap();
            let executors = extractor_table.get(&work.extractor).ok_or(anyhow::anyhow!(
                "no executors for extractor: {}",
                work.extractor
            ))?;
            let rand_index = rand::random::<usize>() % executors.len();
            if !executors.is_empty() {
                let executor_id = executors[rand_index].clone();
                work_assignment.insert(work.id.clone(), executor_id);
            }
        }
        self.repository.assign_work(work_assignment).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
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
                info!(
                    "Creating work for repository: {}, content: {}, extractor: {}, index: {}",
                    &repository_id,
                    &content.id,
                    &extractor_binding.extractor_name,
                    &extractor_binding.index_name
                );
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
                    .mark_content_as_processed(&work.content_id, &extractor_binding.id)
                    .await?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
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

    #[tracing::instrument(skip(self))]
    pub async fn record_extractor(&self, extractor: ExtractorConfig) -> Result<(), anyhow::Error> {
        self.repository.record_extractors(vec![extractor]).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_work_for_worker(&self, worker_id: &str) -> Result<Vec<Work>, anyhow::Error> {
        let work_list = self.repository.work_for_worker(worker_id).await?;

        Ok(work_list)
    }

    #[tracing::instrument(skip(self, rx))]
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

    #[tracing::instrument]
    pub async fn process_and_distribute_work(&self) -> Result<(), anyhow::Error> {
        info!("received work request, processing extraction events");
        self.process_extraction_events().await?;

        info!("doing distribution of work");
        self.distribute_work().await?;
        Ok(())
    }

    pub async fn get_executor(&self, extractor_name: &str) -> Result<ExecutorInfo, anyhow::Error> {
        let extractors_table = self.extractors_table.read().unwrap();
        let executors = extractors_table.get(extractor_name).ok_or(anyhow::anyhow!(
            "no executors for extractor: {}",
            extractor_name
        ))?;
        let rand_index = rand::random::<usize>() % executors.len();
        let executor_id = executors[rand_index].clone();
        let executors = self.executors.read().unwrap();
        let executor = executors
            .get(&executor_id)
            .ok_or(anyhow::anyhow!("no executor found for id: {}", executor_id))?;
        Ok(executor.clone())
    }
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
}

impl CoordinatorServer {
    pub async fn new(config: Arc<CoordinatorConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.listen_addr_sock()?;
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let coordinator = Coordinator::new(repository);
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self { addr, coordinator })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
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
            )
            .route(
                "/embed_query",
                post(embed_query).with_state(self.coordinator.clone()),
            )
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics);
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

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn list_executors(
    State(coordinator): State<Arc<Coordinator>>,
) -> Result<Json<ListExecutors>, IndexifyAPIError> {
    let executors = coordinator
        .executors
        .read()
        .unwrap()
        .values()
        .cloned()
        .collect();
    Ok(Json(ListExecutors { executors }))
}

#[tracing::instrument(level = "debug", skip(coordinator))]
#[tracing::instrument(skip(coordinator, executor))]
#[axum_macros::debug_handler]
async fn sync_executor(
    State(coordinator): State<Arc<Coordinator>>,
    Json(executor): Json<SyncExecutor>,
) -> Result<Json<SyncWorkerResponse>, IndexifyAPIError> {
    // Record the health check of the worker
    let worker_id = executor.executor_id.clone();
    let _ = coordinator
        .record_executor(ExecutorInfo {
            id: worker_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            addr: executor.addr.clone(),
            extractor: executor.extractor.clone(),
        })
        .await;
    // Record the outcome of any work the worker has done
    coordinator
        .update_work_state(executor.work_status, &worker_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Record the extractors available on the executor
    coordinator
        .record_extractor(executor.extractor)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Find more work for the worker
    let queued_work = coordinator
        .get_work_for_worker(&executor.executor_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Respond
    Ok(Json(SyncWorkerResponse {
        content_to_process: queued_work,
    }))
}

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn embed_query(
    State(coordinator): State<Arc<Coordinator>>,
    Json(query): Json<EmbedQueryRequest>,
) -> Result<Json<EmbedQueryResponse>, IndexifyAPIError> {
    let executor = coordinator
        .get_executor(&query.extractor_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let response = reqwest::Client::new()
        .post(&format!("http://{}/embed_query", executor.addr))
        .json(&query)
        .send()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .json::<EmbedQueryResponse>()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(EmbedQueryResponse {
        embedding: response.embedding,
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

#[tracing::instrument]
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
    use serde_json::json;

    use crate::{
        blob_storage::BlobStorageBuilder,
        persistence::ExtractorBinding,
        test_util::{
            self,
            db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
        },
    };
    use std::collections::HashMap;

    use crate::{
        data_repository_manager::DataRepositoryManager,
        persistence::{ContentPayload, DataRepository},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_work() -> Result<(), anyhow::Error> {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (vector_index_manager, extractor_executor, coordinator) =
            test_util::db_utils::create_index_manager(db.clone()).await;
        let blob_storage =
            BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).unwrap();
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), vector_index_manager, blob_storage);

        // Create a repository
        repository_manager
            .create(&DataRepository {
                name: DEFAULT_TEST_REPOSITORY.into(),
                data_connectors: vec![],
                metadata: HashMap::new(),
                extractor_bindings: vec![ExtractorBinding::new(
                    DEFAULT_TEST_REPOSITORY,
                    DEFAULT_TEST_EXTRACTOR.into(),
                    DEFAULT_TEST_EXTRACTOR.into(),
                    vec![],
                    serde_json::json!({}),
                )],
            })
            .await?;

        repository_manager
            .add_texts(
                DEFAULT_TEST_REPOSITORY,
                vec![
                    ContentPayload::from_text(
                        DEFAULT_TEST_REPOSITORY,
                        "hello",
                        HashMap::from([("topic".to_string(), json!("pipe"))]),
                    ),
                    ContentPayload::from_text(
                        DEFAULT_TEST_REPOSITORY,
                        "world",
                        HashMap::from([("topic".to_string(), json!("baz"))]),
                    ),
                ],
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
