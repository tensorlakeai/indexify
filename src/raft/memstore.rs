use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use thiserror::Error;

use openraft::BasicNode;
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftStorage;
use openraft::RaftStorageDebug;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::error;
use crate::coordinator::Coordinator;
use crate::internal_api::CreateWork;
use crate::internal_api::ExecutorInfo;
use crate::persistence::Repository;
use crate::persistence::{ExtractorConfig, Work};

use crate::coordinator::CoordinatorData;
use crate::server_config::CoordinatorConfig;

/// The application data request type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    SyncExecutor {
        executor_id: String,
        extractor: ExtractorConfig,
        addr: String,
        work_status: Vec<Work>,
    },
    EmbedQueryRequest {
        extractor_name: String,
        text: String,
    },
    CreateWork {
        repository_name: String,
        content: Option<String>,
    }
}

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    // used by raft
    Blank,
    Membership,

    // used by app
    EmbedQueryResponse {
        embedding: Vec<f32>,
    },
    SyncWorkerResponse {
        content_to_process: Vec<Work>,
    },
    CreateWorkResponse {}
}

pub type MemNodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for `MemStore`.
    pub Config: D = Request, R = Response, NodeId = MemNodeId, Node = BasicNode
);

/// The application snapshot type which the `MemStore` works with.
#[derive(Debug)]
pub struct MemStoreSnapshot {
    pub meta: SnapshotMeta<MemNodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The state machine of the `MemStore`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: Option<LogId<MemNodeId>>,

    pub last_membership: StoredMembership<MemNodeId, BasicNode>,

	pub coordinator_data: Arc<CoordinatorData>,
}

/// An in-memory storage system implementing the `RaftStorage` trait.
pub struct MemStore {
    last_purged_log_id: RwLock<Option<LogId<MemNodeId>>>,

    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<Config>>>,

    /// The Raft state machine.
    sm: RwLock<MemStoreStateMachine>,

    /// The current hard state.
    vote: RwLock<Option<Vote<MemNodeId>>>,

    snapshot_idx: Arc<Mutex<u64>>,

    /// The current snapshot.
    current_snapshot: RwLock<Option<MemStoreSnapshot>>,

    coordinator: Arc<Coordinator>,
}

impl MemStore {
    /// Create a new `MemStore` instance.
    pub async fn new(config: Arc<CoordinatorConfig>) -> Result<Self, RaftStorageError> {
        let log = RwLock::new(BTreeMap::new());
        let sm = RwLock::new(MemStoreStateMachine::default());
        let current_snapshot = RwLock::new(None);

        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let coordinator = Coordinator::new(repository);

        Ok(Self {
            last_purged_log_id: RwLock::new(None),
            log,
            sm,
            vote: RwLock::new(None),
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,

            coordinator,
        })
    }
}

#[async_trait]
impl RaftStorageDebug<MemStoreStateMachine> for Arc<MemStore> {
    /// Get a handle to the state machine for testing purposes.
    async fn get_state_machine(&mut self) -> MemStoreStateMachine {
        self.sm.write().await.clone()
    }
}

#[async_trait]
impl RaftLogReader<Config> for Arc<MemStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<Config>>, StorageError<MemNodeId>> {
        let res = {
            let log = self.log.read().await;
            log.range(range.clone()).map(|(_, val)| val.clone()).collect::<Vec<_>>()
        };

        Ok(res)
    }

    async fn get_log_state(&mut self) -> Result<LogState<Config>, StorageError<MemNodeId>> {
        let log = self.log.read().await;
        let last = log.iter().rev().next().map(|(_, ent)| ent.log_id);

        let last_deleted = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_deleted,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_deleted,
            last_log_id: last,
        })
    }
}

#[async_trait]
impl RaftSnapshotBuilder<Config, Cursor<Vec<u8>>> for Arc<MemStore> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<MemNodeId, BasicNode, Cursor<Vec<u8>>>, StorageError<MemNodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)
                .map_err(|e| StorageIOError::new(ErrorSubject::StateMachine, ErrorVerb::Read, AnyError::new(&e)))?;

            last_applied_log = sm.last_applied_log;
            last_membership = sm.last_membership.clone();
        }

        let snapshot_size = data.len();

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        tracing::info!(snapshot_size, "log compaction complete");

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<Config> for Arc<MemStore> {
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<MemNodeId>) -> Result<(), StorageError<MemNodeId>> {
        tracing::debug!(?vote, "save_vote");
        let mut h = self.vote.write().await;

        *h = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<MemNodeId>>, StorageError<MemNodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<MemNodeId>>, StoredMembership<MemNodeId, BasicNode>), StorageError<MemNodeId>> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(&mut self, log_id: LogId<MemNodeId>) -> Result<(), StorageError<MemNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut log = self.log.write().await;

            let keys = log.range(log_id.index..).map(|(k, _v)| *k).collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn purge_logs_upto(&mut self, log_id: LogId<MemNodeId>) -> Result<(), StorageError<MemNodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.write().await;

            let keys = log.range(..=log_id.index).map(|(k, _v)| *k).collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(&mut self, entries: &[&Entry<Config>]) -> Result<(), StorageError<MemNodeId>> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, (*entry).clone());
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<Config>],
    ) -> Result<Vec<Response>, StorageError<MemNodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.sm.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response::Blank),
                EntryPayload::Normal(ref request) => {
                    // TODO: DRY - this is duplicated in the coordinator.rs server code
                    match request {
                        Request::CreateWork { repository_name, content } => {
                            let create_work: CreateWork = CreateWork {
                                repository_name: repository_name.clone(),
                                content: content.clone(),
                            };

                            if let Err(err) = self.coordinator.tx.try_send(create_work) {
                                error!("unable to send create work request: {}", err.to_string());
                            }

                            res.push(Response::CreateWorkResponse {});
                        },
                        Request::EmbedQueryRequest { extractor_name, text: _ } => {
                            let query = request;
                            let executor: ExecutorInfo = self.coordinator
                                .get_executor(extractor_name)
                                .await
                                .map_err(|e| RaftStorageError::from(e))?;
                            let response: Response = reqwest::Client::new()
                                .post(&format!("http://{}/embed_query", executor.addr))
                                .json(&query)
                                .send()
                                .await
                                .map_err(|e| RaftStorageError::from(e))?
                                .json::<Response>()
                                .await
                                .map_err(|e| RaftStorageError::from(e))?;
                            match response {
                                Response::EmbedQueryResponse { embedding } => {
                                    res.push(Response::EmbedQueryResponse {
                                        embedding: embedding,
                                    });
                                },
                                _ => {
                                    error!("unexpected response from executor");
                                }
                            }
                        },
                        Request::SyncExecutor { executor_id, extractor, addr, work_status } => {
                            // Record the health check of the worker
                            let worker_id = executor_id.clone();
                            let _ = self.coordinator
                                .record_executor(ExecutorInfo {
                                    id: worker_id.clone(),
                                    last_seen: SystemTime::now()
                                        .duration_since(SystemTime::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs(),
                                    addr: addr.clone(),
                                    extractor: extractor.clone(),
                                })
                                .await;
                            // Record the outcome of any work the worker has done
                            self.coordinator
                                .update_work_state(work_status.clone(), &worker_id) // TODO: remove clone
                                .await
                                .map_err(|e| RaftStorageError::from(e))?;

                            // Record the extractors available on the executor
                            self.coordinator
                                .record_extractor(extractor.clone()) // TODO: remove clone
                                .await
                                .map_err(|e| RaftStorageError::from(e))?;

                            // Find more work for the worker
                            let queued_work = self.coordinator
                                .get_work_for_worker(&executor_id)
                                .await
                                .map_err(|e| RaftStorageError::from(e))?;

                            res.push(Response::SyncWorkerResponse {
                                content_to_process: queued_work,
                            });
                        }
                    }
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response::Membership)
                }
            };
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Self::SnapshotData>, StorageError<MemNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MemNodeId, BasicNode>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<(), StorageError<MemNodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        {
            let t = &new_snapshot.data;
            let y = std::str::from_utf8(t).unwrap();
            tracing::debug!("SNAP META:{:?}", meta);
            tracing::debug!("JSON SNAP DATA:{}", y);
        }

        // Update the state machine.
        {
            let new_sm: MemStoreStateMachine = serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::Snapshot(new_snapshot.meta.signature()),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MemNodeId, BasicNode, Self::SnapshotData>>, StorageError<MemNodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }

    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

// TODO: this is a quick and dirty implementation. Should be double checked.
#[derive(Debug, Error)]
pub enum RaftStorageError {
    #[error("storage io error with raft state machine: {0}")]
    RaftStateMachineError(String),
    #[error("storage io error: {0}")]
    MemStoreError(String),
    #[error("storage io error: {0}")]
    MemStoreRepositoryError(#[from] crate::persistence::RepositoryError),

    #[error("storage io error: {0}")]
    EmbedQueryRequestError(#[from] anyhow::Error),
}

// TODO: this is a quick and dirty implementation. Should be double checked.
impl From<reqwest::Error> for RaftStorageError {
    fn from(error: reqwest::Error) -> Self {
        // match the path of the error to determine the error type. Right now just for embed query.
        match error.url() {
            Some(url) => {
                if url.path() == "/embed_query" {
                    RaftStorageError::EmbedQueryRequestError(anyhow::Error::new(error))
                } else {
                    RaftStorageError::MemStoreError(format!("reqwest error: {}", error.to_string()))
                }
            },
            None => {
                RaftStorageError::MemStoreError(format!("reqwest error: {}", error.to_string()))
            }
        }
    }
}

// TODO: this is a quick and dirty implementation. Should be double checked.
impl From<RaftStorageError> for StorageError<u64> {
    fn from(error: RaftStorageError) -> Self {
        // Wrap the StorageIOError in the IO variant of StorageError
        match error {
            RaftStorageError::RaftStateMachineError(msg) => {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Write,
                    AnyError::error(msg),
                )
            },
            RaftStorageError::MemStoreError(msg) => {
                StorageIOError::new(
                    ErrorSubject::Store, // Adjust this as appropriate
                    ErrorVerb::Read, // Or any other verb that describes the error context
                    AnyError::error(msg),
                )
            },
            RaftStorageError::MemStoreRepositoryError(repo_error) => {
                StorageIOError::new(
                    ErrorSubject::Store, // Again, adjust as needed
                    ErrorVerb::Read, // Adjust this verb too
                    AnyError::new(&repo_error),
                )
            },
            RaftStorageError::EmbedQueryRequestError(anyhow_error) => {
                StorageIOError::new(
                    ErrorSubject::StateMachine, // Adjust as necessary
                    ErrorVerb::Read, // Choose the verb that fits the operation that caused the error
                    AnyError::error(anyhow_error),
                )
            },
        }.into()
    }
}