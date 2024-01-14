use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    sync::{Arc, Mutex},
};

use openraft::{
    async_trait::async_trait,
    storage::{LogState, Snapshot},
    BasicNode,
    Entry,
    EntryPayload,
    LogId,
    RaftLogReader,
    RaftSnapshotBuilder,
    RaftStorage,
    RaftTypeConfig,
    SnapshotMeta,
    StorageError,
    StorageIOError,
    StoredMembership,
    Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::{NodeId, TypeConfig};
use crate::internal_api::{
    ContentMetadata,
    ExecutorMetadata,
    ExtractionEvent,
    ExtractorBinding,
    ExtractorDescription,
    Index,
    Task,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    ExecutorHeartbeat {
        executor_id: String,
        ts_secs: u64,
    },

    RegisterExecutor {
        addr: String,
        executor_id: String,
        extractor: ExtractorDescription,
        ts_secs: u64,
    },
    CreateRepository {
        name: String,
    },
    CreateTasks {
        tasks: Vec<Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    AddExtractionEvent {
        event: ExtractionEvent,
    },
    MarkExtractionEventProcessed {
        event_id: String,
        ts_secs: u64,
    },
    CreateContent {
        content_metadata: Vec<ContentMetadata>,
        extraction_events: Vec<ExtractionEvent>,
    },
    CreateBinding {
        binding: ExtractorBinding,
        extraction_event: Option<ExtractionEvent>,
    },
    CreateIndex {
        index: Index,
        repository: String,
        id: String,
    },
    UpdateTask {
        task: Task,
        mark_finished: bool,
        executor_id: Option<String>,
        content_metadata: Vec<ContentMetadata>,
        extraction_events: Vec<ExtractionEvent>,
    },
}

/**
 * Here you will defined what type of answer you expect from reading the
 * data of a node. In this example it will return a optional value from a
 * given key in the `Request.Set`.
 *
 * TODO: Should we explain how to create multiple `AppDataResponse`?
 *
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, BasicNode>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

pub type RepositoryId = String;
pub type TaskId = String;
pub type ContentId = String;
pub type ExecutorId = String;
pub type ExtractionEventId = String;
pub type ExtractorName = String;

/**
 * Here defines a state machine of the raft, this state represents a copy of
 * the data between each node. Note that we are using `serde` to serialize
 * the `data`, which has a implementation to be serialized. Note that for
 * this test we set both the key and value as String, but you could set any
 * type of value that has the serialization impl.
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    // Executor ID -> Last Seen Timestamp
    pub executor_health_checks: HashMap<ExecutorId, u64>,

    pub executors: HashMap<ExecutorId, ExecutorMetadata>,

    pub tasks: HashMap<TaskId, Task>,

    pub unassigned_tasks: HashSet<TaskId>,

    pub task_assignments: HashMap<ExecutorId, HashSet<TaskId>>,

    pub extraction_events: HashMap<ExtractionEventId, ExtractionEvent>,

    pub unprocessed_extraction_events: HashSet<ExtractionEventId>,

    pub content_table: HashMap<ContentId, ContentMetadata>,

    pub content_repository_table: HashMap<RepositoryId, HashSet<ContentId>>,

    pub bindings_table: HashMap<RepositoryId, HashSet<ExtractorBinding>>,

    pub extractor_executors_table: HashMap<ExtractorName, Vec<ExecutorId>>,

    pub extractors: HashMap<ExtractorName, ExtractorDescription>,

    pub repositories: HashSet<String>,

    pub repository_extractors: HashMap<RepositoryId, HashSet<Index>>,

    pub index_table: HashMap<String, Index>,
}

#[derive(Debug, Default)]
pub struct Store {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,

    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,

    /// The Raft state machine.
    pub state_machine: RwLock<StateMachine>,

    /// The current granted vote.
    vote: RwLock<Option<Vote<NodeId>>>,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<Store> {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last = log.iter().next_back().map(|(_, ent)| ent.log_id);

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let response = log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let data;
        let last_applied_log;
        let last_membership;

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine)
                .map_err(|e| StorageIOError::read_state_machine(&e))?;

            last_applied_log = state_machine.last_applied_log;
            last_membership = state_machine.last_membership.clone();
        }

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

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<Store> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let mut log = self.log.write().await;
        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let mut log = self.log.write().await;

            let keys = log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                log.remove(&key);
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response { value: None }),
                EntryPayload::Normal(ref req) => match req {
                    Request::ExecutorHeartbeat {
                        executor_id,
                        ts_secs,
                    } => {
                        sm.executor_health_checks
                            .insert(executor_id.clone(), *ts_secs);
                        res.push(Response { value: None })
                    }
                    Request::RegisterExecutor {
                        addr,
                        executor_id,
                        extractor,
                        ts_secs,
                    } => {
                        sm.extractors
                            .insert(extractor.name.clone(), extractor.clone());
                        sm.extractor_executors_table
                            .entry(extractor.name.clone())
                            .or_default()
                            .push(executor_id.clone());
                        let executor_info = ExecutorMetadata {
                            id: executor_id.clone(),
                            last_seen: *ts_secs,
                            addr: addr.clone(),
                            extractor: extractor.clone(),
                        };
                        sm.executors.insert(executor_id.clone(), executor_info);
                        res.push(Response { value: None })
                    }
                    Request::CreateTasks { tasks } => {
                        for task in tasks {
                            sm.tasks.insert(task.id.clone(), task.clone());
                            sm.unassigned_tasks.insert(task.id.clone());
                        }
                        res.push(Response { value: None })
                    }
                    Request::AssignTask { assignments } => {
                        for (task_id, executor_id) in assignments {
                            sm.task_assignments
                                .entry(executor_id.clone())
                                .or_default()
                                .insert(task_id.clone());
                            sm.unassigned_tasks.remove(task_id);
                        }
                        res.push(Response { value: None })
                    }
                    Request::AddExtractionEvent { event } => {
                        sm.extraction_events.insert(event.id.clone(), event.clone());
                        sm.unprocessed_extraction_events.insert(event.id.clone());
                        res.push(Response { value: None })
                    }
                    Request::MarkExtractionEventProcessed { event_id, ts_secs } => {
                        sm.unprocessed_extraction_events.retain(|id| id != event_id);
                        let event = sm.extraction_events.get(event_id).map(|event| {
                            let mut event = event.to_owned();
                            event.processed_at = Some(*ts_secs);
                            event
                        });
                        if let Some(event) = event {
                            sm.extraction_events.insert(event_id.clone(), event);
                        }

                        res.push(Response { value: None })
                    }
                    Request::CreateContent {
                        content_metadata,
                        extraction_events,
                    } => {
                        for content in content_metadata {
                            sm.content_table.insert(content.id.clone(), content.clone());
                            sm.content_repository_table
                                .entry(content.repository.clone())
                                .or_default()
                                .insert(content.id.clone());
                        }
                        for event in extraction_events {
                            sm.extraction_events.insert(event.id.clone(), event.clone());
                            sm.unprocessed_extraction_events.insert(event.id.clone());
                        }
                        res.push(Response { value: None })
                    }
                    Request::CreateBinding {
                        binding,
                        extraction_event,
                    } => {
                        sm.bindings_table
                            .entry(binding.repository.clone())
                            .or_default()
                            .insert(binding.clone());
                        if let Some(extraction_event) = extraction_event {
                            sm.extraction_events
                                .insert(extraction_event.id.clone(), extraction_event.clone());
                            sm.unprocessed_extraction_events
                                .insert(extraction_event.id.clone());
                        }
                        res.push(Response { value: None })
                    }
                    Request::CreateRepository { name } => {
                        sm.repositories.insert(name.clone());
                        res.push(Response { value: None })
                    }
                    Request::CreateIndex {
                        index,
                        repository,
                        id,
                    } => {
                        sm.repository_extractors
                            .entry(repository.clone())
                            .or_default()
                            .insert(index.clone());
                        sm.index_table.insert(id.clone(), index.clone());
                        res.push(Response { value: None })
                    }
                    Request::UpdateTask {
                        task,
                        mark_finished,
                        executor_id,
                        content_metadata,
                        extraction_events,
                    } => {
                        sm.tasks.insert(task.id.clone(), task.clone());
                        if *mark_finished {
                            sm.unassigned_tasks.remove(&task.id);
                            if let Some(executor_id) = executor_id {
                                sm.task_assignments
                                    .entry(executor_id.clone())
                                    .or_default()
                                    .remove(&task.id);
                            }
                        }
                        for content in content_metadata {
                            sm.content_table.insert(content.id.clone(), content.clone());
                            sm.content_repository_table
                                .entry(content.repository.clone())
                                .or_default()
                                .insert(content.id.clone());
                        }
                        for event in extraction_events {
                            sm.extraction_events.insert(event.id.clone(), event.clone());
                            sm.unprocessed_extraction_events.insert(event.id.clone());
                        }
                        res.push(Response { value: None })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(Response { value: None })
                }
            };
        }
        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: StateMachine = serde_json::from_slice(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e)
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
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

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
