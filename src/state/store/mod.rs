mod error;
mod impl_sledstoreable;
mod store;

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::Debug,
    io::Cursor,
    ops::RangeBounds,
    string::ToString,
    sync::Arc,
};

use anyerror::AnyError;
use error::*;
use impl_sledstoreable::SledStoreable;
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
pub use store::Store;
use store::*;
use tracing::info;

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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Request {
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
    RemoveExecutor {
        executor_id: String,
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
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

#[derive(Clone)]
pub enum ChangeType {
    NewContent,
    NewBinding,
}

#[derive(Clone)]
pub struct StateChange {
    pub id: String,
    pub change_type: ChangeType,
}

/**
 * Here defines a state machine of the raft, this state represents a copy of
 * the data between each node. Note that we are using `serde` to serialize
 * the `data`, which has a implementation to be serialized. Note that for
 * this test we set both the key and value as String, but you could set any
 * type of value that has the serialization impl.
 *
 * IMPORTANT: All fields of StateMachine must:
 * - have SledStoreable implemented
 * - have a test in ./impl_sledstoreable.rs
 * - be handled in the StateMachine::try_from_sled_tree fn
 *
 * TODO: make the StateMachine migrate-able
 */
#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct StateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, BasicNode>,

    pub executors: HashMap<ExecutorId, ExecutorMetadata>,

    pub tasks: HashMap<TaskId, Task>,

    pub unassigned_tasks: HashSet<TaskId>,

    pub task_assignments: HashMap<ExecutorId, HashSet<TaskId>>,

    pub extraction_events: HashMap<ExtractionEventId, ExtractionEvent>,

    pub unprocessed_extraction_events: HashSet<ExtractionEventId>,

    pub content_table: HashMap<ContentId, ContentMetadata>,

    pub content_repository_table: HashMap<RepositoryId, HashSet<ContentId>>,

    pub bindings_table: HashMap<RepositoryId, HashSet<ExtractorBinding>>,

    pub extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,

    pub extractors: HashMap<ExtractorName, ExtractorDescription>,

    pub repositories: HashSet<String>,

    pub repository_extractors: HashMap<RepositoryId, HashSet<Index>>,

    pub index_table: HashMap<String, Index>,
}

#[async_trait]
impl RaftLogReader<TypeConfig> for Arc<Store> {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged = self
            .get_last_purged_log_id()
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

        let last_log = self
            .get_last_log()
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?
            .map(|(log_id, _)| log_id);

        let last_log_id = match (last_purged, last_log) {
            // If we have both last_purged and last_log, we take the max of the two
            (Some(purged), Some(log)) => std::cmp::max(purged, log),
            // If we only have last log, we use that
            (None, Some(log)) => log,
            // If we only have last purged, we use that
            (Some(purged), None) => purged,
            // If we have no log at all, we return None
            (None, None) => {
                return Ok(LogState {
                    last_purged_log_id: None,
                    last_log_id: None,
                })
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: Some(last_log_id),
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start_bound = range.start_bound();
        let start = match start_bound {
            std::ops::Bound::Included(x) => LogKey::from(*x).to_raw_log_key(),
            std::ops::Bound::Excluded(x) => LogKey::from(*x + 1).to_raw_log_key(),
            std::ops::Bound::Unbounded => LogKey::from(0).to_raw_log_key(),
        };

        let logs_tree = self.open_tree(SledStoreTree::Logs);

        logs_tree
            .range::<&[u8], _>(start.as_slice()..)
            .try_fold(vec![], |mut acc, res| {
                let (raw_log_key, raw_entry) = res.map_err(|e| StorageError::IO {
                    source: StorageIOError::read_logs(&e),
                })?;

                let log_key = LogKey::from(raw_log_key);
                let entry = Entry::<TypeConfig>::load_from_sled_value(raw_entry).map_err(|e| {
                    StorageError::IO {
                        source: StorageIOError::read_logs(AnyError::from_dyn(&*e, None)),
                    }
                })?;

                if range.contains(&log_key.into()) {
                    acc.push(entry);
                }

                Ok(acc)
            })
    }
}

#[async_trait]
impl RaftSnapshotBuilder<TypeConfig> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        let storeable_state_machine = state_machine
            .to_saveable_value()
            .map_err(|e| build_snapshot_err(e.into()))?;

        let last_applied_log = self
            .get_last_applied_log()
            .map_err(|e| build_snapshot_err(e.into()))?;
        let last_membership = self
            .get_last_membership()
            .map_err(|e| build_snapshot_err(e.into()))?;

        let snapshot_idx = self
            .get_snapshot_index()
            .map_err(|e| build_snapshot_err(e.into()))?
            .map_or(0, |idx| idx + 1);

        let snapshot_id = last_applied_log
            .as_ref()
            .map(|last| format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx))
            .unwrap_or_else(|| format!("--{}", snapshot_idx));

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership: last_membership.unwrap_or_default(),
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: storeable_state_machine.to_vec(),
        };

        self.set_current_snapshot(snapshot.clone())
            .await
            .map_err(|e| build_snapshot_err(e.into()))?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        })
    }
}

#[async_trait]
impl RaftStorage<TypeConfig> for Arc<Store> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.insert_vote(*vote)
            .await
            .map_err(|e| save_vote_err(e.into()))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.get_vote().map_err(|e| read_vote_err(e.into()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let logs_tree = self.open_tree(SledStoreTree::Logs);
        let mut batch = sled::Batch::default();
        for entry in entries.into_iter() {
            let log_key = LogKey::from_log(&entry);
            let log_value = entry.to_saveable_value().map_err(append_log_err)?;
            batch.insert(&log_key.to_raw_log_key(), log_value);
        }
        logs_tree
            .apply_batch(batch)
            .map_err(|e| append_log_err(e.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_conflict_logs_since: [{:?}, +oo)", log_id);

        // fetch the logs tree
        let logs_tree = self.open_tree(SledStoreTree::Logs);

        // find all keys greater than the given log id
        let keys: Vec<LogKey> = logs_tree
            .range(LogKey::from_log_id(&log_id).to_raw_log_key()..)
            .map(|res| res.expect("Failed to read log entry").0)
            .map(|log_key| LogKey::from(log_key))
            .collect::<Vec<_>>();

        // delete all keys greater than the given log id
        // we aren't using a transaction here for performance reasons,
        // but deleting the keys in reverse order should ensure that
        // we don't leave any holes in the log
        for key in keys.iter().rev() {
            let tx_key = key.to_raw_log_key();
            logs_tree
                .remove(tx_key)
                .map_err(|e| delete_conflict_logs_since_err(e.into()))?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        // fetch the logs tree
        let logs_tree = self.open_tree(SledStoreTree::Logs);

        // create a batch to delete all logs up to and including the given log id
        let mut batch = sled::Batch::default();

        let start = LogKey::from_log_id(&log_id).to_raw_log_key();
        let end = LogKey::from(log_id.index + 1).to_raw_log_key();

        // iterate over all logs up to and including the given log id
        for result in logs_tree.range(start..end) {
            let (raw_log_key, _) = result.map_err(|e| StorageError::IO {
                source: StorageIOError::read_logs(&e),
            })?;

            let log_key = LogKey::from(raw_log_key);

            // remove the log from the batch
            batch.remove(&log_key.to_raw_log_key());
        }

        logs_tree
            .apply_batch(batch)
            .map_err(|e| purge_logs_upto_err(e.into()))?;

        // set the last purged log id
        let store_tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::LastPurgedLogId.to_string();
        let value = log_id
            .to_saveable_value()
            .map_err(|e| purge_logs_upto_err(e.into()))?;
        store_tree
            .insert(key, value)
            .map_err(|e| purge_logs_upto_err(e.into()))?;

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let last_applied_log = self
            .get_last_applied_log()
            .map_err(|e| last_applied_state_err(e.into()))?;
        let last_membership = self
            .get_last_membership()
            .map_err(|e| last_applied_state_err(e.into()))?;
        Ok((last_applied_log, last_membership.unwrap_or_default()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        let state_machine_tree = self.open_tree(SledStoreTree::StateMachine);

        let mut change_events: Vec<StateChange> = Vec::new();

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(Response { value: None }),
                EntryPayload::Normal(ref req) => match req {
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
                            .insert(executor_id.clone());
                        let executor_info = ExecutorMetadata {
                            id: executor_id.clone(),
                            last_seen: *ts_secs,
                            addr: addr.clone(),
                            extractor: extractor.clone(),
                        };
                        sm.executors.insert(executor_id.clone(), executor_info);
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extractors",
                            Box::new(sm.extractors.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extractor_executors_table",
                            Box::new(sm.extractor_executors_table.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "executors",
                            Box::new(sm.executors.clone()),
                        )?;
                        res.push(Response { value: None })
                    }
                    Request::RemoveExecutor { executor_id } => {
                        // Remove this from the executors table
                        let executor_meta = sm.executors.remove(executor_id);
                        // Remove this from the extractor -> executors table
                        if let Some(executor_meta) = executor_meta {
                            let executors = sm
                                .extractor_executors_table
                                .entry(executor_meta.extractor.name.clone())
                                .or_default();
                            executors.remove(executor_meta.extractor.name.as_str());
                        }
                        // update the state machine in sled
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "executors",
                            Box::new(sm.executors.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extractor_executors_table",
                            Box::new(sm.extractor_executors_table.clone()),
                        )?;
                        res.push(Response { value: None })
                    }
                    Request::CreateTasks { tasks } => {
                        for task in tasks {
                            sm.tasks.insert(task.id.clone(), task.clone());
                            sm.unassigned_tasks.insert(task.id.clone());
                        }
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "tasks",
                            Box::new(sm.tasks.clone()),
                        )?;
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
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "task_assignments",
                            Box::new(sm.task_assignments.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unassigned_tasks",
                            Box::new(sm.unassigned_tasks.clone()),
                        )?;
                        res.push(Response { value: None })
                    }
                    Request::AddExtractionEvent { event } => {
                        sm.extraction_events.insert(event.id.clone(), event.clone());
                        sm.unprocessed_extraction_events.insert(event.id.clone());
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extraction_events",
                            Box::new(sm.extraction_events.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unprocessed_extraction_events",
                            Box::new(sm.unprocessed_extraction_events.clone()),
                        )?;
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
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extraction_events",
                            Box::new(sm.extraction_events.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unprocessed_extraction_events",
                            Box::new(sm.unprocessed_extraction_events.clone()),
                        )?;

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
                            change_events.push(StateChange {
                                id: content.id.clone(),
                                change_type: ChangeType::NewContent,
                            });
                        }
                        for event in extraction_events {
                            sm.extraction_events.insert(event.id.clone(), event.clone());
                            sm.unprocessed_extraction_events.insert(event.id.clone());
                        }
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "content_table",
                            Box::new(sm.content_table.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "content_repository_table",
                            Box::new(sm.content_repository_table.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extraction_events",
                            Box::new(sm.extraction_events.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unprocessed_extraction_events",
                            Box::new(sm.unprocessed_extraction_events.clone()),
                        )?;

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
                        change_events.push(StateChange {
                            id: binding.id.clone(),
                            change_type: ChangeType::NewBinding,
                        });
                        if let Some(extraction_event) = extraction_event {
                            sm.extraction_events
                                .insert(extraction_event.id.clone(), extraction_event.clone());
                            sm.unprocessed_extraction_events
                                .insert(extraction_event.id.clone());
                        }
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "bindings_table",
                            Box::new(sm.bindings_table.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "extraction_events",
                            Box::new(sm.extraction_events.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unprocessed_extraction_events",
                            Box::new(sm.unprocessed_extraction_events.clone()),
                        )?;
                        res.push(Response { value: None })
                    }
                    Request::CreateRepository { name } => {
                        sm.repositories.insert(name.clone());
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "repositories",
                            Box::new(sm.repositories.clone()),
                        )?;
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
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "repository_extractors",
                            Box::new(sm.repository_extractors.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "index_table",
                            Box::new(sm.index_table.clone()),
                        )?;
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
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "tasks",
                            Box::new(sm.tasks.clone()),
                        )?;
                        sm.overwrite_sled_kv(
                            &state_machine_tree,
                            "unassigned_tasks",
                            Box::new(sm.unassigned_tasks.clone()),
                        )?;
                        res.push(Response { value: None })
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    sm.overwrite_sled_kv(
                        &state_machine_tree,
                        "last_membership",
                        Box::new(sm.last_membership.clone()),
                    )?;
                    res.push(Response { value: None })
                }
            };
        }

        for change_event in change_events {
            if let Err(err) = self.state_change_tx.send(change_event) {
                tracing::error!("error sending state change event: {}", err);
            }
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
            let updated_state_machine =
                StateMachine::load_from_sled_value(new_snapshot.data.clone().into())
                    .map_err(install_snapshot_err)?;
            let mut state_machine = self.state_machine.write().await;
            updated_state_machine
                .try_save_to_sled_tree(&self.open_tree(SledStoreTree::StateMachine))
                .map_err(|e| install_snapshot_err(e.into()))?;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        self.set_current_snapshot(new_snapshot)
            .await
            .map_err(|e| install_snapshot_err(e.into()))?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let result = self
            .db
            .get_current_snapshot()
            .map_err(|e| get_current_snapshot_err(e.into()))?;
        match result {
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

fn build_snapshot_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteSnapshot
        .build_with_source("build snapshot failed", e)
        .into()
}
fn save_vote_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteVote
        .build_with_source("save vote failed", e)
        .into()
}
fn read_vote_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::ReadVote
        .build_with_source("read vote failed", e)
        .into()
}
fn append_log_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteLogs
        .build_with_source("append log failed", e)
        .into()
}
fn last_applied_state_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::ReadStateMachine
        .build_with_source("last applied state failed", e)
        .into()
}
fn purge_logs_upto_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteLogs
        .build_with_source("purge logs upto failed", e)
        .into()
}
fn delete_conflict_logs_since_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteLogs
        .build_with_source("delete conflict logs since failed", e)
        .into()
}
fn install_snapshot_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::WriteSnapshot
        .build_with_source("install snapshot failed", e)
        .into()
}
fn get_current_snapshot_err(e: Box<dyn Error>) -> StorageError<NodeId> {
    StoreErrorKind::ReadSnapshot
        .build_with_source("get current snapshot failed", e)
        .into()
}
