use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs::{self, File},
    io::{BufReader, Cursor, Read, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use flate2::bufread::ZlibDecoder;
use indexify_internal_api::{ContentMetadata, ExecutorMetadata, StateChange, StructuredDataSchema};
use openraft::{
    storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot},
    AnyError,
    BasicNode,
    Entry,
    EntryPayload,
    ErrorSubject,
    ErrorVerb,
    LogId,
    OptionalSend,
    RaftLogReader,
    RaftSnapshotBuilder,
    SnapshotMeta,
    StorageError,
    StorageIOError,
    StoredMembership,
    Vote,
};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, OptimisticTransactionDB, Options};
use serde::{de::DeserializeOwned, Deserialize};
use strum::{AsRefStr, IntoEnumIterator};
use thiserror::Error;
use tokio::sync::{broadcast, RwLock};
use tracing::debug;

type Node = BasicNode;

use self::{
    requests::RequestPayload,
    serializer::{JsonEncode, JsonEncoder},
    state_machine_objects::{IndexifyState, IndexifyStateSnapshot},
};
use super::{typ, NodeId, SnapshotData, TypeConfig};
use crate::{
    metrics::{state_machine::Metrics, Timer},
    utils::OptionInspectNone,
};

pub type NamespaceName = String;
pub type TaskId = String;
pub type StateChangeId = String;
pub type ContentId = String;
pub type ExecutorId = String;
pub type ExecutorIdRef<'a> = &'a str;
pub type ExtractionEventId = String;
pub type ExtractionPolicyId = String;
pub type ExtractorName = String;
pub type ContentType = String;
pub type SchemaId = String;

pub mod requests;
pub mod serializer;
pub mod state_machine_objects;

#[derive(Error, Debug)]
pub enum StateMachineError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("RocksDB transaction error: {0}")]
    TransactionError(String),

    #[error("External error: {0}")]
    ExternalError(#[from] anyhow::Error),
}

#[derive(AsRefStr, strum::Display, strum::EnumIter)]
pub enum StateMachineColumns {
    Executors,                          //  ExecutorId -> Executor Metadata
    Tasks,                              //  TaskId -> Task
    GarbageCollectionTasks,             //  GCTaskId -> GCTask
    TaskAssignments,                    //  ExecutorId -> HashSet<TaskId>
    StateChanges,                       //  StateChangeId -> StateChange
    ContentTable,                       //  ContentId -> ContentMetadata
    ExtractionPolicies,                 //  ExtractionPolicyId -> ExtractionPolicy
    Extractors,                         //  ExtractorName -> ExtractorDescription
    Namespaces,                         //  Namespaces
    IndexTable,                         //  String -> Index
    StructuredDataSchemas,              //  SchemaId -> StructuredDataSchema
    ExtractionPoliciesAppliedOnContent, //  ContentId -> Vec<ExtractionPolicyIds>
    CoordinatorAddress,                 //  NodeId -> Coordinator address
}

impl StateMachineColumns {
    pub fn cf<'a>(&'a self, db: &'a Arc<OptimisticTransactionDB>) -> &'a ColumnFamily {
        db.cf_handle(self.as_ref())
            .inspect_none(|| {
                tracing::error!("failed to get column family handle for {}", self.as_ref());
            })
            .unwrap()
    }
}

#[derive(serde::Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(serde::Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

pub struct StateMachineData {
    pub last_applied_log_id: RwLock<Option<LogId<NodeId>>>,

    pub last_membership: RwLock<StoredMembership<NodeId, Node>>,

    /// State built from applying the raft log
    pub indexify_state: IndexifyState,

    state_change_tx: Arc<tokio::sync::watch::Sender<StateChange>>,

    gc_tasks_tx: broadcast::Sender<indexify_internal_api::GarbageCollectionTask>,
}

pub struct StateMachineStore {
    pub data: StateMachineData,

    snapshot_idx: Mutex<u64>,

    db: Arc<OptimisticTransactionDB>,

    pub state_change_rx: tokio::sync::watch::Receiver<StateChange>,

    snapshot_file_path: PathBuf,

    metrics: Metrics,
}

impl StateMachineStore {
    async fn new(
        db: Arc<OptimisticTransactionDB>,
        snapshot_file_path: PathBuf,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let (tx, rx) = tokio::sync::watch::channel(StateChange::default());
        let (gc_tasks_tx, _) = broadcast::channel(100);
        let sm = Self {
            data: StateMachineData {
                last_applied_log_id: RwLock::new(None),
                last_membership: RwLock::new(StoredMembership::default()),
                indexify_state: IndexifyState::default(),
                state_change_tx: Arc::new(tx),
                gc_tasks_tx,
            },
            snapshot_idx: Mutex::new(0),
            db,
            state_change_rx: rx,
            snapshot_file_path,
            metrics: Metrics::new(),
        };

        let snapshot = sm.get_current_snapshot_()?;
        if let Some(snap) = snapshot {
            sm.update_state_machine_(snap).await?;
        }

        Ok(sm)
    }

    /// This method is used to update the in-memory state machine when a new
    /// state machine is provided via the InstallSnapshot RPC
    async fn update_state_machine_(
        &self,
        snapshot: StoredSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        let indexify_state_snapshot: IndexifyStateSnapshot = JsonEncoder::decode(&snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

        {
            let mut guard = self.data.last_applied_log_id.write().await;
            *guard = snapshot.meta.last_log_id;
        }
        {
            let mut guard = self.data.last_membership.write().await;
            *guard = snapshot.meta.last_membership.clone();
        }

        self.data
            .indexify_state
            .install_snapshot(indexify_state_snapshot);

        Ok(())
    }

    fn get_current_snapshot_(&self) -> StorageResult<Option<StoredSnapshot>> {
        debug!("Called get_current_snapshot_");
        if !self.snapshot_file_path.exists() {
            debug!("The snapshot file does not exist");
            return Ok(None);
        }

        let file = File::open(&self.snapshot_file_path).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        //  Decompress the data with ZLib decoder
        let buf_reader = BufReader::new(file);
        let mut decoder = ZlibDecoder::new(buf_reader);
        let mut decompressed_data = Vec::new();
        decoder
            .read_to_end(&mut decompressed_data)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?;

        //  deserialize the data and return it
        let snapshot: StoredSnapshot =
            JsonEncoder::decode(&decompressed_data).map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?;
        Ok(Some(snapshot))
    }

    /// This method is called when a new snapshot is received via
    /// InstallSnapshot RPC and is used to write the snapshot to disk
    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> StorageResult<()> {
        debug!("Called set_current_snapshot_");

        //  Serialize the data into JSON bytes
        let serialized_data = JsonEncoder::encode(&snap).map_err(|e| StorageError::IO {
            source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
        })?;
        let uncompressed_size = serialized_data.len();

        //  Compress the serialized meta and data using Zlib
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(&serialized_data)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
            })?;
        let compressed_data = encoder.finish().map_err(|e| StorageError::IO {
            source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
        })?;
        let compressed_size = compressed_data.len();

        //  Create a temp file, write to temp file and then swap inode pointers
        let temp_file_path = self.snapshot_file_path.with_extension("tmp");
        let mut temp_file = File::create(&temp_file_path).map_err(|e| StorageError::IO {
            source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
        })?;
        temp_file
            .write_all(&compressed_data)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
            })?;
        temp_file.sync_all().map_err(|e| StorageError::IO {
            source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
        })?;
        fs::rename(&temp_file_path, &self.snapshot_file_path).map_err(|e| StorageError::IO {
            source: StorageIOError::write_snapshot(Some(snap.meta.signature()), &e),
        })?;

        // Calculate compression ratio or percentage
        let compression_ratio = compressed_size as f64 / uncompressed_size as f64;
        let compression_percentage = (1.0 - compression_ratio) * 100.0;

        debug!("Uncompressed size: {} bytes", uncompressed_size);
        debug!("Compressed size: {} bytes", compressed_size);
        debug!("Compression ratio: {:.2}", compression_ratio);
        debug!("Compressed by: {:.2}%", compression_percentage);

        Ok(())
    }

    /// Register to task deletion events
    pub async fn subscribe_to_gc_task_events(
        &self,
    ) -> broadcast::Receiver<indexify_internal_api::GarbageCollectionTask> {
        self.data.gc_tasks_tx.subscribe()
    }

    //  START FORWARD INDEX READER METHODS INTERFACES
    pub fn get_latest_version_of_content(
        &self,
        content_id: &str,
    ) -> Result<Option<ContentMetadata>> {
        let txn = self.db.transaction();
        self.data
            .indexify_state
            .get_latest_version_of_content(content_id, &self.db, &txn)
            .map_err(|e| anyhow::anyhow!("Failed to get latest version of content: {}", e))
    }

    /// This method fetches a key from a specific column family
    pub async fn get_from_cf<T, K>(
        &self,
        column: StateMachineColumns,
        key: K,
    ) -> Result<Option<T>, anyhow::Error>
    where
        T: DeserializeOwned,
        K: AsRef<[u8]>,
    {
        self.data.indexify_state.get_from_cf(&self.db, column, key)
    }

    pub async fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<indexify_internal_api::Task>> {
        self.data
            .indexify_state
            .get_tasks_for_executor(executor_id, limit, &self.db)
            .map_err(|e| anyhow::anyhow!("Failed to get tasks for executor: {}", e))
    }

    pub async fn get_all_task_assignments(&self) -> Result<HashMap<TaskId, ExecutorId>> {
        self.data
            .indexify_state
            .get_all_task_assignments(&self.db)
            .map_err(|e| anyhow::anyhow!("Failed to get task assignments: {}", e))
    }

    pub async fn get_indexes_from_ids(
        &self,
        task_ids: HashSet<String>,
    ) -> Result<Vec<indexify_internal_api::Index>> {
        self.data
            .indexify_state
            .get_indexes_from_ids(task_ids, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
    ) -> Result<Option<Vec<indexify_internal_api::ExtractionPolicy>>> {
        self.data
            .indexify_state
            .get_extraction_policies_from_ids(extraction_policy_ids, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
    ) -> Result<Vec<ExecutorMetadata>> {
        self.data
            .indexify_state
            .get_executors_from_ids(executor_ids, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_content_from_ids(
        &self,
        content_ids: HashSet<String>,
    ) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_from_ids(content_ids, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_content_from_ids_with_version(
        &self,
        content_ids: HashSet<indexify_internal_api::ContentMetadataId>,
    ) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_from_ids_with_version(content_ids, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_content_tree_metadata(&self, content_id: &str) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_tree_metadata(content_id, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_content_tree_metadata_with_version(
        &self,
        content_id: &indexify_internal_api::ContentMetadataId,
    ) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_tree_metadata_with_version(content_id, &self.db)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_namespace(
        &self,
        namespace: &str,
    ) -> Result<Option<indexify_internal_api::Namespace>> {
        self.data.indexify_state.get_namespace(namespace, &self.db)
    }

    pub async fn get_schemas(&self, ids: HashSet<String>) -> Result<Vec<StructuredDataSchema>> {
        self.data.indexify_state.get_schemas(ids, &self.db)
    }

    pub async fn get_coordinator_addr(&self, node_id: NodeId) -> Result<Option<String>> {
        self.data
            .indexify_state
            .get_coordinator_addr(node_id, &self.db)
    }

    /// Test utility method to get all key-value pairs from a column family
    pub async fn get_all_rows_from_cf<V>(
        &self,
        column: StateMachineColumns,
    ) -> Result<Vec<(String, V)>, anyhow::Error>
    where
        V: DeserializeOwned,
    {
        self.data
            .indexify_state
            .get_all_rows_from_cf(column, &self.db)
    }

    //  END FORWARD INDEX READER METHOD INTERFACES

    //  START REVERSE INDEX READER METHOD INTERFACES
    pub async fn get_unassigned_tasks(&self) -> HashSet<TaskId> {
        self.data.indexify_state.get_unassigned_tasks()
    }

    pub async fn get_unprocessed_state_changes(&self) -> HashSet<StateChangeId> {
        self.data.indexify_state.get_unprocessed_state_changes()
    }

    pub async fn get_content_namespace_table(
        &self,
    ) -> HashMap<NamespaceName, HashSet<indexify_internal_api::ContentMetadataId>> {
        self.data.indexify_state.get_content_namespace_table()
    }

    pub async fn get_extraction_policies_table(&self) -> HashMap<NamespaceName, HashSet<String>> {
        self.data.indexify_state.get_extraction_policies_table()
    }

    pub async fn get_extractor_executors_table(
        &self,
    ) -> HashMap<ExtractorName, HashSet<ExecutorId>> {
        self.data.indexify_state.get_extractor_executors_table()
    }

    pub async fn get_namespace_index_table(&self) -> HashMap<NamespaceName, HashSet<String>> {
        self.data.indexify_state.get_namespace_index_table()
    }

    pub async fn get_unfinished_tasks_by_extractor(
        &self,
    ) -> HashMap<ExtractorName, HashSet<TaskId>> {
        self.data.indexify_state.get_unfinished_tasks_by_extractor()
    }

    pub async fn get_executor_running_task_count(&self) -> HashMap<ExecutorId, u64> {
        self.data.indexify_state.get_executor_running_task_count()
    }

    pub async fn get_schemas_by_namespace(&self) -> HashMap<NamespaceName, HashSet<SchemaId>> {
        self.data.indexify_state.get_schemas_by_namespace()
    }

    pub async fn are_content_tasks_completed(
        &self,
        content_id: &indexify_internal_api::ContentMetadataId,
    ) -> bool {
        self.data
            .indexify_state
            .are_content_tasks_completed(content_id)
    }

    pub fn get_content_children(
        &self,
        content_id: &indexify_internal_api::ContentMetadataId,
    ) -> HashSet<indexify_internal_api::ContentMetadataId> {
        self.data
            .indexify_state
            .content_children_table
            .get_children(content_id)
    }

    //  END REVERSE INDEX READER METHOD INTERFACES

    //  START REVERSE INDEX WRITER METHOD INTERFACES
    pub async fn insert_executor_running_task_count(&self, executor_id: &str, task_count: u64) {
        self.data
            .indexify_state
            .insert_executor_running_task_count(executor_id, task_count);
    }
    //  END REVERSE INDEX WRITER METHOD INTERFACES
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        debug!("Called build_snapshot");
        let (last_applied_log, last_membership) = {
            let guard = self.data.last_applied_log_id.read().await;
            let last_applied_log = *guard;
            let guard = self.data.last_membership.read().await;
            let last_membership = guard.clone();
            (last_applied_log, last_membership)
        };

        let indexify_state_json = {
            let indexify_state_snapshot = self.data.indexify_state.build_snapshot();
            JsonEncoder::encode(&indexify_state_snapshot)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!(
                "{}-{}-{}",
                last.leader_id,
                last.index,
                self.snapshot_idx.lock().unwrap()
            )
        } else {
            format!("--{}", self.snapshot_idx.lock().unwrap())
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: indexify_state_json.clone(),
        };

        self.set_current_snapshot_(snapshot)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(indexify_state_json)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        // let sm = self.data.read().await;
        // Ok((sm.last_applied_log_id, sm.last_membership.clone()))
        let (last_applied_log_id, last_membership) = {
            let guard = self.data.last_applied_log_id.read().await;
            let last_applied_log_id = *guard;
            let guard = self.data.last_membership.read().await;
            let last_membership = guard.clone();
            (last_applied_log_id, last_membership)
        };
        Ok((last_applied_log_id, last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = typ::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _timer = Timer::start(&self.metrics.state_machine_apply);
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);
        let mut change_events: Vec<StateChange> = Vec::new();

        for ent in entries {
            {
                let mut guard = self.data.last_applied_log_id.write().await;
                *guard = Some(ent.log_id);
            }
            let resp_value = None;
            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(req) => {
                    change_events.extend(req.new_state_changes.clone());

                    if let Err(e) = self
                        .data
                        .indexify_state
                        .apply_state_machine_updates(req.clone(), &self.db)
                    {
                        panic!("error applying state machine update: {}", e);
                    };

                    //  if the payload is a GC task, send it via channel
                    if let RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks } =
                        req.payload
                    {
                        let expected_receiver_count = self.data.gc_tasks_tx.receiver_count();
                        for gc_task in gc_tasks {
                            match self.data.gc_tasks_tx.send(gc_task.clone()) {
                                Ok(sent_count) => {
                                    if sent_count < expected_receiver_count {
                                        tracing::error!(
                                            "The gc task event did not reach all listeners"
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to send task {:?}: {}", gc_task, e);
                                }
                            }
                        }
                    }
                }
                EntryPayload::Membership(mem) => {
                    let mut guard = self.data.last_membership.write().await;
                    *guard = StoredMembership::new(Some(ent.log_id), mem.clone());
                }
            }

            replies.push(Response { value: resp_value });
        }
        for change_event in change_events {
            if let Err(err) = self.data.state_change_tx.send(change_event) {
                tracing::error!("error sending state change event: {}", err);
            }
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let mut l = self.snapshot_idx.lock().unwrap();
        *l += 1;
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        self.update_state_machine_(new_snapshot.clone()).await?;

        self.set_current_snapshot_(new_snapshot)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        debug!("Called get_current_snapshot");
        let x = self.get_current_snapshot_()?;
        Ok(x.map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct LogStore {
    db: Arc<OptimisticTransactionDB>,
}
type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
fn id_to_bin(id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.write_u64::<BigEndian>(id).unwrap();
    buf
}

fn bin_to_id(buf: &[u8]) -> u64 {
    (&buf[0..8]).read_u64::<BigEndian>().unwrap()
}

impl LogStore {
    fn store(&self) -> &ColumnFamily {
        self.db.cf_handle("store").unwrap()
    }

    fn logs(&self) -> &ColumnFamily {
        self.db.cf_handle("logs").unwrap()
    }

    fn flush(
        &self,
        subject: ErrorSubject<NodeId>,
        verb: ErrorVerb,
    ) -> Result<(), StorageIOError<NodeId>> {
        self.db
            .flush_wal(true)
            .map_err(|e| StorageIOError::new(subject, verb, AnyError::new(&e)))?;
        Ok(())
    }

    fn get_last_purged_(&self) -> StorageResult<Option<LogId<u64>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"last_purged_log_id")
            .map_err(|e| StorageIOError::read(&e))?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"last_purged_log_id",
                JsonEncoder::encode(&log_id).unwrap().as_slice(),
            )
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn set_committed_(
        &self,
        committed: &Option<LogId<NodeId>>,
    ) -> Result<(), StorageIOError<NodeId>> {
        let json = JsonEncoder::encode(committed).unwrap();

        self.db
            .put_cf(self.store(), b"committed", json)
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_committed_(&self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"committed")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }

    fn set_vote_(&self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.db
            .put_cf(self.store(), b"vote", JsonEncoder::encode(vote).unwrap())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self
            .db
            .get_cf(self.store(), b"vote")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => id_to_bin(*x),
            std::ops::Bound::Excluded(x) => id_to_bin(*x + 1),
            std::ops::Bound::Unbounded => id_to_bin(0),
        };
        self.db
            .iterator_cf(
                self.logs(),
                rocksdb::IteratorMode::From(&start, Direction::Forward),
            )
            .map(|res| {
                let (id, val) = res.unwrap();
                let entry: StorageResult<Entry<_>> =
                    JsonEncoder::decode(&val).map_err(|e| StorageError::IO {
                        source: StorageIOError::read_logs(&e),
                    });
                let id = bin_to_id(&id);

                assert_eq!(Ok(id), entry.as_ref().map(|e| e.log_id.index));
                (id, entry)
            })
            .take_while(|(id, _)| range.contains(id))
            .map(|x| x.1)
            .collect()
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = self
            .db
            .iterator_cf(self.logs(), rocksdb::IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(JsonEncoder::decode::<Entry<TypeConfig>>(&ent).ok()?.log_id)
            });

        let last_purged_log_id = self.get_last_purged_()?;

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        _committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.set_committed_(&_committed)?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let c = self.get_committed_()?;
        Ok(c)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.set_vote_(vote)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.get_vote_()
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<NodeId>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    self.logs(),
                    id,
                    JsonEncoder::encode(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
                )
                .map_err(|e| StorageIOError::write_logs(&e))?;
        }

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let from = id_to_bin(log_id.index);
        let to = id_to_bin(0xff_ff_ff_ff_ff_ff_ff_ff);
        self.db
            .delete_file_in_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_file_in_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub(crate) async fn new_storage<P: AsRef<Path>>(
    db_path: P,
    snapshot_path: P,
) -> (LogStore, Arc<StateMachineStore>) {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let store = ColumnFamilyDescriptor::new("store", Options::default());
    let logs = ColumnFamilyDescriptor::new("logs", Options::default());

    //  Create the column families for the state machine columns
    let sm_columns: Vec<String> = StateMachineColumns::iter()
        .map(|cf| cf.to_string())
        .collect();
    let sm_column_families: Vec<ColumnFamilyDescriptor> = sm_columns
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
        .collect();
    let mut all_column_families = vec![store, logs];
    all_column_families.extend(sm_column_families);

    let db: OptimisticTransactionDB =
        OptimisticTransactionDB::open_cf_descriptors(&db_opts, db_path, all_column_families)
            .unwrap();

    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };

    let snapshot_path = PathBuf::from(snapshot_path.as_ref());

    let sm_store = StateMachineStore::new(db, snapshot_path).await.unwrap();

    (log_store, Arc::new(sm_store))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::{raft::InstallSnapshotRequest, testing::log_id, SnapshotMeta, Vote};

    use crate::{
        state::{
            self,
            store::{
                serializer::{JsonEncode, JsonEncoder},
                state_machine_objects::IndexifyStateSnapshot,
            },
        },
        test_utils::RaftTestCluster,
    };

    /// This is a dummy test which forces building a snapshot on the cluster by
    /// passing in some overrides Manually check that the snapshot file was
    /// actually created. Still need to find a way to force reading and
    /// deserialization
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_install_snapshot() -> anyhow::Result<()> {
        let cluster = RaftTestCluster::new(3, None).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let indexify_state = IndexifyStateSnapshot::default();
        let serialized_state =
            JsonEncoder::encode(&indexify_state).expect("Failed to serialize the data");
        let install_snapshot_req: InstallSnapshotRequest<state::TypeConfig> =
            InstallSnapshotRequest {
                vote: Vote::new_committed(2, 1),
                meta: SnapshotMeta {
                    snapshot_id: "ss1".into(),
                    last_log_id: Some(log_id(1, 0, 6)),
                    last_membership: Default::default(),
                },
                offset: 0,
                data: serialized_state,
                done: true,
            };
        let node = cluster.get_raft_node(2)?;
        node.forwardable_raft
            .raft
            .install_snapshot(install_snapshot_req)
            .await?;
        Ok(())
    }
}
