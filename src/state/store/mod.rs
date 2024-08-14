use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs::{self, File},
    io::{BufReader, Cursor, Read},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, RwLockReadGuard},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use compat::init_task_analytics;
use flate2::bufread::ZlibDecoder;
use indexify_internal_api::{
    ContentMetadata,
    ContentMetadataId,
    ContentOffset,
    ExecutorMetadata,
    ExtractionGraph,
    ExtractionGraphAnalytics,
    ExtractionGraphLink,
    ExtractionPolicy,
    NamespaceName,
    StructuredDataSchema,
    Task,
    TaskAnalytics,
};
use indexify_proto::indexify_coordinator::{self};
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
use requests::{
    CreateOrUpdateContentEntry,
    StateMachineUpdateRequest,
    V1RequestPayload,
    V1StateMachineUpdateRequest,
};
use rocksdb::{
    checkpoint::Checkpoint,
    ColumnFamily,
    ColumnFamilyDescriptor,
    DBCommon,
    Direction,
    IteratorMode,
    OptimisticTransactionDB,
    Options,
    ReadOptions,
    SingleThreaded,
};
use serde::{de::DeserializeOwned, Deserialize};
use strum::{AsRefStr, IntoEnumIterator};
use thiserror::Error;
use tokio::sync::{broadcast, RwLock};
use tracing::debug;
use uuid::Uuid;

type Node = BasicNode;

use indexify_internal_api::StateChangeId;

use self::{
    requests::RequestPayload,
    serializer::{JsonEncode, JsonEncoder},
    state_machine_objects::IndexifyState,
};
use super::{typ, NodeId, SnapshotData, TypeConfig};
use crate::{
    api::NewContentStreamStart,
    coordinator::ContentStream,
    metrics::{state_machine::Metrics, Timer},
    utils::OptionInspectNone,
};

pub type TaskId = String;
pub type ContentId = String;
pub type ExecutorId = String;
pub type ExecutorIdRef<'a> = &'a str;
pub type ExtractionEventId = String;
pub type ExtractionPolicyId = String;
pub type ExtractorName = String;
pub type ContentType = String;
pub type ExtractionGraphId = String;
pub type SchemaId = String;

mod compat;
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
    ExtractionGraphs,                   //  ExtractionGraphId -> ExtractionGraph
    RaftState,                          //  Raft state
    ExtractionGraphLinks,               //  Namespace/Graph/Source -> Linked graph name
    ChangeIdContentIndex,               //  ChangeId -> ContentId
    TaskAnalytics,                      //  Namespace_Graph_Policy -> TaskAnalytics
}

const LAST_MEMBERSHIP_KEY: &[u8] = b"last_membership";
const LAST_APPLIED_LOG_ID_KEY: &[u8] = b"last_applied_log_id";
const STORE_VERSION: &[u8] = b"store_version";
const CURRENT_STORE_VERSION: u64 = 4;
const LOG_STORE_LOGS_COLUMN: &str = "logs";
const LOG_STORE_STORE_COLUMN: &str = "store";

impl StateMachineColumns {
    pub fn cf<'a>(&'a self, db: &'a OptimisticTransactionDB) -> &'a ColumnFamily {
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

    pub last_membership: std::sync::RwLock<StoredMembership<NodeId, Node>>,

    /// State built from applying the raft log
    pub indexify_state: IndexifyState,

    state_change_tx: Arc<tokio::sync::watch::Sender<StateChangeId>>,

    gc_tasks_tx: broadcast::Sender<indexify_internal_api::GarbageCollectionTask>,
}

pub struct FilterResponse<T> {
    pub items: Vec<T>,
    pub total: usize,
}

impl TryFrom<FilterResponse<ContentMetadata>> for indexify_coordinator::ListContentResponse {
    type Error = anyhow::Error;

    fn try_from(value: FilterResponse<ContentMetadata>) -> Result<Self> {
        let content_list: Result<_, _> = value
            .items
            .into_iter()
            .map(|content| content.try_into())
            .collect();
        Ok(Self {
            content_list: content_list?,
            total: value.total as u64,
        })
    }
}

impl TryFrom<FilterResponse<Task>> for indexify_coordinator::ListTasksResponse {
    type Error = anyhow::Error;

    fn try_from(value: FilterResponse<Task>) -> Result<Self> {
        let tasks: Result<_, _> = value
            .items
            .into_iter()
            .map(|task| task.try_into())
            .collect();
        Ok(Self {
            tasks: tasks?,
            total: value.total as u64,
        })
    }
}

pub fn filter_cf<T, F>(
    db: &OptimisticTransactionDB,
    column: StateMachineColumns,
    filter: F,
    start: Option<&[u8]>,
    limit: Option<usize>,
) -> Result<FilterResponse<T>, anyhow::Error>
where
    T: DeserializeOwned,
    F: Fn(&T) -> bool,
{
    let cf = column.cf(db);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let mode = match start {
        Some(start) => IteratorMode::From(start, Direction::Forward),
        None => IteratorMode::Start,
    };
    let iter = db.iterator_cf_opt(cf, read_options, mode);
    let mut items = Vec::new();
    let mut total = 0;
    let limit = limit.unwrap_or(usize::MAX);
    for kv in iter {
        if let Ok((_, value)) = kv {
            let item = JsonEncoder::decode::<T>(&value)?;
            if filter(&item) {
                total += 1;
                if items.len() < limit {
                    items.push(item);
                    if items.len() >= limit {
                        break;
                    }
                }
            }
        } else {
            return Err(anyhow!("error reading db"));
        }
    }
    Ok(FilterResponse { items, total })
}

/// This method fetches a key from a specific column family
pub fn get_from_cf<T, K>(
    db: &OptimisticTransactionDB,
    column: StateMachineColumns,
    key: K,
) -> Result<Option<T>, anyhow::Error>
where
    T: DeserializeOwned,
    K: AsRef<[u8]>,
{
    let result_bytes = match db.get_cf(column.cf(db), key)? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let result = JsonEncoder::decode::<T>(&result_bytes)
        .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

    Ok(Some(result))
}

fn delete_incomplete_snapshots(path: impl AsRef<Path>) -> std::io::Result<()> {
    let entries = fs::read_dir(path)?;

    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                fs::remove_dir_all(path)?;
            }
        }
    }

    Ok(())
}

// Read snapshot metadata from checkpoint directory
fn snapshot_meta(path: &PathBuf) -> Result<SnapshotMeta<NodeId, Node>, StorageError<NodeId>> {
    let db = open_db(path).map_err(|e| {
        StorageIOError::read_snapshot(None, anyhow!("Failed to open snapshot: {}", e))
    })?;
    let last_membership = get_from_cf(&db, StateMachineColumns::RaftState, LAST_MEMBERSHIP_KEY)
        .map_err(|e| StorageIOError::read_snapshot(None, e))?;
    let last_applied_log_id =
        get_from_cf(&db, StateMachineColumns::RaftState, LAST_APPLIED_LOG_ID_KEY)
            .map_err(|e| StorageIOError::read_snapshot(None, e))?;

    Ok(SnapshotMeta {
        last_log_id: last_applied_log_id,
        last_membership: last_membership.unwrap_or_default(),
        snapshot_id: "0".to_string(),
    })
}

fn read_snapshots(
    path: impl AsRef<Path>,
) -> Result<Vec<Snapshot<TypeConfig>>, StorageError<NodeId>> {
    let entries = fs::read_dir(path).map_err(|e| StorageIOError::read_snapshot(None, &e))?;

    let mut snapshots = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        if entry
            .file_type()
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?
            .is_dir()
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("tmp") {
                let snapshot = Snapshot {
                    meta: snapshot_meta(&path)?,
                    snapshot: Box::new(SnapshotData { snapshot_dir: path }),
                };
                snapshots.push(snapshot);
            }
        }
    }

    snapshots.sort_by(|a, b| a.meta.last_log_id.cmp(&b.meta.last_log_id));

    Ok(snapshots)
}

fn add_link(graph_links: &mut HashMap<String, HashSet<String>>, link: &ExtractionGraphLink) {
    graph_links
        .entry(link.node.graph_name.clone())
        .or_default()
        .insert(link.graph_name.clone());
}

// Returns a set of graph edges in the form (source, target set).
// For cycle detection keep a single link between two graphs,
// no need to distinguish by ContentSource.
fn read_graph_links(
    db: &OptimisticTransactionDB,
    namespace: &str,
) -> Result<HashMap<String, HashSet<String>>> {
    let cf_handle = StateMachineColumns::ExtractionGraphLinks.cf(db);
    let mut graph_links: HashMap<String, HashSet<String>> = HashMap::new();
    for v in db.iterator_cf(cf_handle, IteratorMode::Start) {
        let (key, _) = v.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        let link: ExtractionGraphLink = JsonEncoder::decode(&key)?;
        if link.node.namespace != namespace {
            continue;
        }
        add_link(&mut graph_links, &link);
    }
    Ok(graph_links)
}

pub struct StateMachineStore {
    pub data: StateMachineData,

    db: Arc<std::sync::RwLock<OptimisticTransactionDB>>,

    pub state_change_rx: tokio::sync::watch::Receiver<StateChangeId>,

    db_path: PathBuf,

    snapshot: std::sync::RwLock<Option<Snapshot<TypeConfig>>>,

    metrics: Metrics,
}

fn content_stream_items(
    state: &IndexifyState,
    db: &OptimisticTransactionDB,
    offset: ContentOffset,
) -> Result<Vec<ContentMetadata>> {
    let key = offset.0.to_be_bytes();
    let iter = db.iterator_cf(
        StateMachineColumns::ChangeIdContentIndex.cf(db),
        IteratorMode::From(&key, Direction::Forward),
    );
    let mut ids: Vec<String> = Vec::new();
    for item in iter {
        match item {
            Ok((_key, value)) => {
                let id = String::from_utf8(value.to_vec())?;
                ids.push(id);
            }
            Err(e) => {
                return Err(anyhow!("Failed to read content stream: {}", e));
            }
        }
        if ids.len() >= 100 {
            break;
        }
    }
    state
        .get_content_from_ids(ids, db)
        .map_err(|e| anyhow!("Failed to get content: {}", e))
}

pub fn new_content_stream(
    store: Arc<StateMachineStore>,
    start: NewContentStreamStart,
) -> ContentStream {
    let mut offset = match start {
        NewContentStreamStart::FromOffset(offset) => offset.next(),
        NewContentStreamStart::FromLast => {
            *store.data.indexify_state.content_offset.lock().unwrap()
        }
    };
    let stream = async_stream::stream! {
        let mut rx = store.data.indexify_state.new_content_channel.subscribe();
        loop {
            let items = content_stream_items(&store.data.indexify_state, &store.db.read().unwrap(), offset);
            match items {
                Ok(items) => {
                    if items.is_empty() {
                        let _ = rx.recv().await;
                    } else {
                        offset = items.last().unwrap().change_offset.next();
                        for item in items {
                            yield Ok(item);
                        }
                    }
                }
                Err(e) => {
                    yield Err(e);
                    return;
                }
            }
        }
    };
    Box::pin(stream)
}

impl StateMachineStore {
    async fn new(
        db_path: PathBuf,
        log_store: &LogStore,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let (tx, rx) = tokio::sync::watch::channel(StateChangeId::new(std::u64::MAX));
        let (gc_tasks_tx, _) = broadcast::channel(100);

        delete_incomplete_snapshots(&db_path).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Delete, e)
        })?;

        let mut snapshots = read_snapshots(&db_path)?;
        if snapshots.is_empty() {
            // Create new database if no valid directory found
            let db_name = Uuid::new_v4().to_string();
            let db_path = db_path.join(db_name);
            let db = open_db(&db_path)?;
            db.put_cf(
                StateMachineColumns::RaftState.cf(&db),
                STORE_VERSION,
                CURRENT_STORE_VERSION.to_be_bytes(),
            )
            .map_err(|e| {
                StorageIOError::write_state_machine(anyhow!("failed to init db: {}", e))
            })?;

            snapshots.push(Snapshot {
                meta: Default::default(),
                snapshot: Box::new(SnapshotData {
                    snapshot_dir: db_path,
                }),
            });
        }

        // live db is the directory with the latest applied log id
        let live_snapshot = snapshots.pop().unwrap();

        // Remove all but last snapshot directories
        let snapshot = if !snapshots.is_empty() {
            for snapshot in snapshots[..snapshots.len() - 1].iter() {
                fs::remove_dir_all(&snapshot.snapshot.snapshot_dir).map_err(|e| {
                    StorageIOError::read_snapshot(None, anyhow!("Failed to remove snapshot: {}", e))
                })?;
            }
            Some(snapshots.pop().unwrap())
        } else {
            None
        };

        let db = open_db(&live_snapshot.snapshot.snapshot_dir)?;

        let store_version = db
            .get_cf(StateMachineColumns::RaftState.cf(&db), STORE_VERSION)
            .map_err(|e| {
                StorageIOError::read_state_machine(anyhow!("failed to read version: {}", e))
            })?;
        if let Some(store_version) = store_version {
            let mut cursor = Cursor::new(store_version);
            let store_version = cursor.read_u64::<BigEndian>().map_err(|e| {
                StorageIOError::read_state_machine(anyhow!("failed to read version: {}", e))
            })?;
            if store_version > CURRENT_STORE_VERSION {
                return Err(StorageIOError::read_state_machine(anyhow!(
                    "Store version {} mismatch, current {}",
                    store_version,
                    CURRENT_STORE_VERSION
                ))
                .into());
            } else if store_version == 3 {
                compat::convert_v3(&db, &log_store.db)?;
            } else if store_version == 2 {
                compat::convert_v2(&db, &log_store.db)?;
            } else if store_version == 1 {
                return Err(StorageIOError::read_state_machine(anyhow!(
                    "Store version 1 is not supported"
                ))
                .into());
            }
        } else {
            return Err(
                StorageIOError::read_state_machine(anyhow!("Store version not found")).into(),
            );
        }

        let sm = Self {
            data: StateMachineData {
                last_applied_log_id: RwLock::new(live_snapshot.meta.last_log_id),
                last_membership: std::sync::RwLock::new(live_snapshot.meta.last_membership),
                indexify_state: IndexifyState::default(),
                state_change_tx: Arc::new(tx),
                gc_tasks_tx,
            },
            db: Arc::new(std::sync::RwLock::new(db)),
            state_change_rx: rx,
            db_path,
            snapshot: std::sync::RwLock::new(snapshot),
            metrics: Metrics::new(),
        };

        sm.data
            .indexify_state
            .rebuild_reverse_indexes(&sm.db.read().unwrap())
            .map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    anyhow!("failed to rebuild cache: {}", e),
                )
            })?;

        Ok(sm)
    }

    pub fn get_db(&self) -> RwLockReadGuard<OptimisticTransactionDB> {
        self.db.read().unwrap()
    }

    // Check if adding a link will create a cycle in the graph.
    pub fn creates_cycle(&self, link: &ExtractionGraphLink) -> Result<bool> {
        let db = self.db.read().unwrap();
        let mut graph_links = read_graph_links(&db, &link.node.namespace)?;
        add_link(&mut graph_links, link);
        let mut visited = HashSet::new();
        let mut stack = vec![link.node.graph_name.clone()];

        while let Some(graph) = stack.pop() {
            if visited.contains(&graph) {
                return Ok(true);
            }
            visited.insert(graph.clone());
            if let Some(links) = graph_links.get(&graph) {
                stack.extend(links.iter().cloned());
            }
        }
        Ok(false)
    }

    fn update_applied_log_id<'a>(
        &self,
        log_id: LogId<NodeId>,
        txn: &'a rocksdb::Transaction<'a, OptimisticTransactionDB>,
    ) -> Result<(), StateMachineError> {
        let applied_data = JsonEncoder::encode(&log_id).map_err(|e| {
            StateMachineError::SerializationError(format!("Failed to serialize log id: {}", e))
        })?;
        let db = self.db.read().unwrap();
        txn.put_cf(
            StateMachineColumns::RaftState.cf(&db),
            LAST_APPLIED_LOG_ID_KEY,
            applied_data,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Failed to write log id: {}", e)))
    }

    fn update_membership(
        &self,
        membership: StoredMembership<NodeId, Node>,
        txn: rocksdb::Transaction<'_, OptimisticTransactionDB>,
    ) -> Result<(), StateMachineError> {
        {
            let mut guard = self.data.last_membership.write().unwrap();
            *guard = membership.clone();
        }
        let membership_data = JsonEncoder::encode(&membership).map_err(|e| {
            StateMachineError::SerializationError(format!("Failed to serialize membership: {}", e))
        })?;
        txn.put_cf(
            StateMachineColumns::RaftState.cf(&self.db.read().unwrap()),
            LAST_MEMBERSHIP_KEY,
            membership_data,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!("Failed to write membership: {}", e))
        })?;
        txn.commit().map_err(|e| {
            StateMachineError::TransactionError(format!("Failed to commit transaction: {}", e))
        })
    }

    fn send_gc_tasks(&self, gc_tasks: Vec<indexify_internal_api::GarbageCollectionTask>) {
        let expected_receiver_count = self.data.gc_tasks_tx.receiver_count();
        for gc_task in gc_tasks {
            match self.data.gc_tasks_tx.send(gc_task.clone()) {
                Ok(sent_count) => {
                    if sent_count < expected_receiver_count {
                        tracing::error!("The gc task event did not reach all listeners");
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to send task {:?}: {}", gc_task, e);
                }
            }
        }
    }

    async fn apply_entry(&self, entry: typ::Entry) -> Result<Option<String>, StateMachineError> {
        {
            let mut guard = self.data.last_applied_log_id.write().await;
            *guard = Some(entry.log_id);
        }
        let db = self.db.read().unwrap();
        let txn = db.transaction();
        self.update_applied_log_id(entry.log_id, &txn)?;
        match entry.payload {
            EntryPayload::Blank => {
                txn.commit().map_err(|e| {
                    StateMachineError::TransactionError(format!(
                        "Failed to commit transaction: {}",
                        e
                    ))
                })?;
            }
            // TODO: edwin
            EntryPayload::Normal(req) => {
                let change_id =
                    self.data
                        .indexify_state
                        .apply_state_machine_updates(req.clone(), &db, txn)?;
                if let Some(change_id) = change_id {
                    let _ = self.data.state_change_tx.send(change_id);
                }

                //  if the payload is a GC task, send it via channel
                match req.payload {
                    RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks } => {
                        self.send_gc_tasks(gc_tasks);
                    }
                    RequestPayload::DeleteExtractionGraph {
                        graph_id: _,
                        gc_task,
                    } => {
                        self.send_gc_tasks(vec![gc_task]);
                    }
                    _ => {}
                }
            }
            EntryPayload::Membership(membership) => {
                let membership = StoredMembership::new(Some(entry.log_id), membership);
                self.update_membership(membership, txn)?;
            }
        }
        Ok(None)
    }

    /// Register to task deletion events
    pub async fn subscribe_to_gc_task_events(
        &self,
    ) -> broadcast::Receiver<indexify_internal_api::GarbageCollectionTask> {
        self.data.gc_tasks_tx.subscribe()
    }

    //  START FORWARD INDEX READER METHODS INTERFACES
    pub fn get_extraction_graph_links(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Vec<indexify_coordinator::ExtractionGraphLink>> {
        let db = self.db.read().unwrap();
        let txn = db.transaction();
        let mut graph_links = Vec::new();
        for v in txn.iterator_cf(
            StateMachineColumns::ExtractionGraphLinks.cf(&db),
            IteratorMode::Start,
        ) {
            let (key, _) = v.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let link: ExtractionGraphLink = JsonEncoder::decode(&key)?;
            if link.node.namespace == namespace && link.node.graph_name == graph_name {
                graph_links.push(indexify_coordinator::ExtractionGraphLink {
                    linked_graph_name: link.graph_name,
                    content_source: link.node.source.into(),
                });
            }
        }
        Ok(graph_links)
    }

    pub fn get_latest_version_of_content(
        &self,
        content_id: &str,
    ) -> Result<Option<ContentMetadata>> {
        let db = self.db.read().unwrap();
        let txn = db.transaction();
        self.data
            .indexify_state
            .get_latest_version_of_content(content_id, &db, &txn)
            .map_err(|e| anyhow::anyhow!("Failed to get latest version of content: {}", e))
    }

    /// This method fetches a key from a specific column family
    pub fn get_from_cf<T, K>(
        &self,
        column: StateMachineColumns,
        key: K,
    ) -> Result<Option<T>, anyhow::Error>
    where
        T: DeserializeOwned,
        K: AsRef<[u8]>,
    {
        get_from_cf(&self.db.read().unwrap(), column, key)
    }

    pub async fn list_active_contents(&self, namespace: &str) -> Result<Vec<String>> {
        self.data
            .indexify_state
            .list_active_contents(&self.db.read().unwrap(), namespace)
            .map_err(|e| anyhow::anyhow!("Failed to list active contents: {}", e))
    }

    pub async fn list_tasks<F>(
        &self,
        filter: F,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<Task>>
    where
        F: Fn(&Task) -> bool,
    {
        filter_cf(
            &self.db.read().unwrap(),
            StateMachineColumns::Tasks,
            filter,
            start_id.as_deref().map(|s| s.as_bytes()),
            limit.map(|l| l as usize),
        )
    }

    pub async fn get_graph_analytics(
        &self,
        namespace: &str,
        graph_name: &str,
    ) -> Result<Option<ExtractionGraphAnalytics>> {
        let mut read_options = ReadOptions::default();
        read_options.set_readahead_size(4_194_304);
        let prefix = format!("{}_{}_", namespace, graph_name);
        let mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);

        let db = self.db.read().unwrap();

        let itr = db.iterator_cf_opt(
            StateMachineColumns::TaskAnalytics.cf(&db),
            read_options,
            mode,
        );

        let mut extraction_graph_analytics = ExtractionGraphAnalytics::default();

        for kv in itr {
            let (key, value) = kv.map_err(|e| anyhow::anyhow!("Failed to read db: {}", e))?;
            let key = String::from_utf8(key.to_vec())?;
            let extraction_policy_name = key.strip_prefix(&prefix);
            if let Some(extraction_policy_name) = extraction_policy_name {
                let task_analytics: TaskAnalytics = JsonEncoder::decode(&value)?;
                extraction_graph_analytics
                    .task_analytics
                    .insert(extraction_policy_name.to_string(), task_analytics);
            }
        }

        Ok(Some(extraction_graph_analytics))
    }

    pub async fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<Task>> {
        self.data
            .indexify_state
            .get_tasks_for_executor(executor_id, limit, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!("Failed to get tasks for executor: {}", e))
    }

    pub async fn get_all_task_assignments(&self) -> Result<HashMap<TaskId, ExecutorId>> {
        self.data
            .indexify_state
            .get_all_task_assignments(&self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!("Failed to get task assignments: {}", e))
    }

    pub async fn get_indexes_from_ids(
        &self,
        task_ids: HashSet<String>,
    ) -> Result<Vec<indexify_internal_api::Index>> {
        self.data
            .indexify_state
            .get_indexes_from_ids(task_ids, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
    ) -> Result<Vec<ExtractionPolicy>> {
        self.data
            .indexify_state
            .get_extraction_policies_from_ids(extraction_policy_ids, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_extraction_policy_by_names(
        &self,
        namespace: &str,
        graph_name: &str,
        policy_names: &HashSet<String>,
    ) -> Result<Vec<Option<ExtractionPolicy>>> {
        self.data
            .indexify_state
            .get_extraction_policy_by_names(
                namespace,
                graph_name,
                policy_names,
                &self.db.read().unwrap(),
            )
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
    ) -> Result<Vec<ExecutorMetadata>> {
        self.data
            .indexify_state
            .get_executors_from_ids(executor_ids, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_content_from_ids(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_from_ids(content_ids, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn list_content(
        &self,
        filter: impl Fn(&ContentMetadata) -> bool,
        start_id: Option<String>,
        limit: Option<u64>,
    ) -> Result<FilterResponse<ContentMetadata>> {
        let s = start_id.as_deref().map(|s| s.as_bytes());
        filter_cf(
            &self.db.read().unwrap(),
            StateMachineColumns::ContentTable,
            filter,
            s,
            limit.map(|l| l as usize),
        )
    }

    pub async fn get_content_by_id_and_version(
        &self,
        content_id: &ContentMetadataId,
    ) -> Result<Option<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_by_id_and_version(&self.db.read().unwrap(), content_id)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_content_tree_metadata(&self, content_id: &str) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_tree_metadata(content_id, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_content_tree_metadata_with_version(
        &self,
        content_id: &ContentMetadataId,
    ) -> Result<Vec<ContentMetadata>> {
        self.data
            .indexify_state
            .get_content_tree_metadata_with_version(content_id, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn namespace_exists(&self, namespace: &str) -> Result<bool> {
        self.data
            .indexify_state
            .namespace_exists(namespace, &self.db.read().unwrap())
    }

    pub async fn get_schemas(&self, ids: HashSet<String>) -> Result<Vec<StructuredDataSchema>> {
        self.data
            .indexify_state
            .get_schemas(ids, &self.db.read().unwrap())
    }

    pub fn get_extraction_graphs(
        &self,
        extraction_graph_ids: &[impl AsRef<str>],
    ) -> Result<Vec<Option<ExtractionGraph>>> {
        let db = self.db.read().unwrap();
        let txn = db.transaction();
        self.data
            .indexify_state
            .get_extraction_graphs(extraction_graph_ids, &db, &txn)
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_extraction_graphs_by_name(
        &self,
        namespace: &str,
        graph_names: &[impl AsRef<str>],
    ) -> Result<Vec<Option<ExtractionGraph>>> {
        self.data
            .indexify_state
            .get_extraction_graphs_by_name(namespace, graph_names, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_coordinator_addr(&self, node_id: NodeId) -> Result<Option<String>> {
        self.data
            .indexify_state
            .get_coordinator_addr(node_id, &self.db.read().unwrap())
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
            .get_all_rows_from_cf(column, &self.db.read().unwrap())
            .map_err(|e| anyhow::anyhow!("Failed to get all rows from column family: {}", e))
    }

    //  END FORWARD INDEX READER METHOD INTERFACES

    //  START REVERSE INDEX READER METHOD INTERFACES
    pub async fn get_unassigned_tasks(&self) -> HashMap<TaskId, SystemTime> {
        self.data.indexify_state.get_unassigned_tasks()
    }

    pub async fn get_unprocessed_state_changes(&self) -> HashSet<StateChangeId> {
        self.data.indexify_state.get_unprocessed_state_changes()
    }

    #[cfg(test)]
    pub fn get_content_namespace_table(
        &self,
    ) -> Result<HashMap<NamespaceName, HashSet<ContentMetadataId>>> {
        let db = self.db.read().unwrap();
        let cf = StateMachineColumns::ContentTable.cf(&db);
        let mut res: HashMap<NamespaceName, HashSet<ContentMetadataId>> = HashMap::new();
        for val in db.iterator_cf(cf, IteratorMode::Start) {
            let (_, val) = val?;
            let content: ContentMetadata = JsonEncoder::decode(&val)?;
            res.entry(content.namespace).or_default().insert(content.id);
        }
        Ok(res)
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

    pub async fn get_schemas_by_namespace(
        &self,
    ) -> HashMap<NamespaceName, HashSet<ExtractionGraphId>> {
        self.data.indexify_state.get_schemas_by_namespace()
    }

    pub async fn are_content_tasks_completed(&self, content_id: &ContentMetadataId) -> bool {
        self.data
            .indexify_state
            .are_content_tasks_completed(content_id)
    }

    pub fn get_content_children(
        &self,
        content_id: &ContentMetadataId,
    ) -> HashSet<ContentMetadataId> {
        self.data
            .indexify_state
            .content_children_table
            .get_children(content_id)
    }

    //  END REVERSE INDEX READER METHOD INTERFACES
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied_log, last_membership) = {
            let guard = self.data.last_applied_log_id.read().await;
            let last_applied_log = *guard;
            let guard = self.data.last_membership.read().unwrap();
            let last_membership = guard.clone();
            (last_applied_log, last_membership)
        };

        // Make snapshot ids globally unique since db snapshot reprentation is not
        // guaranteed to be same even if contents is same.
        let snapshot_id = Uuid::new_v4().to_string();

        // Create a checkpoint of the database
        let db = self.db.read().unwrap();
        let checkpoint = Checkpoint::new(&db).map_err(|e| {
            StorageIOError::write_snapshot(None, anyhow!("failed to create checkpoint: {}", e))
        })?;
        let snapshot_tmp_id = format!("{}.tmp", snapshot_id);
        let snapshot_tmp_dir = self.db_path.join(snapshot_tmp_id);
        checkpoint
            .create_checkpoint(&snapshot_tmp_dir)
            .map_err(|e| {
                StorageIOError::write_snapshot(None, anyhow!("Failed to create checkpoint: {}", e))
            })?;

        // Move snapshot to final location
        let snapshot_dir = self.db_path.join(&snapshot_id);
        fs::rename(&snapshot_tmp_dir, &snapshot_dir).map_err(|e| {
            StorageIOError::write_snapshot(None, anyhow!("Failed to move snapshot: {}", e))
        })?;

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        if let Some(prev_snapshot) = (*self.snapshot.read().unwrap()).as_ref() {
            fs::remove_dir_all(&prev_snapshot.snapshot.snapshot_dir).map_err(|e| {
                StorageIOError::write_snapshot(
                    Some(meta.signature()),
                    anyhow!("Failed to remove previous snapshot: {}", e),
                )
            })?;
        }

        let snapshot = Snapshot {
            meta,
            snapshot: Box::new(SnapshotData { snapshot_dir }),
        };

        *self.snapshot.write().unwrap() = Some(snapshot.clone());

        Ok(snapshot)
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let (last_applied_log_id, last_membership) = {
            let guard = self.data.last_applied_log_id.read().await;
            let last_applied_log_id = *guard;
            let guard = self.data.last_membership.read().unwrap();
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

        for entry in entries {
            let resp_value = match self.apply_entry(entry).await {
                Ok(resp) => resp,
                Err(e) => {
                    panic!("Failed to apply entry: {}", e);
                }
            };
            replies.push(Response { value: resp_value });
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    // Create directory to receive snapshot into
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        let name = Uuid::new_v4().to_string();
        let snapshot_dir = self.db_path.join(format!("{}.tmp", name));
        fs::create_dir_all(&snapshot_dir).map_err(|e| {
            StorageIOError::write_snapshot(None, anyhow!("Failed to create directory: {}", e))
        })?;

        Ok(Box::new(SnapshotData { snapshot_dir }))
    }

    // Install snapshot from received rocksdb checkpoint.
    // Open snapshot db and checkpoint it into the new database directory,
    // so we have both current database and snapshot preserved.
    // Remove old database when finished.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_db_path = self.db_path.join(Uuid::new_v4().to_string());

        {
            let snap_db = open_db(&snapshot.snapshot_dir).map_err(|e| {
                StorageIOError::write_snapshot(
                    Some(meta.signature()),
                    anyhow!("Failed to open checkpoint db: {}", e),
                )
            })?;

            let checkpoint = Checkpoint::new(&snap_db).map_err(|e| {
                StorageIOError::write_snapshot(
                    Some(meta.signature()),
                    anyhow!("Failed to create checkpoint: {}", e),
                )
            })?;

            checkpoint.create_checkpoint(&new_db_path).map_err(|e| {
                StorageIOError::write_snapshot(
                    Some(meta.signature()),
                    anyhow!("Failed to create checkpoint: {}", e),
                )
            })?;
        }

        let db = open_db(&new_db_path).map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                anyhow!("Failed to open db: {}", e),
            )
        })?;

        // Move snapshot to final location.
        let snapshot_dir_without_tmp = snapshot.snapshot_dir.with_extension("");
        fs::rename(snapshot.snapshot_dir, &snapshot_dir_without_tmp).map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                anyhow!("Failed to move db: {}", e),
            )
        })?;

        let path = {
            let mut guard = self.db.write().unwrap();
            let path = guard.path().to_path_buf();
            *guard = db;
            path
        };

        fs::remove_dir_all(path).map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                anyhow!("Failed to remove old db: {}", e),
            )
        })?;

        self.data
            .indexify_state
            .rebuild_reverse_indexes(&self.db.read().unwrap())
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write(&e),
            })?;

        {
            let mut guard = self.data.last_applied_log_id.write().await;
            *guard = meta.last_log_id;
        }
        {
            let mut guard = self.data.last_membership.write().unwrap();
            *guard = meta.last_membership.clone();
        }

        Ok(())
    }

    // Find the snapshot with the highest log id.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self.snapshot.read().unwrap().clone())
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

fn store_column(db: &OptimisticTransactionDB) -> &ColumnFamily {
    db.cf_handle("store").unwrap()
}

fn logs_column(db: &OptimisticTransactionDB) -> &ColumnFamily {
    db.cf_handle(LOG_STORE_LOGS_COLUMN).unwrap()
}

impl LogStore {
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

    fn get_last_purged_(&self, db: &OptimisticTransactionDB) -> StorageResult<Option<LogId<u64>>> {
        Ok(db
            .get_cf(store_column(db), b"last_purged_log_id")
            .map_err(|e| StorageIOError::read(&e))?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                store_column(&self.db),
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
            .put_cf(store_column(&self.db), b"committed", json)
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_committed_(&self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self
            .db
            .get_cf(store_column(&self.db), b"committed")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }

    fn set_vote_(&self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.db
            .put_cf(
                store_column(&self.db),
                b"vote",
                JsonEncoder::encode(vote).unwrap(),
            )
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?;

        self.flush(ErrorSubject::Vote, ErrorVerb::Write)?;
        Ok(())
    }

    fn get_vote_(&self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self
            .db
            .get_cf(store_column(&self.db), b"vote")
            .map_err(|e| StorageError::IO {
                source: StorageIOError::write_vote(&e),
            })?
            .and_then(|v| JsonEncoder::decode(&v).ok()))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
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
                logs_column(&self.db),
                IteratorMode::From(&start, Direction::Forward),
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
            .iterator_cf(logs_column(&self.db), IteratorMode::End)
            .next()
            .and_then(|res| {
                let (_, ent) = res.unwrap();
                Some(JsonEncoder::decode::<Entry<TypeConfig>>(&ent).ok()?.log_id)
            });

        let last_purged_log_id = self.get_last_purged_(&self.db)?;

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
    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        for entry in entries {
            let id = id_to_bin(entry.log_id.index);
            assert_eq!(bin_to_id(&id), entry.log_id.index);
            self.db
                .put_cf(
                    logs_column(&self.db),
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
            .delete_file_in_range_cf(logs_column(&self.db), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_file_in_range_cf(logs_column(&self.db), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

fn open_db_with_columns<I>(
    path: &Path,
    columns: I,
) -> Result<OptimisticTransactionDB, StorageError<NodeId>>
where
    I: IntoIterator<Item = ColumnFamilyDescriptor>,
{
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);

    let db: OptimisticTransactionDB =
        OptimisticTransactionDB::open_cf_descriptors(&db_opts, path, columns)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

    Ok(db)
}

pub(crate) fn open_db(path: &Path) -> Result<OptimisticTransactionDB, StorageError<NodeId>> {
    let sm_column_families = StateMachineColumns::iter()
        .map(|cf| ColumnFamilyDescriptor::new(cf.to_string(), Options::default()));

    open_db_with_columns(path, sm_column_families)
}

pub(crate) fn open_logs(path: &Path) -> Result<OptimisticTransactionDB, StorageError<NodeId>> {
    let store = ColumnFamilyDescriptor::new(LOG_STORE_STORE_COLUMN, Options::default());
    let logs = ColumnFamilyDescriptor::new(LOG_STORE_LOGS_COLUMN, Options::default());

    open_db_with_columns(path, [store, logs])
}

fn get_v1_snapshot(snapshot_file_path: PathBuf) -> StorageResult<Option<StoredSnapshot>> {
    if !snapshot_file_path.exists() {
        debug!("The snapshot file does not exist");
        return Ok(None);
    }

    let file = File::open(&snapshot_file_path).map_err(|e| StorageError::IO {
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

fn apply_v1_snapshot(
    db: &OptimisticTransactionDB,
    snapshot: &state_machine_objects::V1Snapshot,
) -> StorageResult<()> {
    fn put_cf<K, T>(
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        cf: &ColumnFamily,
        key: K,
        value: &T,
    ) -> StorageResult<()>
    where
        K: AsRef<[u8]>,
        T: serde::Serialize + Debug,
    {
        let serialized =
            serde_json::to_vec(value).map_err(|e| StorageIOError::write_state_machine(&e))?;
        txn.put_cf(cf, key, serialized)
            .map_err(|e| StorageIOError::write_state_machine(&e).into())
    }

    let txn = db.transaction();

    //  Build the rocksdb forward indexes
    for (_, eg) in &snapshot.extraction_graphs {
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let eg: ExtractionGraph = eg.clone().into();
        put_cf(&txn, cf, &eg.id, &eg)?;
    }
    for (executor_id, executor_metadata) in &snapshot.executors {
        let cf = StateMachineColumns::Executors.cf(db);
        put_cf(&txn, cf, executor_id, &executor_metadata)?;
    }
    for (task_id, task) in &snapshot.tasks {
        let cf = StateMachineColumns::Tasks.cf(db);
        let task = compat::convert_v1_task(task.clone(), db)
            .map_err(|e| StorageIOError::read_snapshot(None, e))?;
        put_cf(&txn, cf, task_id, &task)?;
    }
    for (gc_task_id, gc_task) in &snapshot.gc_tasks {
        let cf = StateMachineColumns::GarbageCollectionTasks.cf(db);
        put_cf(&txn, cf, gc_task_id, &gc_task)?;
    }
    for (executor_id, task_ids) in &snapshot.task_assignments {
        let cf = StateMachineColumns::TaskAssignments.cf(db);
        put_cf(&txn, cf, executor_id, &task_ids)?;
    }
    for (state_change_id, state_change) in &snapshot.state_changes {
        let cf = StateMachineColumns::StateChanges.cf(db);
        put_cf(&txn, cf, state_change_id.to_key(), &state_change)?;
    }
    for (_, content) in &snapshot.content_table {
        let cf = StateMachineColumns::ContentTable.cf(db);
        let content: ContentMetadata = content.clone().into();
        put_cf(&txn, cf, &content.id_key(), &content)?;
    }
    for (extraction_policy_id, extraction_policy_ids) in &snapshot.extraction_policies {
        let cf = StateMachineColumns::ExtractionPolicies.cf(db);
        let policy: ExtractionPolicy = extraction_policy_ids.clone().into();
        put_cf(&txn, cf, extraction_policy_id, &policy)?;
    }
    for (extractor_name, extractor_description) in &snapshot.extractors {
        let cf = StateMachineColumns::Extractors.cf(db);
        put_cf(&txn, cf, extractor_name, &extractor_description)?;
    }
    for namespace in &snapshot.namespaces {
        let cf = StateMachineColumns::Namespaces.cf(db);
        put_cf(&txn, cf, namespace, &namespace)?;
    }
    for (index_name, index) in &snapshot.index_table {
        let cf = StateMachineColumns::IndexTable.cf(db);
        put_cf(&txn, cf, index_name, &index)?;
    }
    for (schema_id, schema) in &snapshot.structured_data_schemas {
        let cf = StateMachineColumns::StructuredDataSchemas.cf(db);
        put_cf(&txn, cf, schema_id, &schema)?;
    }
    for (node_id, addr) in &snapshot.coordinator_address {
        let cf = StateMachineColumns::CoordinatorAddress.cf(db);
        put_cf(&txn, cf, &node_id.to_string(), &addr)?;
    }
    txn.commit()
        .map_err(|e| StorageIOError::write_state_machine(&e))?;
    init_task_analytics(db)
}

// Convert the v1 store to the new store.
// Split out the logs and store column families into new logs db
// Restore sm store from snapshot if present
// Store last log id and membership in sm store
fn convert_v1_store(db_path: PathBuf, v1_db_path: PathBuf) -> Result<(), StorageError<NodeId>> {
    let opts = Options::default();
    let cf_names = rocksdb::DB::list_cf(&opts, &v1_db_path).unwrap();
    let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
        .iter()
        .map(|cf_name| ColumnFamilyDescriptor::new(cf_name, Options::default()))
        .collect();

    let v1_db: DBCommon<SingleThreaded, _> =
        OptimisticTransactionDB::open_cf_descriptors(&opts, &v1_db_path, cf_descriptors).unwrap();

    let new_db_path = db_path.join("store");
    if new_db_path.exists() {
        fs::remove_dir_all(&new_db_path).unwrap();
    }

    let new_log_path = db_path.join("log");
    if new_log_path.exists() {
        fs::remove_dir_all(&new_log_path).unwrap();
    }

    let db_name = Uuid::new_v4().to_string();
    let new_db_path = new_db_path.join(db_name);
    let new_db = open_db(&new_db_path).unwrap();
    let logs = open_logs(&new_log_path).unwrap();

    // Copy logs column to new logs db
    for val in v1_db.iterator_cf(logs_column(&v1_db), IteratorMode::Start) {
        let (key, value) = val.unwrap();
        let entry: Entry<V1TypeConfig> = JsonEncoder::decode(&value).unwrap();

        if let EntryPayload::Normal(req) = entry.payload {
            let log = convert_v1_log(req, &v1_db).map_err(|e| StorageIOError::read_logs(e))?;
            let updated_entry: typ::Entry = Entry {
                log_id: entry.log_id,
                payload: EntryPayload::Normal(log),
            };

            let updated_value = JsonEncoder::encode(&updated_entry).unwrap();
            logs.put_cf(logs_column(&logs), key, updated_value).unwrap();
            continue;
        }

        logs.put_cf(logs_column(&logs), key, value).unwrap();
    }

    for val in v1_db.iterator_cf(store_column(&v1_db), IteratorMode::Start) {
        let (key, value) = val.unwrap();
        logs.put_cf(store_column(&logs), key, value).unwrap();
    }

    // Restore new db from snapshot if present
    let v1_snapshot_path = db_path.join("sm-blob");
    let snapshot = get_v1_snapshot(v1_snapshot_path.clone()).unwrap();
    if let Some(snapshot) = snapshot {
        let indexify_state_snapshot: state_machine_objects::V1Snapshot =
            JsonEncoder::decode(&snapshot.data).unwrap();

        if let Some(data) = snapshot.meta.last_log_id {
            let applied_data = JsonEncoder::encode(&data).unwrap();
            new_db
                .put_cf(
                    StateMachineColumns::RaftState.cf(&new_db),
                    LAST_APPLIED_LOG_ID_KEY,
                    applied_data,
                )
                .unwrap();
        }

        let membership_data = JsonEncoder::encode(&snapshot.meta.last_membership).unwrap();
        new_db
            .put_cf(
                StateMachineColumns::RaftState.cf(&new_db),
                LAST_MEMBERSHIP_KEY,
                membership_data,
            )
            .unwrap();

        apply_v1_snapshot(&new_db, &indexify_state_snapshot).unwrap();

        fs::remove_file(&v1_snapshot_path).unwrap();
    }
    new_db
        .put_cf(
            StateMachineColumns::RaftState.cf(&new_db),
            STORE_VERSION,
            CURRENT_STORE_VERSION.to_be_bytes(),
        )
        .unwrap();
    fs::remove_dir_all(&v1_db_path).unwrap();
    Ok(())
}

fn convert_v1_log(
    log: V1StateMachineUpdateRequest,
    db: &OptimisticTransactionDB,
) -> Result<StateMachineUpdateRequest, anyhow::Error> {
    let payload = match log.payload {
        V1RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        } => RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        },
        V1RequestPayload::RegisterExecutor {
            addr,
            executor_id,
            extractors,
            ts_secs,
        } => RequestPayload::RegisterExecutor {
            addr,
            executor_id,
            extractors,
            ts_secs,
        },
        V1RequestPayload::RemoveExecutor { executor_id } => {
            RequestPayload::RemoveExecutor { executor_id }
        }
        V1RequestPayload::CreateNamespace { name } => RequestPayload::CreateNamespace { name },
        V1RequestPayload::CreateTasks { tasks } => {
            let tasks: Result<Vec<Task>, _> = tasks
                .into_iter()
                .map(|t| compat::convert_v1_task(t, db))
                .collect();
            RequestPayload::CreateTasks { tasks: tasks? }
        }
        V1RequestPayload::AssignTask { assignments } => RequestPayload::AssignTask { assignments },
        V1RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks } => {
            RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks }
        }
        V1RequestPayload::UpdateGarbageCollectionTask {
            gc_task,
            mark_finished,
        } => RequestPayload::UpdateGarbageCollectionTask {
            gc_task,
            mark_finished,
        },
        V1RequestPayload::CreateExtractionGraph {
            extraction_graph,
            structured_data_schema,
            indexes,
        } => {
            let extraction_graph: ExtractionGraph = extraction_graph.into();
            RequestPayload::CreateExtractionGraph {
                extraction_graph,
                structured_data_schema,
                indexes,
            }
        }
        V1RequestPayload::CreateOrUpdateContent { entries } => {
            let entries: Vec<CreateOrUpdateContentEntry> =
                entries.into_iter().map(|e| e.into()).collect();
            RequestPayload::CreateOrUpdateContent { entries }
        }
        V1RequestPayload::TombstoneContentTree { content_metadata } => {
            let content_metadata: Vec<ContentMetadata> =
                content_metadata.into_iter().map(|c| c.into()).collect();
            RequestPayload::TombstoneContentTree { content_metadata }
        }
        V1RequestPayload::SetIndex { indexes } => RequestPayload::SetIndex { indexes },
        V1RequestPayload::UpdateTask {
            task,
            executor_id,
            update_time,
        } => RequestPayload::UpdateTask {
            task: compat::convert_v1_task(task, db)?,
            executor_id,
            update_time,
        },
        V1RequestPayload::MarkStateChangesProcessed { state_changes } => {
            RequestPayload::MarkStateChangesProcessed { state_changes }
        }
    };

    Ok(StateMachineUpdateRequest {
        payload,
        new_state_changes: log.new_state_changes,
        state_changes_processed: log.state_changes_processed,
    })
}

pub(crate) async fn new_storage<P: AsRef<Path>>(db_path: P) -> (LogStore, Arc<StateMachineStore>) {
    fs::create_dir_all(&db_path).expect("Failed to create db directory");

    let db_path = PathBuf::from(db_path.as_ref());

    let v1_db_path = db_path.clone().join("db");
    if v1_db_path.exists() {
        convert_v1_store(db_path.clone(), v1_db_path).unwrap();
    }

    let log_path = db_path.join("log");
    let log_db = open_logs(log_path.as_ref()).unwrap();

    let log_store = LogStore {
        db: Arc::new(log_db),
    };

    let db_path = db_path.join("store");

    fs::create_dir_all(&db_path).expect("Failed to create db directory");

    let sm_store = StateMachineStore::new(db_path, &log_store).await.unwrap();

    (log_store, Arc::new(sm_store))
}

openraft::declare_raft_types!(
    pub V1TypeConfig:
        D = V1StateMachineUpdateRequest,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<V1TypeConfig>,
        SnapshotData = SnapshotData,
        AsyncRuntime = openraft::TokioRuntime
);

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use indexify_internal_api::ContentMetadataId;

    use crate::{setup_fmt_tracing, state::RaftConfigOverrides, test_utils::RaftTestCluster};

    /// This is a dummy test which forces building a snapshot on the cluster by
    /// passing in some overrides Manually check that the snapshot file was
    /// actually created. Still need to find a way to force reading and
    /// deserialization
    #[tokio::test]
    async fn test_install_snapshot() -> anyhow::Result<()> {
        setup_fmt_tracing();
        //  set up raft cluster
        let overrides = RaftConfigOverrides {
            snapshot_policy: Some(openraft::SnapshotPolicy::LogsSinceLast(1)),
            max_in_snapshot_log_to_keep: Some(0),
        };
        let mut cluster = RaftTestCluster::new(1, Some(overrides.clone())).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        let node = cluster.get_raft_node(0)?;

        //  add data
        let namespace = "test_namespace".to_string();
        node.create_namespace(&namespace).await?;
        let content = indexify_internal_api::ContentMetadata {
            id: ContentMetadataId::new("content_id"),
            ..Default::default()
        };
        node.create_content_batch(vec![content]).await?;

        //  add a new node
        cluster.add_node_to_cluster(Some(overrides)).await?;
        tokio::time::sleep(Duration::from_secs(3)).await;

        //  ensure that snapshot invariants are maintained on new node
        let new_node = cluster.get_raft_node(1)?;
        let content_table = new_node.state_machine.get_content_namespace_table()?;
        assert_eq!(content_table.len(), 1);
        let (key, value) = content_table.iter().next().unwrap();
        assert_eq!(*key, namespace);
        assert_eq!(value.len(), 1);

        let contents = new_node
            .list_content(|c| c.namespace == namespace, None, None)
            .await?
            .items;
        assert_eq!(contents.len(), 1);
        let c = contents
            .first()
            .expect("expected the content to be present");
        assert_eq!(c.namespace, namespace);
        Ok(())
    }
}
