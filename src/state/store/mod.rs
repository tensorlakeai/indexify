use std::{
    collections::HashSet,
    fmt::Debug,
    fs::{self, File},
    io::{BufReader, Cursor, Read, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use flate2::bufread::ZlibDecoder;
use indexify_internal_api::{ContentMetadata, ExecutorMetadata, StateChange, StructuredDataSchema};
use itertools::Itertools;
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
use rocksdb::{
    ColumnFamily,
    ColumnFamilyDescriptor,
    Direction,
    OptimisticTransactionDB,
    Options,
    Transaction,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use strum::{AsRefStr, IntoEnumIterator};
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::debug;

type Node = BasicNode;

use self::state_machine_objects::IndexifyState;
use super::{typ, NodeId, SnapshotData, TypeConfig};
use crate::utils::OptionInspectNone;

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
pub mod state_machine_objects;
mod store_utils;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Response {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    /// State built from applying the raft log
    pub indexify_state: Arc<RwLock<IndexifyState>>,

    state_change_tx: Arc<tokio::sync::watch::Sender<StateChange>>,
}

#[derive(Clone)]
pub struct StateMachineStore {
    pub data: StateMachineData,

    /// snapshot index is not persisted in this example.
    ///
    /// It is only used as a suffix of snapshot id, and should be globally
    /// unique. In practice, using a timestamp in micro-second would be good
    /// enough.
    snapshot_idx: u64,

    db: Arc<OptimisticTransactionDB>,

    pub state_change_rx: tokio::sync::watch::Receiver<StateChange>,

    snapshot_file_path: PathBuf,
}

#[derive(Error, Debug)]
pub enum StateMachineError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("RocksDB transaction error: {0}")]
    TransactionError(String),
}

#[derive(AsRefStr, strum::Display, strum::EnumIter)]
pub enum StateMachineColumns {
    Executors,             //  ExecutorId -> Executor Metadata
    Tasks,                 //  TaskId -> Task
    TaskAssignments,       //   ExecutorId -> HashSet<TaskId>
    StateChanges,          //  StateChangeId -> StateChange
    ContentTable,          //  ContentId -> ContentMetadata
    ExtractionPolicies,    //  ExtractionPolicyId -> ExtractionPolicy
    Extractors,            //  ExtractorName -> ExtractorDescription
    Namespaces,            //  Namespaces
    IndexTable,            //  String -> Index
    StructuredDataSchemas, //  SchemaId -> StructuredDataSchema
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

impl StateMachineStore {
    async fn new(
        db: Arc<OptimisticTransactionDB>,
        snapshot_file_path: PathBuf,
    ) -> Result<StateMachineStore, StorageError<NodeId>> {
        let (tx, rx) = tokio::sync::watch::channel(StateChange::default());
        let mut sm = Self {
            data: StateMachineData {
                last_applied_log_id: None,
                last_membership: Default::default(),
                state_change_tx: Arc::new(tx),
                indexify_state: Arc::new(RwLock::new(IndexifyState::default())),
            },
            snapshot_idx: 0,
            db,
            state_change_rx: rx,
            snapshot_file_path,
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
        &mut self,
        snapshot: StoredSnapshot,
    ) -> Result<(), StorageError<NodeId>> {
        let indexify_state: IndexifyState = serde_json::from_slice(&snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(snapshot.meta.signature()), &e))?;

        self.data.last_applied_log_id = snapshot.meta.last_log_id;
        self.data.last_membership = snapshot.meta.last_membership.clone();
        let mut x = self.data.indexify_state.write().await;
        *x = indexify_state;

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
            serde_json::from_slice(&decompressed_data).map_err(|e| StorageError::IO {
                source: StorageIOError::read(&e),
            })?;
        Ok(Some(snapshot))
    }

    /// This method is called when a new snapshot is received via
    /// InstallSnapshot RPC and is used to write the snapshot to disk
    fn set_current_snapshot_(&self, snap: StoredSnapshot) -> StorageResult<()> {
        debug!("Called set_current_snapshot_");

        //  Serialize the data into JSON bytes
        let serialized_data = serde_json::to_vec(&snap).map_err(|e| StorageError::IO {
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

    /// This method fetches a key from a specific column family within a
    /// transaction
    pub async fn get_from_cf<T, K>(
        &self,
        column: StateMachineColumns,
        key: K,
    ) -> Result<T, anyhow::Error>
    where
        T: DeserializeOwned,
        K: AsRef<[u8]>,
    {
        let result_bytes = self
            .db
            .get_cf(column.cf(&self.db), key)?
            .ok_or(anyhow::anyhow!(
                "Failed to get value from column family {}",
                column.to_string()
            ))?;
        let result = serde_json::from_slice::<T>(&result_bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

        Ok(result)
    }

    pub async fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
    ) -> Result<Vec<indexify_internal_api::Task>> {
        let sm = self.data.indexify_state.read().await;
        sm.get_tasks_for_executor(executor_id, limit, &self.db)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get tasks for executor: {}", e))
    }

    pub async fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
    ) -> Result<Vec<ExecutorMetadata>> {
        let sm = self.data.indexify_state.read().await;
        sm.get_executors_from_ids(executor_ids, &self.db)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub async fn get_content_from_ids(
        &self,
        content_ids: HashSet<String>,
    ) -> Result<Vec<ContentMetadata>> {
        let sm = self.data.indexify_state.read().await;
        sm.get_content_from_ids(content_ids, &self.db)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn get_schemas(&self, ids: HashSet<String>) -> Result<Vec<StructuredDataSchema>> {
        let txn = self.db.transaction();
        let keys = ids
            .iter()
            .map(|id| (StateMachineColumns::StructuredDataSchemas.cf(&self.db), id))
            .collect_vec();
        let schema_bytes = txn.multi_get_cf(keys);
        let mut schemas = vec![];
        for schema in schema_bytes {
            let schema = schema
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
                .ok_or(StateMachineError::DatabaseError("Schema not found".into()))?;
            let schema = serde_json::from_slice(&schema)?;
            schemas.push(schema);
        }
        Ok(schemas)
    }

    pub async fn with_transaction<F, Fut>(&self, operation: F) -> Result<(), anyhow::Error>
    where
        F: FnOnce(&Transaction<OptimisticTransactionDB>) -> Fut,
        Fut: std::future::Future<Output = Result<(), anyhow::Error>>,
    {
        let txn = self.db.transaction();
        operation(&txn).await?;
        txn.commit()?;
        Ok(())
    }

    /// Test utility method to get all key-value pairs from a column family
    pub fn get_all_rows_from_cf<V>(
        &self,
        column: StateMachineColumns,
    ) -> Result<Vec<(String, V)>, anyhow::Error>
    where
        V: DeserializeOwned,
    {
        let cf_handle = self.db.cf_handle(column.as_ref()).ok_or(anyhow::anyhow!(
            "Failed to get column family {}",
            column.to_string()
        ))?;
        let iter = self.db.iterator_cf(cf_handle, rocksdb::IteratorMode::Start);

        iter.map(|item| {
            item.map_err(|e| anyhow::anyhow!(e))
                .and_then(|(key, value)| {
                    let key = String::from_utf8(key.to_vec())
                        .map_err(|e| anyhow::anyhow!("UTF-8 conversion error for key: {}", e))?;
                    let value = serde_json::from_slice(&value)
                        .map_err(|e| anyhow::anyhow!("Deserialization error for value: {}", e))?;
                    Ok((key, value))
                })
        })
        .collect::<Result<Vec<(String, V)>, _>>()
    }

    pub fn deserialize_all_cf_data<K, V>(
        &self,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<(K, V)>, anyhow::Error>
    where
        K: DeserializeOwned,
        V: DeserializeOwned,
    {
        pairs
            .into_iter()
            .map(|(key_bytes, value_bytes)| {
                let key = serde_json::from_slice(&key_bytes)
                    .map_err(|e| anyhow::anyhow!("Deserialization error for key: {}", e))?;
                let value = serde_json::from_slice(&value_bytes)
                    .map_err(|e| anyhow::anyhow!("Deserialization error for value: {}", e))?;
                Ok((key, value))
            })
            .collect()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        debug!("Called build_snapshot");
        let last_applied_log = self.data.last_applied_log_id;
        let last_membership = self.data.last_membership.clone();

        let indexify_state_json = {
            let indexify_state = self.data.indexify_state.read().await;
            serde_json::to_vec(&*indexify_state)
                .map_err(|e| StorageIOError::read_state_machine(&e))?
        };

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, self.snapshot_idx)
        } else {
            format!("--{}", self.snapshot_idx)
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

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        Ok((
            self.data.last_applied_log_id,
            self.data.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = typ::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);
        let mut change_events: Vec<StateChange> = Vec::new();
        let mut sm = self.data.indexify_state.write().await;

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);
            let resp_value = None;
            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(req) => {
                    change_events.extend(req.new_state_changes.clone());

                    if let Err(e) = sm.apply_state_machine_updates(req.clone(), &self.db) {
                        //  TODO: Should we just log the error here? This seems incorrect as it
                        // includes any errors that are thrown from a RocksDB transaction
                        panic!("error applying state machine update: {}", e);
                    };
                }
                EntryPayload::Membership(mem) => {
                    self.data.last_membership = StoredMembership::new(Some(ent.log_id), mem);
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
        self.snapshot_idx += 1;
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
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_last_purged_(&self, log_id: LogId<u64>) -> StorageResult<()> {
        self.db
            .put_cf(
                self.store(),
                b"last_purged_log_id",
                serde_json::to_vec(&log_id).unwrap().as_slice(),
            )
            .map_err(|e| StorageIOError::write(&e))?;

        self.flush(ErrorSubject::Store, ErrorVerb::Write)?;
        Ok(())
    }

    fn set_committed_(
        &self,
        committed: &Option<LogId<NodeId>>,
    ) -> Result<(), StorageIOError<NodeId>> {
        let json = serde_json::to_vec(committed).unwrap();

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
            .and_then(|v| serde_json::from_slice(&v).ok()))
    }

    fn set_vote_(&self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.db
            .put_cf(self.store(), b"vote", serde_json::to_vec(vote).unwrap())
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
            .and_then(|v| serde_json::from_slice(&v).ok()))
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
                    serde_json::from_slice(&val).map_err(|e| StorageError::IO {
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
                Some(
                    serde_json::from_slice::<Entry<TypeConfig>>(&ent)
                        .ok()?
                        .log_id,
                )
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
                    serde_json::to_vec(&entry).map_err(|e| StorageIOError::write_logs(&e))?,
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
) -> (LogStore, StateMachineStore) {
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
    // let db = DB::open_cf_descriptors(&db_opts, db_path,
    // all_column_families).unwrap();
    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };

    let snapshot_path = PathBuf::from(snapshot_path.as_ref());

    let sm_store = StateMachineStore::new(db, snapshot_path).await.unwrap();

    (log_store, sm_store)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::{raft::InstallSnapshotRequest, testing::log_id, SnapshotMeta, Vote};

    use crate::{
        state::{self, store::state_machine_objects::IndexifyState},
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
        let indexify_state = IndexifyState::default();
        let serialized_state =
            serde_json::to_vec(&indexify_state).expect("Failed to serialize the data");
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
        let node = cluster.get_node(2)?;
        node.forwardable_raft
            .raft
            .install_snapshot(install_snapshot_req)
            .await?;
        Ok(())
    }
}
