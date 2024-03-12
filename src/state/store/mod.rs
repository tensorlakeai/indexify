use std::{
    fmt::Debug,
    fs::{self, File},
    io::{BufReader, Cursor, Read, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use flate2::bufread::ZlibDecoder;
use indexify_internal_api::StateChange;
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
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, Options, DB};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::debug;

type Node = BasicNode;

use self::state_machine_objects::IndexifyState;
use super::{typ, NodeId, SnapshotData, TypeConfig};

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
pub struct StateMachineStore {
    pub data: StateMachineData,

    /// snapshot index is not persisted in this example.
    ///
    /// It is only used as a suffix of snapshot id, and should be globally
    /// unique. In practice, using a timestamp in micro-second would be good
    /// enough.
    snapshot_idx: u64,

    /// State machine stores snapshot in db.
    _db: Arc<DB>,

    pub state_change_rx: tokio::sync::watch::Receiver<StateChange>,

    snapshot_file_path: PathBuf,
}

#[derive(Clone)]
pub struct StateMachineData {
    pub last_applied_log_id: Option<LogId<NodeId>>,

    pub last_membership: StoredMembership<NodeId, Node>,

    /// State built from applying the raft log
    pub indexify_state: Arc<RwLock<IndexifyState>>,

    state_change_tx: Arc<tokio::sync::watch::Sender<StateChange>>,
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

impl StateMachineStore {
    async fn new(
        db: Arc<DB>,
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
            _db: db,
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
        let mut sm = self.data.indexify_state.write().await;
        let mut change_events: Vec<StateChange> = Vec::new();

        for ent in entries {
            self.data.last_applied_log_id = Some(ent.log_id);
            let resp_value = None;
            match ent.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(req) => {
                    change_events.extend(req.new_state_changes.clone());
                    sm.apply(req.clone());
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
        debug!("Called install_snapshot");
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
    db: Arc<DB>,
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
            .delete_range_cf(self.logs(), &from, &to)
            .map_err(|e| StorageIOError::write_logs(&e).into())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [0, {:?}]", log_id);

        self.set_last_purged_(log_id)?;
        let from = id_to_bin(0);
        let to = id_to_bin(log_id.index + 1);
        self.db
            .delete_range_cf(self.logs(), &from, &to)
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

    let db = DB::open_cf_descriptors(&db_opts, db_path, vec![store, logs]).unwrap();
    let db = Arc::new(db);

    let log_store = LogStore { db: db.clone() };

    let snapshot_path = PathBuf::from(snapshot_path.as_ref());

    let sm_store = StateMachineStore::new(db, snapshot_path).await.unwrap();

    (log_store, sm_store)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use openraft::SnapshotPolicy;

    use crate::{state::RaftConfigOverrides, test_utils::RaftTestCluster};

    /// This is a dummy test which forces building a snapshot on the cluster by
    /// passing in some overrides Manually check that the snapshot file was
    /// actually created. Still need to find a way to force reading and
    /// deserialization
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_install_snapshot() -> anyhow::Result<()> {
        let raft_overrides = RaftConfigOverrides {
            snapshot_policy: Some(SnapshotPolicy::LogsSinceLast(1)),
        };
        let cluster = RaftTestCluster::new(3, Some(raft_overrides)).await?;
        cluster.initialize(Duration::from_secs(2)).await?;
        Ok(())
    }
}
