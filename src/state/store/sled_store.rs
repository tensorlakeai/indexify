use std::{
    collections::{HashMap, HashSet},
    error::Error,
    fmt::{Debug, Display, Formatter},
    ops::Deref,
    string::ToString,
    sync::Arc,
};

use byteorder::{BigEndian, ByteOrder};
use indexify_internal_api as internal_api;
use openraft::{BasicNode, Entry, LogId, StoredMembership, Vote};
use serde::{Deserialize, Serialize};
use sled::{transaction::ConflictableTransactionError, IVec};
use tokio::sync::RwLock;

use super::{error::*, impl_sled_storable::SledStorable, NodeId, TypeConfig, *};
use crate::server_config::SledConfig;

pub struct SledStore {
    /// sled database
    pub(super) db: Arc<SledStoreDb>,

    /// in-memory state machine
    pub state_machine: RwLock<StateMachine>,

    pub state_change_tx: tokio::sync::watch::Sender<StateChange>,
    pub state_change_rx: tokio::sync::watch::Receiver<StateChange>,
}

impl SledStore {
    pub async fn new(config: SledConfig) -> SledStore {
        let (tx, rx) = tokio::sync::watch::channel(StateChange {
            id: "".to_string(),
            change_type: ChangeType::NewContent,
        });

        let sled_config = match config.path {
            None => {
                tracing::warn!("sled store path not set - using default tmp path");
                sled::Config::default().temporary(true)
            }
            Some(path) => sled::Config::default().path(path),
        };

        let db = Arc::new(
            sled_config
                .open()
                .unwrap_or_else(|_| panic!("failed to open sled db with {:?}", sled_config)),
        );

        let store_db = Arc::new(SledStoreDb(db.clone()));

        let state_machine = RwLock::new(
            StateMachine::from_sled_db(db.clone())
                .expect("failed to load state machine from sled db"),
        );

        SledStore {
            db: store_db,
            state_machine,
            state_change_tx: tx,
            state_change_rx: rx,
        }
    }
}

impl Deref for SledStore {
    type Target = Arc<SledStoreDb>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

#[derive(Debug)]
pub struct SledStoreDb(Arc<sled::Db>);

impl SledStoreDb {
    pub fn open_tree(&self, name: SledStoreTree) -> sled::Tree {
        self.0
            .open_tree(name.to_string())
            .expect("open tree failed")
    }

    pub fn get_last_log(&self) -> StoreResult<Option<(LogId<NodeId>, Entry<TypeConfig>)>> {
        let tree = self.open_tree(SledStoreTree::Logs);
        let last = tree.last().map_err(|e| {
            StoreErrorKind::ReadLogs.build_with_tree(
                "get last log failed: {}",
                e.into(),
                SledStoreTree::Logs,
            )
        })?;
        match last {
            Some((_, raw_entry)) => {
                let entry = Entry::<TypeConfig>::load_from_sled_value(raw_entry).map_err(|e| {
                    StoreErrorKind::ReadLogs.build_with_tree(
                        "get last log failed to parse",
                        e,
                        SledStoreTree::Logs,
                    )
                })?;

                StoreResult::Ok(Some((entry.log_id, entry)))
            }
            None => StoreResult::Ok(None),
        }
    }

    pub fn get_last_purged_log_id(&self) -> StoreResult<Option<LogId<NodeId>>> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::LastPurgedLogId.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadLogs.build_with_tree_and_key(
                "get last purged log id failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                LogId::<NodeId>::load_from_sled_value(value).map_err(|e| {
                    StoreErrorKind::ReadLogs.build_with_tree_and_key(
                        "failed to parse last log id",
                        e,
                        SledStoreTree::Store,
                        key,
                    )
                })
            })
            .transpose()
    }

    pub fn get_snapshot_index(&self) -> StoreResult<Option<u64>> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::SnapshotIndex.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadSnapshot.build_with_tree_and_key(
                "get snapshot index failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                SnapshotIndex::load_from_sled_value(value)
                    .map_err(|e| {
                        StoreErrorKind::ReadSnapshot.build_with_tree_and_key(
                            "failed to parse snapshot index",
                            e,
                            SledStoreTree::Store,
                            key,
                        )
                    })
                    .map(|SnapshotIndex(index)| index)
            })
            .transpose()
    }

    pub fn get_current_snapshot(&self) -> StoreResult<Option<StoredSnapshot>> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::CurrentSnapshot.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadSnapshot.build_with_tree_and_key(
                "get current snapshot failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                StoredSnapshot::load_from_sled_value(value).map_err(|e| {
                    StoreErrorKind::ReadSnapshot.build_with_tree_and_key(
                        "failed to parse current snapshot",
                        e,
                        SledStoreTree::Store,
                        key,
                    )
                })
            })
            .transpose()
    }

    pub async fn set_current_snapshot(&self, snapshot: StoredSnapshot) -> StoreResult<()> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::CurrentSnapshot.to_string();

        let result = snapshot.to_saveable_value().map_err(|e| {
            StoreErrorKind::WriteSnapshot.build_with_tree_and_key(
                "set current snapshot failed",
                e,
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        tree.insert(key.clone(), result).map(|_| ()).map_err(|e| {
            StoreErrorKind::WriteSnapshot.build_with_tree_and_key(
                "set current snapshot failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })
    }

    pub fn get_vote(&self) -> StoreResult<Option<Vote<NodeId>>> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::Vote.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadVote.build_with_tree_and_key(
                "get vote failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                Vote::<NodeId>::load_from_sled_value(value).map_err(|e| {
                    StoreErrorKind::ReadVote.build_with_tree_and_key(
                        "failed to parse vote",
                        e,
                        SledStoreTree::Store,
                        key,
                    )
                })
            })
            .transpose()
    }

    pub async fn insert_vote(&self, vote: Vote<NodeId>) -> StoreResult<()> {
        let tree = self.open_tree(SledStoreTree::Store);
        let key = StoreKey::Vote.to_string();

        let result = vote.to_saveable_value().map_err(|e| {
            StoreErrorKind::WriteVote.build_with_tree_and_key(
                "insert vote failed",
                e,
                SledStoreTree::Store,
                key.clone(),
            )
        })?;

        tree.insert(key.clone(), result).map(|_| ()).map_err(|e| {
            StoreErrorKind::WriteVote.build_with_tree_and_key(
                "insert vote failed",
                e.into(),
                SledStoreTree::Store,
                key.clone(),
            )
        })
    }

    pub fn get_last_membership(&self) -> StoreResult<Option<StoredMembership<NodeId, BasicNode>>> {
        let tree = self.open_tree(SledStoreTree::StateMachine);
        let key = StateMachineKey::LastMembership.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadLogs.build_with_tree_and_key(
                "get last membership failed",
                e.into(),
                SledStoreTree::StateMachine,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                StoredMembership::<NodeId, BasicNode>::load_from_sled_value(value).map_err(|e| {
                    StoreErrorKind::ReadLogs.build_with_tree_and_key(
                        "failed to parse last membership",
                        e,
                        SledStoreTree::StateMachine,
                        key,
                    )
                })
            })
            .transpose()
    }

    pub fn get_last_applied_log(&self) -> StoreResult<Option<LogId<NodeId>>> {
        let tree = self.open_tree(SledStoreTree::StateMachine);
        let key = StateMachineKey::LastAppliedLog.to_string();

        let value_option = tree.get(&key).map_err(|e| {
            StoreErrorKind::ReadLogs.build_with_tree_and_key(
                "get last applied log failed",
                e.into(),
                SledStoreTree::StateMachine,
                key.clone(),
            )
        })?;

        value_option
            .map(|value| {
                LogId::<NodeId>::load_from_sled_value(value).map_err(|e| {
                    StoreErrorKind::ReadLogs.build_with_tree_and_key(
                        "failed to parse last applied log",
                        e,
                        SledStoreTree::StateMachine,
                        key,
                    )
                })
            })
            .transpose()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StateMachineKey {
    LastMembership,
    LastAppliedLog,
}

impl Display for StateMachineKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StateMachineKey::LastMembership => write!(f, "last_membership"),
            StateMachineKey::LastAppliedLog => write!(f, "last_applied_log"),
        }
    }
}

impl StateMachine {
    pub fn try_from_sled_tree(tree: sled::Tree) -> Result<StateMachine, StoreError> {
        let mut state_machine = StateMachine::default();

        for res in tree.iter() {
            let (key, value) = res.map_err(|e| {
                StoreErrorKind::ReadStateMachine.build_with_tree(
                    "failed to read state machine entry",
                    e.into(),
                    SledStoreTree::StateMachine,
                )
            })?;
            let key = String::from_utf8(key.to_vec()).map_err(|e| {
                StoreErrorKind::ParseError.build_with_tree(
                    "failed to parse state machine key",
                    e.into(),
                    SledStoreTree::StateMachine,
                )
            })?;
            let err_kind = StoreErrorKind::ReadStateMachine;
            match key.as_str() {
                "last_applied_log" => {
                    state_machine.last_applied_log =
                        Some(LogId::<NodeId>::load_from_sled_value(value).map_err(
                            |e: Box<dyn Error>| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load last_applied_log",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            },
                        )?);
                }
                "last_membership" => {
                    state_machine.last_membership =
                        StoredMembership::<NodeId, BasicNode>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load last_membership",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "executors" => {
                    state_machine.executors =
                        HashMap::<ExecutorId, internal_api::ExecutorMetadata>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load executors",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "tasks" => {
                    state_machine.tasks =
                        HashMap::<TaskId, internal_api::Task>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load tasks",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "unassigned_tasks" => {
                    state_machine.unassigned_tasks = HashSet::<TaskId>::load_from_sled_value(value)
                        .map_err(|e| {
                            err_kind.build_with_tree_and_key(
                                "failed to load unassigned_tasks",
                                e,
                                SledStoreTree::StateMachine,
                                key.clone(),
                            )
                        })?;
                }
                "task_assignments" => {
                    state_machine.task_assignments =
                        HashMap::<ExecutorId, HashSet<TaskId>>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load task_assignments",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "extraction_events" => {
                    state_machine.extraction_events = HashMap::<
                        ExtractionEventId,
                        internal_api::ExtractionEvent,
                    >::load_from_sled_value(
                        value
                    )
                    .map_err(|e| {
                        err_kind.build_with_tree_and_key(
                            "failed to load extraction_events",
                            e,
                            SledStoreTree::StateMachine,
                            key.clone(),
                        )
                    })?;
                }
                "unprocessed_extraction_events" => {
                    state_machine.unprocessed_extraction_events =
                        HashSet::<ExtractionEventId>::load_from_sled_value(value).map_err(|e| {
                            err_kind.build_with_tree_and_key(
                                "failed to load unprocessed_extraction_events",
                                e,
                                SledStoreTree::StateMachine,
                                key.clone(),
                            )
                        })?;
                }
                "content_table" => {
                    state_machine.content_table =
                        HashMap::<ContentId, internal_api::ContentMetadata>::load_from_sled_value(
                            value,
                        )
                        .map_err(|e| {
                            err_kind.build_with_tree_and_key(
                                "failed to load content_table",
                                e,
                                SledStoreTree::StateMachine,
                                key.clone(),
                            )
                        })?;
                }
                "content_repository_table" => {
                    state_machine.content_repository_table =
                        HashMap::<RepositoryId, HashSet<ContentId>>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load content_repository_table",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "bindings_table" => {
                    state_machine.bindings_table = HashMap::<
                        RepositoryId,
                        HashSet<internal_api::ExtractorBinding>,
                    >::load_from_sled_value(
                        value
                    )
                    .map_err(|e| {
                        err_kind.build_with_tree_and_key(
                            "failed to load bindings_table",
                            e,
                            SledStoreTree::StateMachine,
                            key.clone(),
                        )
                    })?;
                }
                "extractor_executors_table" => {
                    state_machine.extractor_executors_table =
                        HashMap::<ExtractorName, HashSet<ExecutorId>>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load extractor_executors_table",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                "extractors" => {
                    state_machine.extractors = HashMap::<
                        ExtractorName,
                        internal_api::ExtractorDescription,
                    >::load_from_sled_value(value)
                    .map_err(|e| {
                        err_kind.build_with_tree_and_key(
                            "failed to load extractors",
                            e,
                            SledStoreTree::StateMachine,
                            key.clone(),
                        )
                    })?;
                }
                "repositories" => {
                    state_machine.repositories = HashSet::<String>::load_from_sled_value(value)
                        .map_err(|e| {
                            err_kind.build_with_tree_and_key(
                                "failed to load repositories",
                                e,
                                SledStoreTree::StateMachine,
                                key.clone(),
                            )
                        })?;
                }
                "repository_extractors" => {
                    state_machine.repository_extractors = HashMap::<
                        RepositoryId,
                        HashSet<internal_api::Index>,
                    >::load_from_sled_value(
                        value
                    )
                    .map_err(|e| {
                        err_kind.build_with_tree_and_key(
                            "failed to load repository_extractors",
                            e,
                            SledStoreTree::StateMachine,
                            key.clone(),
                        )
                    })?;
                }
                "index_table" => {
                    state_machine.index_table =
                        HashMap::<String, internal_api::Index>::load_from_sled_value(value)
                            .map_err(|e| {
                                err_kind.build_with_tree_and_key(
                                    "failed to load index_table",
                                    e,
                                    SledStoreTree::StateMachine,
                                    key.clone(),
                                )
                            })?;
                }
                _ => {
                    return Err(StoreError::new(
                        StoreErrorKind::ParseError,
                        format!("unknown key {}", key),
                    )
                    .with_tree(SledStoreTree::StateMachine)
                    .with_key(key))?
                }
            }
        }
        Ok(state_machine)
    }

    pub fn overwrite_sled_kv(
        &self,
        tree: &sled::Tree,
        key: &str,
        raw_value: impl SledStorable,
    ) -> Result<(), StoreError> {
        let value = raw_value.to_saveable_value().map_err(|e| {
            StoreError::new(
                StoreErrorKind::WriteStateMachine,
                format!(
                    "failed to serialize state machine from sled with key {}",
                    key
                ),
            )
            .with_tree(SledStoreTree::StateMachine)
            .with_key(key)
            .with_source(e)
        })?;

        tree.insert(key, value).map(|_| ()).map_err(|e| {
            StoreError::new(
                StoreErrorKind::WriteStateMachine,
                format!("failed to write state machine to sled with key {}", key),
            )
            .with_tree(SledStoreTree::StateMachine)
            .with_key(key)
            .with_source(e.into())
        })
    }

    pub fn try_save_to_sled_tree(&self, tree: &sled::Tree) -> Result<(), StoreError> {
        let err_fn = |e: Box<dyn Error>, key: String| {
            ConflictableTransactionError::Abort(StoreErrorKind::WriteStateMachine.build_with_tree(
                format!(
                    "failed to serialize state machine from sled with key {}",
                    key
                ),
                e,
                SledStoreTree::StateMachine,
            ))
        };
        tree.transaction(|tx| {
            // insert last applied log if it exists
            if let Some(last_applied_log) = &self.last_applied_log {
                tx.insert(
                    "last_applied_log",
                    last_applied_log
                        .to_saveable_value()
                        .map_err(|e| err_fn(e, "last_applied_log".to_string()))?,
                )?;
            }
            tx.insert(
                "executors",
                self.executors
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "executors".to_string()))?,
            )?;
            tx.insert(
                "tasks",
                self.tasks
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "tasks".to_string()))?,
            )?;
            tx.insert(
                "unassigned_tasks",
                self.unassigned_tasks
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "unassigned_tasks".to_string()))?,
            )?;
            tx.insert(
                "task_assignments",
                self.task_assignments
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "task_assignments".to_string()))?,
            )?;
            tx.insert(
                "extraction_events",
                self.extraction_events
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "extraction_events".to_string()))?,
            )?;
            tx.insert(
                "unprocessed_extraction_events",
                self.unprocessed_extraction_events
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "unprocessed_extraction_events".to_string()))?,
            )?;
            tx.insert(
                "content_table",
                self.content_table
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "content_table".to_string()))?,
            )?;
            tx.insert(
                "content_repository_table",
                self.content_repository_table
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "content_repository_table".to_string()))?,
            )?;
            tx.insert(
                "bindings_table",
                self.bindings_table
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "bindings_table".to_string()))?,
            )?;
            tx.insert(
                "extractor_executors_table",
                self.extractor_executors_table
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "extractor_executors_table".to_string()))?,
            )?;
            tx.insert(
                "extractors",
                self.extractors
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "extractors".to_string()))?,
            )?;
            tx.insert(
                "repositories",
                self.repositories
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "repositories".to_string()))?,
            )?;
            tx.insert(
                "repository_extractors",
                self.repository_extractors
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "repository_extractors".to_string()))?,
            )?;
            tx.insert(
                "index_table",
                self.index_table
                    .to_saveable_value()
                    .map_err(|e| err_fn(e, "index_table".to_string()))?,
            )?;
            Ok(())
        })
        .map_err(|e| {
            StoreError::new(
                StoreErrorKind::WriteStateMachine,
                "failed to write state machine to sled".to_string(),
            )
            .with_tree(SledStoreTree::StateMachine)
            .with_source(e.into())
        })?;
        Ok(())
    }

    pub fn from_sled_db(db: Arc<sled::Db>) -> Result<StateMachine, StoreError> {
        let tree = db.open_tree(SledStoreTree::StateMachine.to_string())?;

        let state_machine = StateMachine::try_from_sled_tree(tree)?;

        Ok(state_machine)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotIndex(pub u64);

#[cfg(test)]
mod store_tests {
    use std::collections::BTreeSet;

    use openraft::{LeaderId, Membership};

    use super::*;

    // test serialization and deserialization of SledStoreValues and log keys
    #[test]
    fn test_store_retrieve_log() {
        let mut nodes = BTreeSet::<NodeId>::new();
        nodes.insert(1);
        nodes.insert(2);
        let log = Entry::<TypeConfig> {
            log_id: LogId {
                index: 1,
                leader_id: LeaderId::new(1, 1),
            },
            payload: EntryPayload::Membership(Membership::new(vec![], nodes)),
        };
        let log_key = LogKey::from_log(&log);
        // let log_value = SledStoreValues::log(log.clone());
        let serialized_log_value = log.to_saveable_value();
        let serialized_log_value = match serialized_log_value {
            Ok(v) => v,
            Err(e) => {
                panic!("failed to serialize log: {}", e);
            }
        };
        // put into a sled tree and retrieve it
        let temp_sled_db = sled::Config::default().temporary(true).open().unwrap();
        let tree = "test-log";
        let tree = temp_sled_db.open_tree(tree).unwrap();
        let _ = tree
            .insert(log_key.to_raw_log_key(), serialized_log_value.clone())
            .unwrap();
        let retrieved_log_value = tree.get(log_key.to_raw_log_key()).unwrap();
        assert_eq!(retrieved_log_value, Some(serialized_log_value));
        let retrieved_log_value = retrieved_log_value.unwrap();

        let deserialized_log_value =
            Entry::<TypeConfig>::load_from_sled_value(retrieved_log_value).unwrap();
        assert_eq!(deserialized_log_value, log);
        assert_eq!(log_key, LogKey::from_log(&log));
        assert_eq!(log_key, LogKey::from_log_id(&log.log_id));
        assert_eq!(log_key, LogKey::from_u64(1));
        assert_eq!(log_key, LogKey::from(1));
    }

    #[test]
    fn test_store_retrieve_snapshot() {
        let snapshot = StoredSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(LogId {
                    index: 1,
                    leader_id: LeaderId::new(1, 1),
                }),
                last_membership: StoredMembership::new(
                    Some(LogId {
                        index: 1,
                        leader_id: LeaderId::new(1, 1),
                    }),
                    Membership::new(vec![], ()),
                ),
                snapshot_id: "test".to_string(),
            },
            data: vec![],
        };
        let serialized_snapshot_value = snapshot.to_saveable_value().unwrap();
        let deserialized_snapshot_value =
            StoredSnapshot::load_from_sled_value(serialized_snapshot_value).unwrap();
        assert_eq!(deserialized_snapshot_value, snapshot);
    }

    #[test]
    fn test_store_retrieve_vote() {
        let vote = Vote::<NodeId> {
            leader_id: LeaderId::new(1, 1),
            committed: true,
        };
        let serialized_vote_value = vote.to_saveable_value().unwrap();
        let deserialized_vote_value =
            Vote::<u64>::load_from_sled_value(serialized_vote_value).unwrap();
        assert_eq!(deserialized_vote_value, vote);
    }

    #[test]
    fn test_store_retrieve_state_machine() {
        let state_machine = StateMachine::default();
        let temp_sled_db = sled::Config::default().temporary(true).open().unwrap();
        let tree = "test-state-machine";
        let tree = temp_sled_db.open_tree(tree).unwrap();
        state_machine.try_save_to_sled_tree(&tree).unwrap();
        let deserialized_state_machine_value = StateMachine::try_from_sled_tree(tree).unwrap();
        // remove the db from the filesystem
        temp_sled_db
            .flush()
            .expect("failed to flush sled db to disk");
        temp_sled_db
            .clear()
            .expect("failed to clear sled db from disk");

        assert_eq!(deserialized_state_machine_value, state_machine);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SledStoreTree {
    Store,
    StateMachine,
    Logs,
}

impl Display for SledStoreTree {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SledStoreTree::Store => write!(f, "store"),
            SledStoreTree::StateMachine => write!(f, "state_machine"),
            SledStoreTree::Logs => write!(f, "logs"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreKey {
    LastPurgedLogId,
    SnapshotIndex,
    Vote,
    CurrentSnapshot,
}

impl Display for StoreKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreKey::LastPurgedLogId => write!(f, "last_purged_log_id"),
            StoreKey::SnapshotIndex => write!(f, "snapshot_index"),
            StoreKey::Vote => write!(f, "vote"),
            StoreKey::CurrentSnapshot => write!(f, "current_snapshot"),
        }
    }
}

// converts an id to a byte vector for storing in the database.
/// Note that we're using big endian encoding to ensure correct sorting of keys
/// with notes form: <https://github.com/spacejam/sled#a-note-on-lexicographic-ordering-and-endianness>
fn log_idx_to_bin(id: u64) -> [u8; 8] {
    let mut buf: [u8; 8] = [0; 8];
    BigEndian::write_u64(&mut buf, id);
    buf
}

fn bin_to_log_idx(buf: &[u8]) -> u64 {
    BigEndian::read_u64(&buf[0..8])
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogKey([u8; 8]);

impl LogKey {
    pub fn from_u64(id: u64) -> Self {
        Self(log_idx_to_bin(id))
    }

    pub fn from_log(log: &Entry<TypeConfig>) -> Self {
        Self::from_u64(log.log_id.index)
    }

    pub fn from_log_id(log_id: &LogId<NodeId>) -> Self {
        Self::from_u64(log_id.index)
    }

    pub fn to_raw_log_key(&self) -> [u8; 8] {
        self.0
    }
}

impl From<LogKey> for [u8; 8] {
    fn from(key: LogKey) -> Self {
        key.0
    }
}

impl From<LogKey> for u64 {
    fn from(key: LogKey) -> Self {
        bin_to_log_idx(&key.0)
    }
}

impl From<u64> for LogKey {
    fn from(id: u64) -> Self {
        Self::from_u64(id)
    }
}

impl From<IVec> for LogKey {
    fn from(ivec: IVec) -> Self {
        Self::from_u64(bin_to_log_idx(&ivec))
    }
}

impl AsRef<[u8]> for LogKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
