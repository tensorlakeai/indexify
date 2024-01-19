use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    error::Error,
    fmt::Debug,
    string::ToString,
};

use openraft::{
    BasicNode,
    Entry,
    LeaderId,
    LogId,
    Membership,
    SnapshotMeta,
    StoredMembership,
    Vote,
};
use serde::{Deserialize, Serialize};
use sled::IVec;

use super::{NodeId, TypeConfig, *};
use crate::internal_api::{
    ContentMetadata,
    ExecutorMetadata,
    ExtractionEvent,
    ExtractionEventPayload,
    ExtractorBinding,
    ExtractorDescription,
    Index,
    OutputSchema,
    Task,
};

pub trait SledStoreable: Serialize + for<'de> Deserialize<'de> + SledStoreableTestFactory {
    fn to_saveable_value(&self) -> Result<IVec, Box<dyn Error>> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        self.serialize(&mut s)?;
        Ok(s.take_buffer().into())
    }
    fn load_from_sled_value(raw_value: IVec) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        // convert Vec<u8> to &[u8]
        let raw_value = raw_value.to_vec();
        let raw_value_slice = raw_value.as_slice();

        let r = flexbuffers::Reader::get_root(raw_value_slice).map_err(|e| {
            sled::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Couldn't read a FlexBuffer bytestream from the cache: {}",
                    e
                ),
            ))
        })?;

        let value = Self::deserialize(r).map_err(|e| {
            sled::Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Deserializer would not deserialize using FlexBuffer: {}", e),
            ))
        })?;
        Ok(value)
    }
}

impl SledStoreable for Vote<NodeId> {}

impl SledStoreable for StoredSnapshot {
    // use bincode because StoredSnapshot.meta contains a BTreeMap
    fn to_saveable_value(&self) -> Result<IVec, Box<dyn Error>> {
        let serialized_data = bincode::serialize(self)?;
        Ok(serialized_data.into())
    }

    fn load_from_sled_value(raw_value: IVec) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        let deserialized_data = bincode::deserialize(&raw_value)?;
        Ok(deserialized_data)
    }
}

/// Flexbuffer doesn't support serializing BTreeMaps, so we need to convert them
/// to HashMaps using bincode instead.
impl SledStoreable for Entry<TypeConfig> {
    fn to_saveable_value(&self) -> Result<IVec, Box<dyn Error>> {
        let serialized_data = bincode::serialize(self)?;
        Ok(serialized_data.into())
    }

    fn load_from_sled_value(raw_value: IVec) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        let deserialized_data = bincode::deserialize(&raw_value)?;
        Ok(deserialized_data)
    }
}

// all the state machine values can be stored in Sled
impl SledStoreable for LogId<NodeId> {}
impl SledStoreable for StoredMembership<NodeId, BasicNode> {
    fn to_saveable_value(&self) -> Result<IVec, Box<dyn Error>> {
        let serialized_data = bincode::serialize(self)?;
        Ok(serialized_data.into())
    }

    fn load_from_sled_value(raw_value: IVec) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        let deserialized_data = bincode::deserialize(&raw_value)?;
        Ok(deserialized_data)
    }
}
impl SledStoreable for SnapshotIndex {}

impl SledStoreable for HashMap<ExecutorId, u64> {}
impl SledStoreable for HashMap<ExecutorId, ExecutorMetadata> {}
impl SledStoreable for HashMap<TaskId, Task> {}
impl SledStoreable for HashMap<ExtractionEventId, ExtractionEvent> {}
impl SledStoreable for HashMap<ContentId, ContentMetadata> {}
impl SledStoreable for HashMap<String, HashSet<String>> {}
impl SledStoreable for HashMap<RepositoryId, HashSet<ExtractorBinding>> {}
impl SledStoreable for HashMap<ExtractorName, Vec<ExecutorId>> {}
impl SledStoreable for HashMap<ExtractorName, ExtractorDescription> {}
impl SledStoreable for HashSet<String> {}
impl SledStoreable for HashMap<RepositoryId, HashSet<Index>> {}
impl SledStoreable for HashMap<String, Index> {}
impl SledStoreable for StateMachine {}
impl SledStoreable for SnapshotMeta<u64, BasicNode> {
    fn to_saveable_value(&self) -> Result<IVec, Box<dyn Error>> {
        let serialized_data = bincode::serialize(self)?;
        Ok(serialized_data.into())
    }

    fn load_from_sled_value(raw_value: IVec) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized,
    {
        let deserialized_data = bincode::deserialize(&raw_value)?;
        Ok(deserialized_data)
    }
}

// factories
impl SledStoreableTestFactory for StateMachine {
	fn spawn_instance_for_store_test() -> Self {
		StateMachine {
			last_applied_log: Some(LogId::spawn_instance_for_store_test()),
			last_membership: StoredMembership::spawn_instance_for_store_test(),
			executors: HashMap::<ExecutorId, ExecutorMetadata>::spawn_instance_for_store_test(),
			tasks: HashMap::<TaskId, Task>::spawn_instance_for_store_test(),
			unassigned_tasks: HashSet::<TaskId>::spawn_instance_for_store_test(),
			task_assignments: HashMap::<ExecutorId, HashSet<TaskId>>::spawn_instance_for_store_test(),
			extraction_events: HashMap::<ExtractionEventId, ExtractionEvent>::spawn_instance_for_store_test(),
			unprocessed_extraction_events: HashSet::<ExtractionEventId>::spawn_instance_for_store_test(),
			content_table: HashMap::<ContentId, ContentMetadata>::spawn_instance_for_store_test(),
			content_repository_table: HashMap::<RepositoryId, HashSet<ContentId>>::spawn_instance_for_store_test(),
			bindings_table: HashMap::<RepositoryId, HashSet<ExtractorBinding>>::spawn_instance_for_store_test(),
			extractor_executors_table: HashMap::<ExtractorName, HashSet<ExecutorId>>::spawn_instance_for_store_test(),
			extractors: HashMap::<ExtractorName, ExtractorDescription>::spawn_instance_for_store_test(),
			repositories: HashSet::<String>::spawn_instance_for_store_test(),
			repository_extractors: HashMap::<RepositoryId, HashSet<Index>>::spawn_instance_for_store_test(),
			index_table: HashMap::<String, Index>::spawn_instance_for_store_test(),
		}
	}
}

impl SledStoreableTestFactory for LogId<NodeId> {
    fn spawn_instance_for_store_test() -> Self {
        LogId {
            leader_id: LeaderId::new(0, 0),
            index: 0,
        }
    }
}

impl SledStoreableTestFactory for StoredMembership<NodeId, BasicNode> {
    fn spawn_instance_for_store_test() -> Self {
        StoredMembership::new(
            Some(LogId::spawn_instance_for_store_test()),
            Membership::spawn_instance_for_store_test(),
        )
    }
}

impl SledStoreableTestFactory for Membership<NodeId, BasicNode> {
    fn spawn_instance_for_store_test() -> Self {
        Membership::new(
            vec![{
                let mut config_set = BTreeSet::new();
                config_set.insert(0);
                config_set
            }],
            {
                let mut node_set = BTreeMap::new();
                node_set.insert(0, BasicNode::new("localhost:8080".to_string()));
            },
        )
    }
}

pub trait SledStoreableTestFactory {
    fn spawn_instance_for_store_test() -> Self;
}

impl SledStoreableTestFactory for Vote<NodeId> {
    fn spawn_instance_for_store_test() -> Self {
        Vote {
            leader_id: LeaderId::new(0, 0),
            committed: true,
        }
    }
}

impl SledStoreableTestFactory for StoredSnapshot {
    fn spawn_instance_for_store_test() -> Self {
        StoredSnapshot {
            meta: SnapshotMeta::spawn_instance_for_store_test(),
            data: vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06],
        }
    }
}

impl SledStoreableTestFactory for SnapshotMeta<u64, BasicNode> {
    fn spawn_instance_for_store_test() -> Self {
        SnapshotMeta {
            last_log_id: Some(LogId::spawn_instance_for_store_test()),
            last_membership: StoredMembership::spawn_instance_for_store_test(),
            snapshot_id: "test".to_string(),
        }
    }
}

impl SledStoreableTestFactory for Entry<TypeConfig> {
    fn spawn_instance_for_store_test() -> Self {
        Entry {
            log_id: LogId::spawn_instance_for_store_test(),
            payload: openraft::EntryPayload::Membership(Membership::spawn_instance_for_store_test()),
        }
    }
}

impl SledStoreableTestFactory for SnapshotIndex {
    fn spawn_instance_for_store_test() -> Self {
        SnapshotIndex(0)
    }
}

impl SledStoreableTestFactory for HashMap<ExecutorId, u64> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), 0);
        hm
    }
}

impl SledStoreableTestFactory for HashMap<ExecutorId, ExecutorMetadata> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert(
            "test".to_string(),
            ExecutorMetadata::spawn_instance_for_store_test(),
        );
        hm
    }
}

impl SledStoreableTestFactory for HashMap<TaskId, Task> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), Task::spawn_instance_for_store_test());
        hm
    }
}

impl SledStoreableTestFactory for HashSet<String> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hs = HashSet::new();
        hs.insert("test".to_string());
        hs
    }
}

impl SledStoreableTestFactory for HashMap<ExtractionEventId, ExtractionEvent> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert(
            "test".to_string(),
            ExtractionEvent::spawn_instance_for_store_test(),
        );
        hm
    }
}

impl SledStoreableTestFactory for HashMap<ContentId, ContentMetadata> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert(
            "test".to_string(),
            ContentMetadata::spawn_instance_for_store_test(),
        );
        hm
    }
}

fn test_json_value() -> serde_json::Value {
    serde_json::from_str(
        r#"
        {
            "name": "test",
            "description": "test",
            "input_params": [],
            "outputs": []
        }
        "#,
    )
    .unwrap()
}

impl SledStoreableTestFactory for ExtractorDescription {
    fn spawn_instance_for_store_test() -> Self {
        ExtractorDescription {
            name: "test".to_string(),
            description: "test".to_string(),
            input_params: test_json_value(),
            outputs: {
                let mut outputs = HashMap::new();
                outputs.insert(
                    "test".to_string(),
                    OutputSchema::Attributes(test_json_value()),
                );
                outputs
            },
        }
    }
}

impl SledStoreableTestFactory for ExecutorMetadata {
    fn spawn_instance_for_store_test() -> Self {
        ExecutorMetadata {
            id: "test".to_string(),
            last_seen: 0,
            addr: "localhost:8080".to_string(),
            extractor: ExtractorDescription::spawn_instance_for_store_test(),
        }
    }
}

impl SledStoreableTestFactory for Task {
    fn spawn_instance_for_store_test() -> Self {
        Task {
            id: "test".to_string(),
            extractor: "test".to_string(),
            extractor_binding: "test".to_string(),
            output_index_table_mapping: HashMap::new(),
            repository: "test".to_string(),
            content_metadata: ContentMetadata::spawn_instance_for_store_test(),
            input_params: test_json_value(),
            outcome: crate::internal_api::TaskOutcome::Success,
        }
    }
}

impl SledStoreableTestFactory for ContentMetadata {
    fn spawn_instance_for_store_test() -> Self {
        ContentMetadata {
            id: "test_id".to_string(),
            parent_id: "test_parent_id".to_string(),
            repository: "test_repository".to_string(),
            name: "test_name".to_string(),
            content_type: "test_content_type".to_string(),
            labels: {
                let mut labels = HashMap::new();
                labels.insert("key1".to_string(), "value1".to_string());
                labels.insert("key2".to_string(), "value2".to_string());
                labels
            },
            storage_url: "http://example.com/test_url".to_string(),
            created_at: 1234567890, // example timestamp
            source: "test_source".to_string(),
        }
    }
}

impl SledStoreableTestFactory for ExtractionEvent {
    fn spawn_instance_for_store_test() -> Self {
        ExtractionEvent {
            id: "test_id".to_string(),
            repository: "test_repository".to_string(),
            payload: ExtractionEventPayload::CreateContent {
                content: ContentMetadata::spawn_instance_for_store_test(),
            },
            created_at: 1234567890,
            processed_at: Some(1234567890),
        }
    }
}

impl SledStoreableTestFactory for ExtractorBinding {
    fn spawn_instance_for_store_test() -> Self {
        ExtractorBinding {
            id: "test_id".to_string(),
            name: "test_name".to_string(),
            repository: "test_repository".to_string(),
            extractor: "test_extractor".to_string(),
            filters: {
                let mut filters = HashMap::new();
                filters.insert("key1".to_string(), "value1".to_string());
                filters.insert("key2".to_string(), "value2".to_string());
                filters
            },
            input_params: test_json_value(),
            output_index_name_mapping: HashMap::new(),
            index_name_table_mapping: HashMap::new(),
            content_source: "test_content_source".to_string(),
        }
    }
}

impl SledStoreableTestFactory for Index {
    fn spawn_instance_for_store_test() -> Self {
        Index {
            repository: "test_repository".to_string(),
            name: "test_name".to_string(),
            table_name: "test_table_name".to_string(),
            schema: "test_schema".to_string(),
            extractor_binding: "test_extractor_binding".to_string(),
            extractor: "test_extractor".to_string(),
        }
    }
}

impl SledStoreableTestFactory for HashMap<String, Index> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), Index::spawn_instance_for_store_test());
        hm
    }
}

impl SledStoreableTestFactory for HashMap<ExtractorName, ExtractorDescription> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert(
            "test".to_string(),
            ExtractorDescription::spawn_instance_for_store_test(),
        );
        hm
    }
}

impl SledStoreableTestFactory for HashMap<ExtractorName, Vec<ExecutorId>> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), vec!["test".to_string()]);
        hm
    }
}

impl SledStoreableTestFactory for HashMap<RepositoryId, HashSet<ExtractorBinding>> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), {
            let mut hs = HashSet::new();
            hs.insert(ExtractorBinding::spawn_instance_for_store_test());
            hs
        });
        hm
    }
}

impl SledStoreableTestFactory for HashMap<RepositoryId, HashSet<Index>> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), {
            let mut hs = HashSet::new();
            hs.insert(Index::spawn_instance_for_store_test());
            hs
        });
        hm
    }
}

impl SledStoreableTestFactory for HashMap<String, HashSet<String>> {
    fn spawn_instance_for_store_test() -> Self {
        let mut hm = HashMap::new();
        hm.insert("test".to_string(), {
            let mut hs = HashSet::new();
            hs.insert("test".to_string());
            hs
        });
        hm
    }
}

trait SledTestObject: SledStoreable + SledStoreableTestFactory + Debug + PartialEq {}

#[allow(unused_macros)]
macro_rules! test_sled_storeable {
    ($type:ty) => {
        paste::item! {
            #[test]
            fn [<test_sled_storeable_ $type:snake>]() {
                let instance: $type = <$type>::spawn_instance_for_store_test();
                let serialized = instance.to_saveable_value().unwrap();

                // Save to sled
                let tree = sled::Config::new().temporary(true).open().unwrap();
                tree.insert("test", &serialized).unwrap();

                // Load from sled
                let retrieved = tree.get("test").unwrap().unwrap();
                assert_eq!(serialized, retrieved);

                // Deserialize
                let deserialized = <$type>::load_from_sled_value(serialized.into()).unwrap();
                assert_eq!(instance, deserialized);
            }
        }
    };
}

#[cfg(test)]
mod sled_tests {
    use super::*;

    type TestLogId = LogId<NodeId>;
    type TestStoredMembership = StoredMembership<NodeId, BasicNode>;
    type TestExecutorHealthChecks = HashMap<ExecutorId, u64>;
    type TestExecutors = HashMap<ExecutorId, ExecutorMetadata>;
    type TestTasks = HashMap<TaskId, Task>;
    type TestUnassignedTasks = HashSet<TaskId>;
    type TestTaskAssignments = HashMap<ExecutorId, HashSet<TaskId>>;
    type TestExtractionEvents = HashMap<ExtractionEventId, ExtractionEvent>;
    type TestUnprocessedExtractionEvents = HashSet<ExtractionEventId>;
    type TestContentTable = HashMap<ContentId, ContentMetadata>;
    type TestContentRepositoryTable = HashMap<RepositoryId, HashSet<ContentId>>;
    type TestBindingsTable = HashMap<RepositoryId, HashSet<ExtractorBinding>>;
    type TestExtractorExecutorsTable = HashMap<ExtractorName, Vec<ExecutorId>>;
    type TestExtractors = HashMap<ExtractorName, ExtractorDescription>;
    type TestRepositories = HashSet<String>;
    type TestRepositoryExtractors = HashMap<RepositoryId, HashSet<Index>>;
    type TestIndexTable = HashMap<String, Index>;
    type TestStateMachine = StateMachine;
    type TestVoteNodeId = Vote<NodeId>;
    type TestStoredSnapshot = StoredSnapshot;
    type TestEntryTypeConfig = Entry<TypeConfig>;
    type TestSnapshotIndex = SnapshotIndex;
    type TestSnapshotMeta = SnapshotMeta<u64, BasicNode>;

    test_sled_storeable!(TestStateMachine);
    test_sled_storeable!(TestLogId);
    test_sled_storeable!(TestStoredMembership);
    test_sled_storeable!(TestExecutorHealthChecks);
    test_sled_storeable!(TestExecutors);
    test_sled_storeable!(TestTasks);
    test_sled_storeable!(TestUnassignedTasks);
    test_sled_storeable!(TestTaskAssignments);
    test_sled_storeable!(TestExtractionEvents);
    test_sled_storeable!(TestUnprocessedExtractionEvents);
    test_sled_storeable!(TestContentTable);
    test_sled_storeable!(TestContentRepositoryTable);
    test_sled_storeable!(TestBindingsTable);
    test_sled_storeable!(TestExtractorExecutorsTable);
    test_sled_storeable!(TestExtractors);
    test_sled_storeable!(TestRepositories);
    test_sled_storeable!(TestRepositoryExtractors);
    test_sled_storeable!(TestIndexTable);
    test_sled_storeable!(TestVoteNodeId);
    test_sled_storeable!(TestStoredSnapshot);
    test_sled_storeable!(TestSnapshotMeta);
    test_sled_storeable!(TestEntryTypeConfig);
    test_sled_storeable!(TestSnapshotIndex);
}
