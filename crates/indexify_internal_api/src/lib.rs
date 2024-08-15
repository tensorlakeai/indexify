pub mod utils;
pub mod v1;
pub mod v2;
pub mod v3;
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
    fmt::{self, Display},
    hash::{Hash, Hasher},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use derive_builder::Builder;
use filter::LabelsFilter;
use indexify_proto::indexify_coordinator::{self};
use jsonschema::JSONSchema;
use serde::{de::Deserializer, Deserialize, Serialize, Serializer};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};
use utoipa::{schema, ToSchema};

pub type ExtractionGraphId = String;
pub type ExtractionGraphName = String;

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(build_fn(skip))]
pub struct ExtractionGraph {
    pub id: ExtractionGraphId,
    pub name: ExtractionGraphName,
    pub namespace: String,
    #[serde(default)]
    pub description: Option<String>,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl TryFrom<ExtractionGraph> for indexify_coordinator::ExtractionGraph {
    type Error = anyhow::Error;

    fn try_from(value: ExtractionGraph) -> Result<Self> {
        let extraction_policies: Result<_, _> = value
            .extraction_policies
            .into_iter()
            .map(|policy| policy.try_into())
            .collect();
        Ok(Self {
            id: value.id,
            name: value.name,
            namespace: value.namespace,
            description: value.description.unwrap_or_default(),
            extraction_policies: extraction_policies?,
        })
    }
}

impl ExtractionGraph {
    pub fn create_id(name: &str, namespace: &str) -> String {
        let mut s = DefaultHasher::new();
        name.hash(&mut s);
        namespace.hash(&mut s);
        format!("{:x}", s.finish())
    }
}

impl ExtractionGraphBuilder {
    pub fn build(&mut self) -> Result<ExtractionGraph> {
        let name = self.name.clone().ok_or(anyhow!("name can't be empty"))?;
        let namespace = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace can't be empty"))?;
        let extraction_policies = self
            .extraction_policies
            .clone()
            .ok_or(anyhow!("child policies can't be empty"))?;
        let id = self
            .id
            .clone()
            .ok_or(anyhow!("extraction graph id can't be empty"))?;
        Ok(ExtractionGraph {
            id,
            name,
            namespace,
            extraction_policies,
            description: self.description.clone().unwrap_or_default(),
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExtractionGraphAnalytics {
    pub task_analytics: HashMap<String, TaskAnalytics>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskAnalytics {
    pub pending_tasks: u64,
    pub successful_tasks: u64,
    pub failed_tasks: u64,
}

impl TaskAnalytics {
    pub fn pending(&mut self) {
        self.pending_tasks += 1;
    }

    pub fn success(&mut self) {
        self.successful_tasks += 1;
        // This is for upgrade path from older versions
        if self.pending_tasks > 0 {
            self.pending_tasks -= 1;
        }
    }

    pub fn fail(&mut self) {
        self.failed_tasks += 1;
        if self.pending_tasks > 0 {
            self.pending_tasks -= 1;
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize, Hash)]
pub struct ExtractionGraphNode {
    pub namespace: String,
    pub graph_name: String,
    pub source: ContentSource,
}

/// Links a node in extraction graph to another graph.
/// All top level policies in the linked graph will be applied to the
/// specified node.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize)]
pub struct ExtractionGraphLink {
    pub node: ExtractionGraphNode,
    pub graph_name: String,
}

impl From<indexify_coordinator::LinkExtractionGraphsRequest> for ExtractionGraphLink {
    fn from(value: indexify_coordinator::LinkExtractionGraphsRequest) -> Self {
        Self {
            node: ExtractionGraphNode {
                namespace: value.namespace,
                graph_name: value.source_graph_name,
                source: value.content_source.into(),
            },
            graph_name: value.linked_graph_name,
        }
    }
}

pub type IndexName = String;
pub type IndexId = String;

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize, Default)]
pub struct Index {
    pub id: IndexId,
    pub namespace: String,
    pub name: IndexName,
    pub table_name: String,
    pub schema: String,
    pub extraction_policy_name: ExtractionPolicyName,
    pub extractor_name: ExtractorName,
    pub graph_name: ExtractionGraphName,
    pub visibility: bool,
}

impl Index {
    pub fn id(&self) -> String {
        let mut s = DefaultHasher::new();
        self.namespace.hash(&mut s);
        self.name.hash(&mut s);
        format!("{:x}", s.finish())
    }

    pub fn build_name(&self, output_name: &String) -> String {
        format!(
            "{}.{}.{}",
            self.graph_name,
            self.extraction_policy_name,
            output_name.to_string()
        )
    }

    pub fn build_table_name(&self, output_name: &String) -> String {
        format!(
            "{}.{}.{}.{}",
            self.namespace,
            self.graph_name,
            self.extraction_policy_name,
            output_name.to_string()
        )
    }
}

impl Hash for Index {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.name.hash(state);
    }
}

impl From<Index> for indexify_coordinator::Index {
    fn from(value: Index) -> Self {
        Self {
            name: value.name,
            table_name: value.table_name,
            schema: value.schema,
            extractor: value.extractor_name,
            extraction_policy: value.extraction_policy_name,
            namespace: value.namespace,
            graph_name: value.graph_name,
        }
    }
}

impl From<indexify_coordinator::Index> for Index {
    fn from(value: indexify_coordinator::Index) -> Self {
        let mut index = Index {
            id: "".to_string(),
            name: value.name,
            table_name: value.table_name,
            schema: value.schema,
            extractor_name: value.extractor,
            extraction_policy_name: value.extraction_policy,
            namespace: value.namespace,
            graph_name: value.graph_name,
            visibility: false,
        };
        index.id = index.id();
        index
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingSchema {
    pub dim: usize,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Embedding {
    pub values: Vec<f32>,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OutputSchema {
    #[serde(rename = "embedding")]
    Embedding(EmbeddingSchema),
    #[serde(rename = "attributes")]
    Attributes(HashMap<String, SchemaColumn>),
}

pub type ExtractorName = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ExtractorDescription {
    pub name: ExtractorName,
    pub description: String,
    pub input_params: serde_json::Value,
    pub outputs: HashMap<String, OutputSchema>,
    pub input_mime_types: Vec<String>,
}

impl From<ExtractorDescription> for indexify_coordinator::Extractor {
    fn from(value: ExtractorDescription) -> Self {
        let mut embedding_schemas = HashMap::new();
        let mut metadata_schemas = HashMap::new();
        for (output_name, schema) in value.outputs {
            match schema {
                OutputSchema::Embedding(embedding_schema) => {
                    embedding_schemas.insert(
                        output_name,
                        serde_json::to_string(&embedding_schema).unwrap(),
                    );
                }
                OutputSchema::Attributes(attributes) => {
                    metadata_schemas
                        .insert(output_name, serde_json::to_string(&attributes).unwrap());
                }
            }
        }
        Self {
            name: value.name,
            description: value.description,
            input_params: value.input_params.to_string(),
            embedding_schemas,
            input_mime_types: value.input_mime_types,
            metadata_schemas,
        }
    }
}

impl ExtractorDescription {
    pub fn validate_input_params(&self, input_params: &serde_json::Value) -> Result<()> {
        if input_params.eq(&serde_json::Value::Null) {
            return Ok(());
        }
        let input_params_schema = JSONSchema::compile(&self.input_params).map_err(|e| {
            anyhow!(
                "unable to compile json schema for input params: {:?}, error: {:?}",
                &self.input_params,
                e
            )
        })?;
        let validation_result = input_params_schema.validate(input_params);
        if let Err(errors) = validation_result {
            let errors = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>();
            return Err(anyhow!(
                "unable to validate input params for extractor policy: {}, errors: {}",
                &self.name,
                errors.join(",")
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct IngestionServerMetadata {
    pub id: String,
    pub addr: String,
    pub last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OutputColumn {
    #[serde(rename = "type")]
    pub output_type: String,
    pub comment: Option<String>,
}

impl From<indexify_coordinator::Extractor> for ExtractorDescription {
    fn from(value: indexify_coordinator::Extractor) -> Self {
        let mut output_schema = HashMap::new();
        for (output_name, embedding_schema) in value.embedding_schemas {
            let embedding_schema: EmbeddingSchema =
                serde_json::from_str(&embedding_schema).unwrap();
            output_schema.insert(output_name, OutputSchema::Embedding(embedding_schema));
        }
        for (output_name, metadata_schema) in value.metadata_schemas {
            let metadata_schema: HashMap<String, OutputColumn> =
                serde_json::from_str(&metadata_schema).unwrap();
            let mut attrs = HashMap::new();
            for (k, v) in metadata_schema {
                let column_type = match v.output_type.as_str() {
                    "integer" => SchemaColumnType::Int,
                    "string" => SchemaColumnType::Text,
                    "array" => SchemaColumnType::Array,
                    "object" => SchemaColumnType::Object,
                    "number" => SchemaColumnType::Float,
                    "boolean" => SchemaColumnType::Bool,
                    _ => SchemaColumnType::Object,
                };
                let comment = v.comment;
                let column = SchemaColumn {
                    column_type,
                    comment,
                };
                attrs.insert(k, column);
            }
            output_schema.insert(output_name, OutputSchema::Attributes(attrs));
        }
        Self {
            name: value.name,
            description: value.description,
            input_params: serde_json::from_str(&value.input_params).unwrap(),
            outputs: output_schema,
            input_mime_types: value.input_mime_types,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutorInfo {
    pub id: String,
    pub last_seen: u64,
    pub addr: String,
    pub extractors: Vec<ExtractorDescription>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractRequest {
    pub extractor_name: String,
    pub content: Content,
    pub input_params: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractResponse {
    pub content: Vec<Content>,
    pub features: Vec<Feature>,
}

#[derive(
    Debug, PartialEq, Eq, Serialize, Clone, Deserialize, EnumString, Display, SmartDefault,
)]
pub enum TaskState {
    #[default]
    Unknown,
    Pending,
    InProgress,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct CreateWork {
    pub namespace: String,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateWorkResponse {}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString)]
pub enum FeatureType {
    #[strum(serialize = "embedding")]
    #[serde(rename = "embedding")]
    Embedding,
    #[strum(serialize = "metadata")]
    #[serde(rename = "metadata")]
    Metadata,
    #[strum(serialize = "unknown")]
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    pub feature_type: FeatureType,
    pub name: String,
    pub data: serde_json::Value,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
// Specifying alternate name as utoipa has issues with namespaced schemas. See https://github.com/juhaku/utoipa/issues/569#issuecomment-1503466468
#[schema(as = internal_api::Content)]
pub struct Content {
    pub content_type: String,
    #[serde_as(as = "BytesOrString")]
    pub bytes: Vec<u8>,
    pub features: Vec<Feature>,
    pub labels: HashMap<String, serde_json::Value>,
}

impl Content {
    pub fn source_as_text(&self) -> Option<String> {
        let mime_type = mime::Mime::from_str(&self.content_type);
        if let Ok(mime_type) = mime_type {
            if mime_type == mime::TEXT_PLAIN {
                return Some(String::from_utf8(self.bytes.clone()).unwrap());
            }
        }
        None
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq, ToSchema, Default, Copy)]
#[schema(as = internal_api::TaskOutcome)]
pub enum TaskOutcome {
    #[default]
    Unknown,
    Success,
    Failed,
}

impl From<indexify_coordinator::TaskOutcome> for TaskOutcome {
    fn from(value: indexify_coordinator::TaskOutcome) -> Self {
        match value {
            indexify_coordinator::TaskOutcome::Unknown => TaskOutcome::Unknown,
            indexify_coordinator::TaskOutcome::Success => TaskOutcome::Success,
            indexify_coordinator::TaskOutcome::Failed => TaskOutcome::Failed,
        }
    }
}

impl From<TaskOutcome> for indexify_coordinator::TaskOutcome {
    fn from(value: TaskOutcome) -> Self {
        match value {
            TaskOutcome::Unknown => indexify_coordinator::TaskOutcome::Unknown,
            TaskOutcome::Success => indexify_coordinator::TaskOutcome::Success,
            TaskOutcome::Failed => indexify_coordinator::TaskOutcome::Failed,
        }
    }
}

impl From<bool> for TaskOutcome {
    fn from(value: bool) -> Self {
        if value {
            TaskOutcome::Success
        } else {
            TaskOutcome::Failed
        }
    }
}

fn default_creation_time() -> SystemTime {
    UNIX_EPOCH
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq, ToSchema)]
#[schema(as = internal_api::Task)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extraction_policy_name: String,
    pub extraction_graph_name: String,
    pub output_index_table_mapping: HashMap<String, String>,
    pub namespace: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
    #[schema(value_type = internal_api::TaskOutcome)]
    pub outcome: TaskOutcome,
    pub index_tables: Vec<String>, // list of index tables that this content may be present in
    #[serde(default = "default_creation_time")]
    pub creation_time: SystemTime,
}

impl Task {
    pub fn terminal_state(&self) -> bool {
        self.outcome != TaskOutcome::Unknown
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task(id: {}, extractor: {}, extraction_policy_id: {}, extraction_graph_name: {}, namespace: {}, content_id: {}, outcome: {:?})",
            self.id, self.extractor, self.extraction_policy_name, self.extraction_graph_name, self.namespace, self.content_metadata.id.id, self.outcome
        )
    }
}

impl TryFrom<Task> for indexify_coordinator::Task {
    type Error = anyhow::Error;

    fn try_from(value: Task) -> Result<Self> {
        let outcome: indexify_coordinator::TaskOutcome = value.outcome.into();
        Ok(indexify_coordinator::Task {
            id: value.id,
            extractor: value.extractor,
            namespace: value.namespace,
            content_metadata: Some(value.content_metadata.try_into()?),
            input_params: value.input_params.to_string(),
            extraction_policy_id: value.extraction_policy_name,
            extraction_graph_name: value.extraction_graph_name,
            output_index_mapping: value.output_index_table_mapping,
            outcome: outcome as i32,
            index_tables: value.index_tables,
        })
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct TaskOutcomeFilter(Option<TaskOutcome>);

impl TaskOutcomeFilter {
    pub fn matches(&self, outcome: TaskOutcome) -> bool {
        match self.0 {
            Some(filter) => filter == outcome,
            None => true,
        }
    }
}

impl From<TaskOutcomeFilter> for indexify_coordinator::TaskOutcomeFilter {
    fn from(value: TaskOutcomeFilter) -> Self {
        match value.0 {
            Some(TaskOutcome::Success) => indexify_coordinator::TaskOutcomeFilter::FilterSuccess,
            Some(TaskOutcome::Failed) => indexify_coordinator::TaskOutcomeFilter::FilterFailed,
            Some(TaskOutcome::Unknown) => indexify_coordinator::TaskOutcomeFilter::FilterUnknown,
            None => indexify_coordinator::TaskOutcomeFilter::FilterNotSet,
        }
    }
}

impl From<indexify_coordinator::TaskOutcomeFilter> for TaskOutcomeFilter {
    fn from(value: indexify_coordinator::TaskOutcomeFilter) -> Self {
        match value {
            indexify_coordinator::TaskOutcomeFilter::FilterSuccess => {
                TaskOutcomeFilter(Some(TaskOutcome::Success))
            }
            indexify_coordinator::TaskOutcomeFilter::FilterFailed => {
                TaskOutcomeFilter(Some(TaskOutcome::Failed))
            }
            indexify_coordinator::TaskOutcomeFilter::FilterUnknown => {
                TaskOutcomeFilter(Some(TaskOutcome::Unknown))
            }
            indexify_coordinator::TaskOutcomeFilter::FilterNotSet => TaskOutcomeFilter(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Copy)]
pub enum ServerTaskType {
    Delete = 0,
    UpdateLabels = 1,
    DeleteBlobStore = 2,
    DropIndexes = 3,
}

pub type GarbageCollectionTaskId = String;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[schema(as=internal_api::GarbageCollectionTask)]
pub struct GarbageCollectionTask {
    pub namespace: String,
    pub id: GarbageCollectionTaskId,
    pub latest: bool,
    pub content_id: ContentMetadataId,
    pub parent_content_id: Option<ContentMetadataId>,
    pub output_tables: HashSet<String>,
    #[schema(value_type = internal_api::TaskOutcome)]
    pub outcome: TaskOutcome,
    pub blob_store_path: String,
    pub assigned_to: Option<String>,
    pub task_type: ServerTaskType,
    #[serde(default)]
    pub change_offset: ContentOffset,
}

impl AsRef<GarbageCollectionTask> for GarbageCollectionTask {
    fn as_ref(&self) -> &GarbageCollectionTask {
        self
    }
}

impl GarbageCollectionTask {
    pub fn new(
        namespace: &str,
        content_metadata: ContentMetadata,
        output_tables: HashSet<String>,
        task_type: ServerTaskType,
    ) -> Self {
        let mut hasher = DefaultHasher::new();
        namespace.hash(&mut hasher);
        content_metadata.id.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        Self {
            namespace: namespace.to_string(),
            id,
            content_id: content_metadata.id,
            parent_content_id: content_metadata.parent_id,
            latest: content_metadata.latest,
            output_tables,
            outcome: TaskOutcome::Unknown,
            blob_store_path: content_metadata.storage_url,
            assigned_to: None,
            task_type,
            change_offset: content_metadata.change_offset,
        }
    }
}

impl From<GarbageCollectionTask> for indexify_coordinator::GcTask {
    fn from(value: GarbageCollectionTask) -> Self {
        Self {
            task_id: value.id,
            namespace: value.namespace,
            content_id: value.content_id.id,
            output_tables: value.output_tables.into_iter().collect::<Vec<String>>(),
            blob_store_path: value.blob_store_path,
            task_type: value.task_type as i32,
        }
    }
}

pub type ExtractionPolicyId = String;
pub type ExtractionPolicyName = String;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder, PartialEq, Eq)]
#[builder(build_fn(skip))]
pub struct ExtractionPolicy {
    pub id: ExtractionPolicyId,
    pub graph_name: ExtractionGraphName,
    pub name: ExtractionPolicyName,
    pub namespace: String,
    pub extractor: String,
    pub filter: LabelsFilter,
    pub input_params: serde_json::Value,
    // Extractor Output -> Table Name
    pub output_table_mapping: HashMap<String, String>,
    // The source of the content this policy will match against. Will either be the graph id or a
    // parent policy id
    pub content_source: ContentSource,
}

impl TryFrom<ExtractionPolicy> for indexify_coordinator::ExtractionPolicy {
    type Error = anyhow::Error;

    fn try_from(value: ExtractionPolicy) -> Result<Self> {
        let filter = value.filter.0.iter().map(|expr| expr.to_string()).collect();

        Ok(Self {
            id: value.id,
            extractor: value.extractor,
            name: value.name,
            filter,
            input_params: value.input_params.to_string(),
            content_source: value.content_source.into(),
            graph_name: value.graph_name,
            output_table_mapping: value.output_table_mapping,
        })
    }
}

impl ExtractionPolicy {
    pub fn create_id(graph_name: &str, name: &str, namespace: &str) -> String {
        let mut s = DefaultHasher::new();
        name.hash(&mut s);
        namespace.hash(&mut s);
        graph_name.hash(&mut s);
        format!("{:x}", s.finish())
    }
}

impl ExtractionPolicyBuilder {
    pub fn build(
        &self,
        graph_name: &str,
        extractor_description: ExtractorDescription,
    ) -> Result<ExtractionPolicy> {
        let input_params = self.input_params.clone().unwrap_or_default();
        extractor_description.validate_input_params(&input_params)?;
        let ns = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is not present"))?;
        let name = self.name.clone().ok_or(anyhow!("name is not present"))?;
        let extractor = self
            .extractor
            .clone()
            .ok_or(anyhow!("extractor is not present"))?;

        let content_source = self
            .content_source
            .clone()
            .ok_or(anyhow!("content source is not present"))?;
        let id = ExtractionPolicy::create_id(graph_name, &name, &ns);
        let mut output_table_mapping = HashMap::new();
        for (output_name, output_schema) in extractor_description.outputs {
            if let OutputSchema::Embedding(_) = output_schema {
                let index_table_name = format!("{}.{}.{}.{}", ns, graph_name, name, output_name);
                output_table_mapping.insert(output_name, index_table_name);
            }
        }
        Ok(ExtractionPolicy {
            id,
            graph_name: graph_name.to_string(),
            name,
            namespace: ns,
            extractor,
            filter: self.filter.clone().unwrap_or_default(),
            input_params,
            output_table_mapping,
            content_source,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContentMetadataId {
    pub id: String,
    pub version: u64,
}

impl Serialize for ContentMetadataId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}:{}", self.id, self.version);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for ContentMetadataId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(serde::de::Error::custom("expected id:version"));
        }
        Ok(ContentMetadataId {
            id: parts[0].to_string(),
            version: parts[1].parse().map_err(serde::de::Error::custom)?,
        })
    }
}

impl ContentMetadataId {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            version: 1,
        }
    }

    pub fn new_with_version(id: &str, version: u64) -> Self {
        Self {
            id: id.to_string(),
            version,
        }
    }
}

impl Display for ContentMetadataId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::v{}", self.id, self.version)
    }
}

impl TryFrom<&String> for ContentMetadataId {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self> {
        if value.is_empty() {
            return Ok(Self {
                id: "".to_string(),
                version: 0,
            });
        }

        let parts: Vec<&str> = value.split("::v").collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid ContentMetadataId"));
        }
        Ok(Self {
            id: parts[0].to_string(),
            version: parts[1].parse()?,
        })
    }
}

impl TryFrom<String> for ContentMetadataId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::try_from(&value)
    }
}

impl AsRef<[u8]> for ContentMetadataId {
    fn as_ref(&self) -> &[u8] {
        self.id.as_bytes()
    }
}

impl Default for ContentMetadataId {
    fn default() -> Self {
        Self {
            id: "test_id".to_string(),
            version: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ContentSource {
    Ingestion,
    ExtractionPolicyName(ExtractionPolicyName),
}

use indexify_coordinator::ContentSource as ProtoContentSource;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentSourceFilter(pub Option<ContentSource>);

impl From<ContentSourceFilter> for ProtoContentSource {
    fn from(value: ContentSourceFilter) -> Self {
        match value.0 {
            Some(ContentSource::Ingestion) => Self {
                value: Some(indexify_coordinator::content_source::Value::Ingestion(
                    indexify_coordinator::Empty {},
                )),
            },
            Some(ContentSource::ExtractionPolicyName(name)) => Self {
                value: Some(indexify_coordinator::content_source::Value::Policy(name)),
            },
            None => Self {
                value: Some(indexify_coordinator::content_source::Value::None(
                    indexify_coordinator::Empty {},
                )),
            },
        }
    }
}

impl TryFrom<ProtoContentSource> for ContentSourceFilter {
    type Error = tonic::Status;

    fn try_from(value: ProtoContentSource) -> Result<Self, Self::Error> {
        match value.value {
            Some(indexify_coordinator::content_source::Value::Ingestion(_)) => {
                Ok(ContentSourceFilter(Some(ContentSource::Ingestion)))
            }
            Some(indexify_coordinator::content_source::Value::Policy(name)) => Ok(
                ContentSourceFilter(Some(ContentSource::ExtractionPolicyName(name))),
            ),
            Some(indexify_coordinator::content_source::Value::None(_)) => {
                Ok(ContentSourceFilter(None))
            }
            None => Err(tonic::Status::invalid_argument("Invalid ContentSource")),
        }
    }
}

impl TryFrom<Option<ProtoContentSource>> for ContentSourceFilter {
    type Error = tonic::Status;

    fn try_from(value: Option<ProtoContentSource>) -> Result<Self, Self::Error> {
        match value {
            Some(value) => Self::try_from(value),
            None => Ok(ContentSourceFilter(None)),
        }
    }
}

impl ContentSourceFilter {
    pub fn matches(&self, source: &ContentSource) -> bool {
        match &self.0 {
            Some(filter) => filter == source,
            None => true,
        }
    }
}

impl Default for ContentSource {
    fn default() -> Self {
        ContentSource::ExtractionPolicyName(ExtractionPolicyName::default())
    }
}

impl From<ContentSource> for String {
    fn from(source: ContentSource) -> Self {
        String::from(&source)
    }
}

impl From<&ContentSource> for String {
    fn from(value: &ContentSource) -> Self {
        match value {
            ContentSource::Ingestion => "".to_string(),
            ContentSource::ExtractionPolicyName(id) => id.clone(),
        }
    }
}

impl From<String> for ContentSource {
    fn from(value: String) -> Self {
        if value.is_empty() {
            return ContentSource::Ingestion;
        }
        ContentSource::ExtractionPolicyName(value)
    }
}

impl From<&str> for ContentSource {
    fn from(value: &str) -> Self {
        if value.is_empty() {
            return ContentSource::Ingestion;
        }
        ContentSource::ExtractionPolicyName(value.to_string())
    }
}

impl Display for ContentSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentSource::Ingestion => write!(f, ""),
            ContentSource::ExtractionPolicyName(name) => write!(f, "{}", name),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy, Default)]
pub struct ContentOffset(pub u64);

impl ContentOffset {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct ContentMetadata {
    pub id: ContentMetadataId,
    pub parent_id: Option<ContentMetadataId>,
    pub root_content_id: Option<String>,
    pub latest: bool, // if true this is the latest version of content with same ids
    // Namespace name == Namespace ID
    pub namespace: NamespaceName,
    pub name: String,
    pub content_type: String,
    pub labels: HashMap<String, serde_json::Value>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: ContentSource,
    pub size_bytes: u64,
    pub tombstoned: bool,
    pub hash: String,
    pub extraction_policy_ids: HashMap<ExtractionPolicyId, u64>, /*  map of completion time for
                                                                  * each extraction policy id */
    pub extraction_graph_names: Vec<ExtractionGraphName>,
    /// monotonically increasing change id
    #[serde(default)]
    pub change_offset: ContentOffset,

    #[serde(default)]
    pub extracted_metadata: serde_json::Value,
}

impl ContentMetadata {
    pub fn get_root_id(&self) -> &str {
        self.root_content_id.as_ref().unwrap_or(&self.id.id)
    }

    // Return key to store structure in k/v store. The latest version of root and
    // children are stored with id as key (children always have version 1 and
    // are never overwritten). Overwritten or deleted roots keys are
    // formed from id and version.
    pub fn id_key(&self) -> String {
        if self.latest {
            self.id.id.clone()
        } else {
            format!("{}::v{}", self.id.id, self.id.version)
        }
    }

    pub fn make_id_key(id: &str, version: Option<u64>) -> String {
        match version {
            None => id.to_string(),
            Some(v) => format!("{}::v{}", id, v),
        }
    }
}

impl TryFrom<ContentMetadata> for indexify_coordinator::ContentMetadata {
    type Error = anyhow::Error;

    fn try_from(value: ContentMetadata) -> Result<Self> {
        let labels = utils::convert_map_serde_to_prost_json(value.labels)?;
        Ok(Self {
            id: value.id.id, //  don't expose the version on the task
            parent_id: value.parent_id.map(|id| id.id).unwrap_or_default(), /*  don't expose the
                              * version on the
                              * task */
            root_content_id: value.root_content_id.unwrap_or_default(),
            file_name: value.name,
            mime: value.content_type,
            labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            namespace: value.namespace,
            source: value.source.to_string(),
            size_bytes: value.size_bytes,
            hash: value.hash,
            extraction_policy_ids: value.extraction_policy_ids,
            extraction_graph_names: value.extraction_graph_names,
            extracted_metadata: value.extracted_metadata.to_string(),
        })
    }
}

impl TryFrom<indexify_coordinator::ContentMetadata> for ContentMetadata {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::ContentMetadata) -> Result<Self> {
        let labels = utils::convert_map_prost_to_serde_json(value.labels)?;

        let root_content_id = if value.root_content_id.is_empty() {
            None
        } else {
            Some(value.root_content_id)
        };
        let parent_id = if value.parent_id.is_empty() {
            None
        } else {
            Some(ContentMetadataId::new(&value.parent_id))
        };
        Ok(Self {
            id: ContentMetadataId {
                id: value.id,
                version: 1,
            },
            parent_id,
            root_content_id,
            latest: true,
            name: value.file_name,
            content_type: value.mime,
            labels,
            storage_url: value.storage_url,
            created_at: value.created_at,
            namespace: value.namespace,
            source: value.source.into(),
            size_bytes: value.size_bytes,
            tombstoned: false,
            hash: value.hash,
            extraction_policy_ids: value.extraction_policy_ids,
            extraction_graph_names: value.extraction_graph_names,
            change_offset: ContentOffset(0),
            extracted_metadata: serde_json::from_str(&value.extracted_metadata)?,
        })
    }
}

impl Default for ContentMetadata {
    fn default() -> Self {
        let test_labels = {
            let mut labels = HashMap::new();
            labels.insert("key1".to_string(), serde_json::json!("value1"));
            labels.insert("key2".to_string(), serde_json::json!(25));
            labels
        };

        Self {
            id: ContentMetadataId::default(),
            parent_id: None,
            root_content_id: Some(ContentMetadataId::default().id),
            latest: true,
            namespace: "test_namespace".to_string(),
            name: "test_name".to_string(),
            content_type: "test_content_type".to_string(),
            labels: test_labels,
            storage_url: "http://example.com/test_url".to_string(),
            created_at: 1234567890, // example timestamp
            source: ContentSource::Ingestion,
            size_bytes: 1234567890,
            extraction_policy_ids: HashMap::new(),
            tombstoned: false,
            hash: "test_hash".to_string(),
            extraction_graph_names: vec![],
            change_offset: ContentOffset(0),
            extracted_metadata: serde_json::Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutorMetadata {
    pub id: String,
    pub last_seen: u64,
    pub addr: String,
    pub extractors: Vec<ExtractorDescription>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExtractorHeartbeat {
    pub executor_id: String,
    pub extractor: ExtractorDescription,
    pub addr: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ExtractorHeartbeatResponse {
    pub content_to_process: Vec<Task>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TaskResult {
    pub task_id: String,
    pub outcome: TaskOutcome,
    pub extracted_content: Vec<Content>,
    pub error_msg: Option<String>,
}

impl TaskResult {
    pub fn failed(task_id: &str, msg: Option<String>) -> Self {
        Self {
            task_id: task_id.to_string(),
            outcome: TaskOutcome::Failed,
            extracted_content: Vec::new(),
            error_msg: msg,
        }
    }

    pub fn success(task_id: &str, extracted_content: Vec<Content>) -> Self {
        Self {
            task_id: task_id.to_string(),
            outcome: TaskOutcome::Success,
            extracted_content,
            error_msg: None,
        }
    }
}

pub type NamespaceName = String;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ChangeType {
    NewContent,
    TombstoneContentTree,
    ExecutorAdded,
    ExecutorRemoved,
    ContentUpdated,
    TaskCompleted { root_content_id: ContentMetadataId },
    AddGraphToContent { extraction_graph: String },
    ExtractionGraphDeleted { start_content_id: Vec<u8> },
    TombstoneContent { is_root: bool },
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::NewContent => write!(f, "NewContent"),
            ChangeType::TombstoneContentTree => write!(f, "TombstoneContentTree"),
            ChangeType::ExecutorAdded => write!(f, "ExecutorAdded"),
            ChangeType::ExecutorRemoved => write!(f, "ExecutorRemoved"),
            ChangeType::ContentUpdated => write!(f, "ContentUpdated"),
            ChangeType::TaskCompleted {
                root_content_id: content_id,
            } => write!(f, "TaskCompleted(content_id: {})", content_id),
            ChangeType::AddGraphToContent { extraction_graph } => write!(
                f,
                "AddGraphToContent(extraction_graph: {})",
                extraction_graph,
            ),
            ChangeType::ExtractionGraphDeleted { start_content_id } => write!(
                f,
                "ExtractionGraphDeleted(start_content_id: {:?})",
                start_content_id
            ),
            ChangeType::TombstoneContent { is_root } => write!(f, "TombstoneContent: {}", is_root),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Copy, Ord, PartialOrd)]
pub struct StateChangeId(u64);

impl StateChangeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Return key to store in k/v db
    pub fn to_key(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_key(key: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(key))
    }
}

impl From<StateChangeId> for u64 {
    fn from(value: StateChangeId) -> Self {
        value.0
    }
}

impl Display for StateChangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StateChange {
    pub id: StateChangeId,
    pub object_id: String,
    pub change_type: ChangeType,
    pub created_at: u64,
    pub processed_at: Option<u64>,

    /// If Some, this change holds a reference to an object until it is
    /// processed.
    pub refcnt_object_id: Option<String>,
}

impl StateChange {
    pub fn new(object_id: String, change_type: ChangeType, created_at: u64) -> Self {
        Self {
            id: StateChangeId(0),
            object_id,
            change_type,
            created_at,
            processed_at: None,
            refcnt_object_id: None,
        }
    }

    pub fn new_with_refcnt(
        object_id: String,
        change_type: ChangeType,
        created_at: u64,
        refcnt_object_id: String,
    ) -> Self {
        Self {
            id: StateChangeId(0),
            object_id,
            change_type,
            created_at,
            processed_at: None,
            refcnt_object_id: Some(refcnt_object_id),
        }
    }
}

impl TryFrom<indexify_coordinator::StateChange> for StateChange {
    type Error = anyhow::Error;

    fn try_from(value: indexify_coordinator::StateChange) -> Result<Self> {
        let change_type = match value.change_type.as_str() {
            "NewContent" => ChangeType::NewContent,
            "ExecutorAdded" => ChangeType::ExecutorAdded,
            "ExecutorRemoved" => ChangeType::ExecutorRemoved,
            _ => return Err(anyhow!("Invalid ChangeType")),
        };
        Ok(Self {
            id: StateChangeId(value.id),
            object_id: value.object_id,
            change_type,
            created_at: value.created_at,
            processed_at: Some(value.processed_at),
            refcnt_object_id: None,
        })
    }
}

impl From<StateChange> for indexify_coordinator::StateChange {
    fn from(value: StateChange) -> Self {
        Self {
            id: value.id.into(),
            object_id: value.object_id,
            change_type: value.change_type.to_string(),
            created_at: value.created_at,
            processed_at: value.processed_at.unwrap_or(0),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ExtractedEmbeddings {
    pub content_id: String,
    pub embedding: Vec<f32>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub root_content_metadata: Option<ContentMetadata>,
    pub content_metadata: ContentMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaColumn {
    #[serde(rename = "type")]
    column_type: SchemaColumnType,
    comment: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaColumnType {
    Null,
    Array,
    Int,
    BigInt,
    Text,
    Float,
    Bool,
    Object,
}

impl From<SchemaColumnType> for SchemaColumn {
    fn from(column_type: SchemaColumnType) -> Self {
        Self {
            column_type,
            comment: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct StructuredDataSchema {
    pub id: String,
    pub extraction_graph_name: String,
    pub namespace: String,
    pub columns: BTreeMap<String, SchemaColumn>,
}

impl StructuredDataSchema {
    pub fn new(extraction_graph_name: &str, namespace: &str) -> Self {
        let id = Self::schema_id(namespace, extraction_graph_name);
        Self {
            id,
            namespace: namespace.to_string(),
            extraction_graph_name: extraction_graph_name.to_string(),
            columns: BTreeMap::new(),
        }
    }

    pub fn merge(&mut self, other: HashMap<String, SchemaColumn>) -> Self {
        for (column_name, column) in other {
            self.columns.insert(column_name, column);
        }
        self.clone()
    }

    pub fn schema_id(namespace: &str, extraction_graph_name: &str) -> String {
        let mut s = DefaultHasher::new();
        namespace.hash(&mut s);
        extraction_graph_name.hash(&mut s);
        format!("{:x}", s.finish())
    }

    pub fn to_ddl(&self) -> String {
        let mut columns = vec![r#""content_id" TEXT NULL"#.to_string()];

        for (column_name, column) in &self.columns {
            let SchemaColumn {
                column_type,
                comment,
            } = column;
            let dtype = match column_type {
                SchemaColumnType::Null => "OBJECT",
                SchemaColumnType::Array => "LIST",
                SchemaColumnType::BigInt => "BIGINT",
                SchemaColumnType::Bool => "BOOLEAN",
                SchemaColumnType::Float => "FLOAT",
                SchemaColumnType::Int => "INT",
                SchemaColumnType::Text => "TEXT",
                SchemaColumnType::Object => "JSON",
            };
            let mut column = format!(r#""{}" {} NULL"#, column_name, dtype);

            if let Some(comment) = comment {
                column.push_str(&format!(" COMMENT '{}'", comment));
            }

            columns.push(column);
        }

        let column_str = columns.join(", ");
        let schema_str = format!(
            r#"CREATE TABLE IF NOT EXISTS "{}" ({});"#,
            self.extraction_graph_name, column_str
        );

        schema_str
    }
}

//#[cfg(test)]
//mod test {
//    use super::*;
//
//    #[test]
//    fn test_structured_data_schema() {
//        let schema = StructuredDataSchema::new("test", "test-namespace");
//        let other = HashMap::from([
//            (
//                "bounding_box".to_string(),
//                SchemaColumn {
//                    column_type: SchemaColumnType::Object,
//                    comment: Some("Bounding box of the object".to_string()),
//                },
//            ),
//            ("object_class".to_string(), SchemaColumnType::Text.into()),
//        ]);
//        let result = schema.merge(other).unwrap();
//        let other1 = HashMap::from([
//            ("a".to_string(), SchemaColumnType::Int.into()),
//            ("b".to_string(), SchemaColumnType::Text.into()),
//        ]);
//        let result1 = result.merge(other1).unwrap();
//        assert_eq!(result1.id, "test");
//        assert_eq!(result1.namespace, "test-namespace");
//        assert_eq!(result1.columns.len(), 4);
//
//        let ddl = result1.to_ddl();
//        assert_eq!(
//            ddl,
//            "CREATE TABLE IF NOT EXISTS \"test\" (\
//                \"content_id\" TEXT NULL, \
//                \"a\" INT NULL, \
//                \"b\" TEXT NULL, \
//                \"bounding_box\" JSON NULL COMMENT 'Bounding box of the
// object', \                \"object_class\" TEXT NULL\
//            );"
//        );
//    }
//}
