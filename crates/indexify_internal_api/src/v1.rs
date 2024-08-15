use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractionGraph {
    pub id: super::ExtractionGraphId,
    pub name: super::ExtractionGraphName,
    pub namespace: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
    pub description: Option<String>,
}

impl From<ExtractionGraph> for super::ExtractionGraph {
    fn from(graph: ExtractionGraph) -> Self {
        super::ExtractionGraph {
            id: graph.id,
            name: graph.name,
            namespace: graph.namespace,
            description: graph.description,
            extraction_policies: graph
                .extraction_policies
                .iter()
                .map(|p| p.clone().into())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize, Default)]
pub struct ExtractionPolicy {
    pub id: super::ExtractionPolicyId,
    pub graph_name: super::ExtractionGraphName,
    pub name: super::ExtractionPolicyName,
    pub namespace: String,
    pub extractor: String,
    pub filters: HashMap<String, String>,
    pub input_params: serde_json::Value,
    pub output_table_mapping: HashMap<String, String>,
    pub content_source: super::ContentSource,
}

impl From<ExtractionPolicy> for super::ExtractionPolicy {
    fn from(policy: ExtractionPolicy) -> Self {
        let expressions: Vec<_> = policy
            .filters
            .iter()
            .map(|(k, v)| filter::Expression {
                key: k.clone(),
                value: from_str_to_json(v),
                operator: filter::Operator::Eq,
            })
            .collect();
        super::ExtractionPolicy {
            filter: filter::LabelsFilter(expressions),
            id: policy.id,
            graph_name: policy.graph_name,
            name: policy.name,
            namespace: policy.namespace,
            extractor: policy.extractor,
            input_params: policy.input_params,
            output_table_mapping: policy.output_table_mapping,
            content_source: policy.content_source,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContentMetadata {
    pub id: super::ContentMetadataId,
    pub parent_id: Option<super::ContentMetadataId>,
    pub root_content_id: Option<String>,
    pub latest: bool,
    pub namespace: super::NamespaceName,
    pub name: String,
    pub content_type: String,
    pub labels: HashMap<String, String>,
    pub storage_url: String,
    pub created_at: i64,
    pub source: super::ContentSource,
    pub size_bytes: u64,
    pub tombstoned: bool,
    pub hash: String,
    pub extraction_policy_ids: HashMap<super::ExtractionPolicyId, u64>,
    pub extraction_graph_names: Vec<super::ExtractionGraphName>,
    pub extracted_metadata: serde_json::Value,
}

impl From<ContentMetadata> for super::ContentMetadata {
    fn from(metadata: ContentMetadata) -> Self {
        super::ContentMetadata {
            labels: metadata
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), from_str_to_json(v)))
                .collect(),
            id: metadata.id,
            parent_id: metadata.parent_id,
            root_content_id: metadata.root_content_id,
            latest: metadata.latest,
            namespace: metadata.namespace,
            source: metadata.source,
            extraction_policy_ids: metadata.extraction_policy_ids,
            extraction_graph_names: metadata.extraction_graph_names,
            name: metadata.name,
            storage_url: metadata.storage_url,
            created_at: metadata.created_at,
            size_bytes: metadata.size_bytes,
            tombstoned: metadata.tombstoned,
            hash: metadata.hash,
            content_type: metadata.content_type,
            change_offset: super::ContentOffset(0),
            extracted_metadata: metadata.extracted_metadata,
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extraction_policy_id: String,
    pub extraction_graph_name: String,
    pub output_index_table_mapping: HashMap<String, String>,
    pub namespace: String,
    pub content_metadata: ContentMetadata,
    pub input_params: serde_json::Value,
    pub outcome: crate::TaskOutcome,
    pub index_tables: Vec<String>,
}

fn from_str_to_json(value: &str) -> serde_json::Value {
    serde_json::from_str(value).unwrap_or(serde_json::json!(value))
}
