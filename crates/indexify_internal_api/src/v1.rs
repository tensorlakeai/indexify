use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExtractionGraph {
    pub id: super::ExtractionGraphId,
    pub name: super::ExtractionGraphName,
    pub namespace: String,
    pub extraction_policies: Vec<ExtractionPolicy>,
}

impl From<ExtractionGraph> for super::ExtractionGraph {
    fn from(graph: ExtractionGraph) -> Self {
        super::ExtractionGraph {
            id: graph.id,
            name: graph.name,
            namespace: graph.namespace,
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
    pub content_source: super::ExtractionPolicyContentSource,
}

impl From<ExtractionPolicy> for super::ExtractionPolicy {
    fn from(policy: ExtractionPolicy) -> Self {
        super::ExtractionPolicy {
            filters: policy
                .filters
                .iter()
                .map(|(k, v)| (k.clone(), from_str_to_json(v)))
                .collect(),
            ..policy.into()
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
}

impl From<ContentMetadata> for super::ContentMetadata {
    fn from(metadata: ContentMetadata) -> Self {
        super::ContentMetadata {
            labels: metadata
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), from_str_to_json(v)))
                .collect(),
            ..metadata.into()
        }
    }
}

fn from_str_to_json(value: &str) -> serde_json::Value {
    serde_json::from_str(value).unwrap_or(serde_json::json!(value))
}
