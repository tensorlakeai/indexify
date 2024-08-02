use std::{collections::HashMap, time::SystemTime};

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::{ExtractionGraphId, ExtractionGraphName};

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq)]
pub struct Task {
    pub id: String,
    pub extractor: String,
    pub extraction_policy_id: String,
    pub extraction_graph_name: String,
    pub output_index_table_mapping: HashMap<String, String>,
    pub namespace: String,
    pub content_metadata: super::ContentMetadata,
    pub input_params: serde_json::Value,
    pub outcome: super::TaskOutcome,
    pub index_tables: Vec<String>, // list of index tables that this content may be present in
    #[serde(default = "super::default_creation_time")]
    pub creation_time: SystemTime,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Deserialize, Default)]
pub struct ExtractionPolicy {
    pub id: super::ExtractionPolicyId,
    pub graph_name: super::ExtractionGraphName,
    pub name: super::ExtractionPolicyName,
    pub namespace: String,
    pub extractor: String,
    pub filters: HashMap<String, serde_json::Value>,
    pub input_params: serde_json::Value,
    pub output_table_mapping: HashMap<String, String>,
    pub content_source: super::ContentSource,
}

impl From<ExtractionPolicy> for super::ExtractionPolicy {
    fn from(policy: ExtractionPolicy) -> Self {
        let expressions: Vec<_> = policy
            .filters
            .into_iter()
            .map(|(k, v)| filter::Expression {
                key: k,
                value: v,
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

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ExtractionGraph {
    pub id: ExtractionGraphId,
    pub name: ExtractionGraphName,
    pub namespace: String,
    #[serde(default)]
    pub description: Option<String>,
    pub extraction_policies: Vec<ExtractionPolicy>,
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
                .into_iter()
                .map(|p| p.into())
                .collect(),
        }
    }
}
