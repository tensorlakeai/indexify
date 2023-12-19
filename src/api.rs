use std::collections::HashMap;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, BytesOrString};
use smart_default::SmartDefault;
use strum::{Display, EnumString};
use utoipa::{IntoParams, ToSchema};

use crate::{persistence, vectordbs};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, EnumString, Display)]
#[serde(rename = "extractor_filter")]
pub enum ExtractorFilter {
    #[serde(rename = "eq")]
    Eq {
        #[serde(flatten)]
        filters: HashMap<String, serde_json::Value>,
    },
    #[serde(rename = "neq")]
    Neq {
        #[serde(flatten)]
        filters: HashMap<String, serde_json::Value>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractorBinding {
    pub extractor: String,
    pub name: String,
    pub filters: Option<Vec<ExtractorFilter>>,
    pub input_params: Option<serde_json::Value>,
}

impl From<persistence::ExtractorBinding> for ExtractorBinding {
    fn from(value: persistence::ExtractorBinding) -> Self {
        let mut eq_filters = HashMap::new();
        let mut neq_filters = HashMap::new();
        for filter in value.filters {
            match filter {
                persistence::ExtractorFilter::Eq { field, value } => {
                    eq_filters.insert(field, value);
                }
                persistence::ExtractorFilter::Neq { field, value } => {
                    neq_filters.insert(field, value);
                }
            }
        }
        let mut filters = vec![];
        if !eq_filters.is_empty() {
            filters.push(ExtractorFilter::Eq {
                filters: eq_filters,
            });
        }
        if !neq_filters.is_empty() {
            filters.push(ExtractorFilter::Neq {
                filters: neq_filters,
            });
        }
        Self {
            name: value.name,
            extractor: value.extractor,
            filters: Some(filters),
            input_params: Some(value.input_params),
        }
    }
}

pub fn into_persistence_extractor_binding(
    repository: &str,
    extractor_binding: ExtractorBinding,
) -> persistence::ExtractorBinding {
    let mut extraction_filters = vec![];
    for filter in extractor_binding.filters.unwrap_or_default() {
        match filter {
            ExtractorFilter::Eq { filters } => {
                for (field, value) in filters {
                    extraction_filters.push(persistence::ExtractorFilter::Eq { field, value });
                }
            }
            ExtractorFilter::Neq { filters } => {
                for (field, value) in filters {
                    extraction_filters.push(persistence::ExtractorFilter::Neq { field, value });
                }
            }
        }
    }
    persistence::ExtractorBinding::new(
        &extractor_binding.name,
        repository,
        extractor_binding.extractor.clone(),
        extraction_filters,
        extractor_binding
            .input_params
            .unwrap_or(serde_json::json!({})),
    )
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DataRepository {
    pub name: String,
    pub extractor_bindings: Vec<ExtractorBinding>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<persistence::DataRepository> for DataRepository {
    fn from(value: persistence::DataRepository) -> Self {
        let ap_extractors = value
            .extractor_bindings
            .into_iter()
            .map(|e| e.into())
            .collect();
        DataRepository {
            name: value.name,
            extractor_bindings: ap_extractors,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault, ToSchema)]
pub struct CreateRepository {
    pub name: String,
    pub extractor_bindings: Vec<ExtractorBinding>,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateRepositoryResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetRepositoryResponse {
    pub repository: DataRepository,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListRepositoriesResponse {
    pub repositories: Vec<DataRepository>,
}

#[derive(Display, Debug, Serialize, Deserialize, Clone, Default, ToSchema)]
#[serde(rename = "distance")]
pub enum IndexDistance {
    #[serde(rename = "dot")]
    #[strum(serialize = "dot")]
    #[default]
    Dot,

    #[serde(rename = "cosine")]
    #[strum(serialize = "cosine")]
    Cosine,

    #[serde(rename = "euclidean")]
    #[strum(serialize = "euclidean")]
    Euclidean,
}

impl From<IndexDistance> for vectordbs::IndexDistance {
    fn from(value: IndexDistance) -> Self {
        match value {
            IndexDistance::Dot => vectordbs::IndexDistance::Dot,
            IndexDistance::Cosine => vectordbs::IndexDistance::Cosine,
            IndexDistance::Euclidean => vectordbs::IndexDistance::Euclidean,
        }
    }
}

impl From<vectordbs::IndexDistance> for IndexDistance {
    fn from(val: vectordbs::IndexDistance) -> Self {
        match val {
            vectordbs::IndexDistance::Dot => IndexDistance::Dot,
            vectordbs::IndexDistance::Cosine => IndexDistance::Cosine,
            vectordbs::IndexDistance::Euclidean => IndexDistance::Euclidean,
        }
    }
}

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExtractorBindRequest {
    #[serde(flatten)]
    pub extractor_binding: ExtractorBinding,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ExtractorBindResponse {
    #[serde(default)]
    pub index_names: Vec<String>
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Text {
    pub text: String,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TextAddRequest {
    pub documents: Vec<Text>,
    pub sync: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunExtractorsResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ExtractorOutputSchema {
    #[serde(rename = "embedding")]
    Embedding { dim: usize, distance: IndexDistance },
    #[serde(rename = "attributes")]
    Attributes { schema: serde_json::Value },
}

impl From<persistence::ExtractorOutputSchema> for ExtractorOutputSchema {
    fn from(value: persistence::ExtractorOutputSchema) -> Self {
        match value {
            persistence::ExtractorOutputSchema::Embedding(schema) => {
                ExtractorOutputSchema::Embedding {
                    dim: schema.dim,
                    distance: schema.distance.into(),
                }
            }
            persistence::ExtractorOutputSchema::Attributes(schema) => {
                ExtractorOutputSchema::Attributes {
                    schema: schema.schema,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractorSchema {
    pub outputs: HashMap<String, ExtractorOutputSchema>,
}

impl From<persistence::ExtractorSchema> for ExtractorSchema {
    fn from(value: persistence::ExtractorSchema) -> Self {
        Self {
            outputs: value
                .outputs
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExtractorDescription {
    pub name: String,
    pub description: String,
    pub input_params: serde_json::Value,
    pub schemas: ExtractorSchema,
}

impl From<persistence::Extractor> for ExtractorDescription {
    fn from(value: persistence::Extractor) -> Self {
        Self {
            name: value.name,
            description: value.description,
            input_params: value.input_params,
            schemas: value.schemas.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Executor {
    pub id: String,
    pub extractors: Vec<ExtractorDescription>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListExecutorsResponse {
    pub executors: Vec<Executor>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct ListExtractorsResponse {
    pub extractors: Vec<ExtractorDescription>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct TextAdditionResponse {}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Index {
    pub name: String,
    pub schema: ExtractorOutputSchema,
}

impl From<persistence::Index> for Index {
    fn from(value: persistence::Index) -> Self {
        Self {
            name: value.name,
            schema: value.schema.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListIndexesResponse {
    pub indexes: Vec<Index>,
}

#[derive(Debug, Serialize, Deserialize, IntoParams, ToSchema)]
pub struct SearchRequest {
    pub index: String,
    pub query: String,
    pub k: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtractedAttributes {
    pub id: String,
    pub content_id: String,
    pub attributes: serde_json::Value,
    pub extractor_name: String,
}

impl From<persistence::ExtractedAttributes> for ExtractedAttributes {
    fn from(value: persistence::ExtractedAttributes) -> Self {
        Self {
            id: value.id,
            content_id: value.content_id,
            attributes: value.attributes,
            extractor_name: value.extractor_name,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, IntoParams, ToSchema)]
pub struct AttributeLookupRequest {
    pub content_id: Option<String>,
    pub index: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AttributeLookupResponse {
    pub attributes: Vec<ExtractedAttributes>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, ToSchema)]
pub struct Event {
    text: String,
    unix_timestamp: Option<u64>,
    metadata: HashMap<String, serde_json::Value>,
}

impl From<Event> for persistence::Event {
    fn from(value: Event) -> Self {
        persistence::Event::new(&value.text, value.unix_timestamp, value.metadata)
    }
}

impl From<persistence::Event> for Event {
    fn from(value: persistence::Event) -> Self {
        Self {
            text: value.message,
            unix_timestamp: Some(value.unix_timestamp),
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EventAddRequest {
    pub events: Vec<Event>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EventAddResponse {}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListEventsResponse {
    pub messages: Vec<Event>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct DocumentFragment {
    pub content_id: String,
    pub text: String,
    pub confidence_score: f32,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default, ToSchema)]
pub struct IndexSearchResponse {
    pub results: Vec<DocumentFragment>,
}
pub struct IndexifyAPIError {
    status_code: StatusCode,
    message: String,
}

impl IndexifyAPIError {
    pub fn new(status_code: StatusCode, message: String) -> Self {
        Self {
            status_code,
            message,
        }
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        (self.status_code, self.message).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, EnumString)]
pub enum FeatureType {
    #[strum(serialize = "embedding")]
    Embedding,
    #[strum(serialize = "ner")]
    NamedEntity,
    #[strum(serialize = "metadata")]
    Metadata,
    #[strum(serialize = "unknown")]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feature {
    pub feature_type: FeatureType,
    pub name: String,
    pub data: serde_json::Value,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct Content {
    pub content_type: String,
    #[serde_as(as = "BytesOrString")]
    pub source: Vec<u8>,
    pub feature: Option<Feature>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractRequest {
    pub content: Content,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExtractResponse {
    pub content: Vec<Content>,
}
