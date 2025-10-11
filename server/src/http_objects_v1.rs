use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{self, ApplicationBuilder, RequestCtx},
    executor_api::executor_api_pb::DataPayloadEncoding,
    http_objects::{
        ApplicationFunction,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        IndexifyAPIError,
        RequestError,
    },
    utils::get_epoch_time_in_ms,
};

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct EntryPointManifest {
    pub function_name: String,
    pub input_serializer: String,
    pub output_serializer: String,
    pub output_type_hints_base64: String,
}

impl From<data_model::ApplicationEntryPoint> for EntryPointManifest {
    fn from(entrypoint: data_model::ApplicationEntryPoint) -> Self {
        Self {
            function_name: entrypoint.function_name,
            input_serializer: entrypoint.input_serializer,
            output_serializer: entrypoint.output_serializer,
            output_type_hints_base64: entrypoint.output_type_hints_base64.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ApplicationState {
    #[default]
    Active,
    Disabled {
        reason: String,
    },
}

impl From<data_model::ApplicationState> for ApplicationState {
    fn from(state: data_model::ApplicationState) -> Self {
        match state {
            data_model::ApplicationState::Active => ApplicationState::Active,
            data_model::ApplicationState::Disabled { reason } => {
                ApplicationState::Disabled { reason }
            }
        }
    }
}

impl From<ApplicationState> for data_model::ApplicationState {
    fn from(state: ApplicationState) -> Self {
        match state {
            ApplicationState::Active => data_model::ApplicationState::Active,
            ApplicationState::Disabled { reason } => {
                data_model::ApplicationState::Disabled { reason }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Application {
    pub name: String,
    pub namespace: String,
    pub description: String,
    // This is not supplied by the client, do we need this here?
    #[serde(default)]
    pub tombstoned: bool,
    pub version: String,
    pub tags: HashMap<String, String>,
    pub functions: HashMap<String, ApplicationFunction>,
    #[serde(default = "get_epoch_time_in_ms")]
    pub created_at: u64,
    pub entrypoint: EntryPointManifest,
    // state is not something that a client should be able to set.
    // It's managed by the "change-state" internal endpoint.
    #[serde(default, skip_deserializing)]
    state: ApplicationState,
}

impl Application {
    pub fn into_data_model(
        self,
        code_path: &str,
        sha256_hash: &str,
        size: u64,
    ) -> Result<data_model::Application, IndexifyAPIError> {
        let mut functions = HashMap::new();
        for (name, node) in self.functions {
            node.validate()?;
            let converted_node: data_model::Function = node.try_into().map_err(|e| {
                IndexifyAPIError::bad_request(&format!(
                    "Invalid placement constraints in function '{name}': {e}"
                ))
            })?;
            functions.insert(name, converted_node);
        }
        let Some(_start_fn) = functions.get(&self.entrypoint.function_name) else {
            return Err(IndexifyAPIError::bad_request(&format!(
                "Entry point function '{}' not found",
                self.entrypoint.function_name
            )));
        };

        let application = ApplicationBuilder::default()
            .name(self.name)
            .namespace(self.namespace)
            .description(self.description)
            .tags(self.tags)
            .version(self.version)
            .code(data_model::DataPayload {
                id: nanoid::nanoid!(),
                metadata_size: 0,
                offset: 0,
                encoding: DataPayloadEncoding::BinaryZip.as_str_name().to_string(),
                sha256_hash: sha256_hash.to_string(),
                size,
                path: code_path.to_string(),
            })
            .functions(functions)
            .created_at(self.created_at)
            .tombstoned(self.tombstoned)
            .state(self.state.into())
            .entrypoint(data_model::ApplicationEntryPoint {
                function_name: self.entrypoint.function_name,
                input_serializer: self.entrypoint.input_serializer,
                output_serializer: self.entrypoint.output_serializer,
                output_type_hints_base64: self.entrypoint.output_type_hints_base64,
            })
            .build()
            .map_err(|e| {
                IndexifyAPIError::bad_request(&format!("Failed to create application: {e}"))
            })?;
        Ok(application)
    }
}

impl From<data_model::Application> for Application {
    fn from(application: data_model::Application) -> Self {
        let mut nodes = HashMap::new();
        for (k, v) in application.functions.into_iter() {
            nodes.insert(k, v.into());
        }
        Self {
            name: application.name,
            namespace: application.namespace,
            description: application.description,
            tags: application.tags,
            entrypoint: application.entrypoint.into(),
            version: application.version,
            functions: nodes,
            created_at: application.created_at,
            tombstoned: application.tombstoned,
            state: application.state.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApplicationsList {
    pub applications: Vec<Application>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ShallowRequest {
    pub id: String,
    pub created_at: u128,
    pub outcome: Option<RequestOutcome>,
    pub function_runs_count: usize,
    pub application_version: String,
}

impl From<RequestCtx> for ShallowRequest {
    fn from(ctx: RequestCtx) -> Self {
        Self {
            id: ctx.request_id.to_string(),
            created_at: ctx.created_at.into(),
            outcome: ctx.outcome.map(|outcome| outcome.into()),
            function_runs_count: ctx.function_runs.len(),
            application_version: ctx.application_version.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApplicationRequests {
    pub requests: Vec<ShallowRequest>,
    pub prev_cursor: Option<String>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FunctionRun {
    pub id: String,
    pub name: String,
    pub application: String,
    pub namespace: String,
    pub status: FunctionRunStatus,
    pub outcome: Option<FunctionRunOutcome>,
    pub failure_reason: Option<FunctionRunFailureReason>,
    pub application_version: String,
    pub allocations: Vec<Allocation>,
    pub created_at: u128,
}

impl FunctionRun {
    pub fn from_data_model_function_run(
        function_run: data_model::FunctionRun,
        allocations: Vec<Allocation>,
    ) -> Self {
        let failure_reason = match &function_run.outcome {
            Some(data_model::FunctionRunOutcome::Failure(reason)) => Some((*reason).into()),
            _ => None,
        };
        Self {
            id: function_run.id.to_string(),
            name: function_run.name,
            application: function_run.application,
            namespace: function_run.namespace,
            outcome: function_run.outcome.map(|outcome| outcome.into()),
            failure_reason,
            status: function_run.status.into(),
            application_version: function_run.version,
            allocations,
            created_at: function_run.creation_time_ns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RequestOutcome {
    Undefined,
    Success,
    Failure(RequestFailureReason),
}

impl From<data_model::RequestOutcome> for RequestOutcome {
    fn from(outcome: data_model::RequestOutcome) -> Self {
        match outcome {
            data_model::RequestOutcome::Unknown => RequestOutcome::Undefined,
            data_model::RequestOutcome::Success => RequestOutcome::Success,
            data_model::RequestOutcome::Failure(reason) => RequestOutcome::Failure(reason.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RequestFailureReason {
    Unknown,
    InternalError,
    FunctionError,
    RequestError,
    ConstraintUnsatisfiable,
    Cancelled,
}

impl From<data_model::RequestFailureReason> for RequestFailureReason {
    fn from(failure_reason: data_model::RequestFailureReason) -> Self {
        match failure_reason {
            data_model::RequestFailureReason::Unknown => RequestFailureReason::Unknown,
            data_model::RequestFailureReason::InternalError => RequestFailureReason::InternalError,
            data_model::RequestFailureReason::FunctionError => RequestFailureReason::FunctionError,
            data_model::RequestFailureReason::RequestError => RequestFailureReason::RequestError,
            data_model::RequestFailureReason::ConstraintUnsatisfiable => {
                RequestFailureReason::ConstraintUnsatisfiable
            }
            data_model::RequestFailureReason::Cancelled => RequestFailureReason::Cancelled,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Request {
    pub id: String,
    pub outcome: Option<RequestOutcome>,
    pub failure_reason: Option<RequestFailureReason>,
    pub application_version: String,
    pub created_at: u128,
    pub request_error: Option<RequestError>,
    pub function_runs: Vec<FunctionRun>,
}

impl Request {
    pub fn build(
        ctx: RequestCtx,
        request_error: Option<RequestError>,
        allocations: Vec<data_model::Allocation>,
    ) -> Self {
        let mut allocs_by_function_call_id: HashMap<String, Vec<Allocation>> = HashMap::new();
        for allocation in allocations {
            allocs_by_function_call_id
                .entry(allocation.function_call_id.to_string())
                .or_default()
                .push(allocation.into());
        }
        let mut function_runs = vec![];
        for function_run in ctx.function_runs.values() {
            let allocations = allocs_by_function_call_id
                .get(&function_run.id.to_string())
                .cloned()
                .unwrap_or_default();
            function_runs.push(FunctionRun::from_data_model_function_run(
                function_run.clone(),
                allocations,
            ));
        }
        let failure_reason = match &ctx.outcome {
            Some(data_model::RequestOutcome::Failure(reason)) => Some(reason.clone().into()),
            _ => None,
        };
        Self {
            id: ctx.request_id.to_string(),
            outcome: ctx.outcome.map(|outcome| outcome.into()),
            application_version: ctx.application_version.to_string(),
            failure_reason,
            created_at: ctx.created_at.into(),
            request_error,
            function_runs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Allocation {
    pub id: String,
    pub function_name: String,
    pub executor_id: String,
    pub function_executor_id: String,
    pub created_at: u128,
    pub outcome: FunctionRunOutcome,
    pub attempt_number: u32,
    pub execution_duration_ms: Option<u64>,
}

impl From<data_model::Allocation> for Allocation {
    fn from(allocation: data_model::Allocation) -> Self {
        Self {
            id: allocation.id.to_string(),
            function_name: allocation.function.to_string(),
            executor_id: allocation.target.executor_id.to_string(),
            function_executor_id: allocation.target.function_executor_id.get().to_string(),
            created_at: allocation.created_at,
            outcome: allocation.outcome.into(),
            attempt_number: allocation.attempt_number,
            execution_duration_ms: allocation.execution_duration_ms,
        }
    }
}
