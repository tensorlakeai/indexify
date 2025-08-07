use anyhow::anyhow;
use axum::{
    extract::{Multipart, Path, Query, State},
    Json,
};
use base64::{prelude::BASE64_STANDARD, Engine};
use futures::StreamExt;
use tracing::info;
use utoipa::ToSchema;

use crate::{
    blob_store::PutResult,
    data_model::{ComputeGraph, ComputeGraphError},
    http_objects::{self, IndexifyAPIError, ListParams},
    http_objects_v1,
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateOrUpdateComputeGraphRequest,
        DeleteComputeGraphRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
};

#[allow(dead_code)]
#[derive(ToSchema)]
struct ComputeGraphCreateType {
    compute_graph: http_objects::ComputeGraph,
    #[schema(format = "binary")]
    code: String,
}

async fn validate_placement_constraints_against_executor_label_sets(
    compute_graph: &ComputeGraph,
    state: &RouteState,
) -> Result<(), IndexifyAPIError> {
    let lock_guard = state.indexify_state.in_memory_state.read().await;

    let executor_catalog = &lock_guard.executor_catalog;

    if executor_catalog.allows_any_labels() {
        return Ok(());
    }

    for (function_name, node) in &compute_graph.nodes {
        let can_be_satisfied = executor_catalog
            .label_sets()
            .iter()
            .any(|label_set| node.placement_constraints.matches(label_set));

        if !can_be_satisfied {
            let constraints_str = node
                .placement_constraints
                .0
                .iter()
                .map(|expr| format!("{}", expr))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(IndexifyAPIError::bad_request(&format!(
                "Function '{}' has unsatisfiable placement constraints [{}].",
                function_name, constraints_str
            )));
        }
    }

    Ok(())
}

/// Create or update a workflow
#[utoipa::path(
    post,
    path = "v1/namespaces/{namespace}/compute-graphs",
    tag = "operations",
    request_body(content_type = "multipart/form-data", content = inline(ComputeGraphCreateType)),
    responses(
        (status = 200, description = "create or update a compute graph"),
        (status = INTERNAL_SERVER_ERROR, description = "unable to create compute graph")
    ),
)]
pub async fn create_or_update_compute_graph_v1(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    mut compute_graph_code: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut compute_graph_definition: Option<http_objects_v1::ComputeGraph> = Option::None;
    let mut put_result: Option<PutResult> = None;
    let mut upgrade_tasks_to_current_version: Option<bool> = None;
    while let Some(field) = compute_graph_code
        .next_field()
        .await
        .map_err(|err| IndexifyAPIError::internal_error(anyhow!(err)))?
    {
        let name = field.name();
        if let Some(name) = name {
            if name == "code" {
                let stream = field.map(|res| res.map_err(|err| anyhow!(err)));
                let file_name = format!("{}_{}", namespace, nanoid::nanoid!());
                let result = state
                    .blob_storage
                    .get_blob_store(&namespace)
                    .put(&file_name, stream)
                    .await
                    .map_err(IndexifyAPIError::internal_error)?;
                put_result = Some(result);
            } else if name == "compute_graph" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let mut json_value: serde_json::Value = serde_json::from_str(&text)?;
                json_value["namespace"] = serde_json::Value::String(namespace.clone());
                compute_graph_definition = Some(serde_json::from_value(json_value)?);
            } else if name == "upgrade_tasks_to_latest_version" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                upgrade_tasks_to_current_version = Some(serde_json::from_str::<bool>(&text)?);
            } else if name == "code_content_type" {
                let code_content_type = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                if code_content_type != "application/zip" {
                    return Err(IndexifyAPIError::bad_request(
                        "Code content type must be application/zip",
                    ));
                }
            }
        }
    }

    let compute_graph_definition = compute_graph_definition.ok_or(
        IndexifyAPIError::bad_request("Compute graph definition is required"),
    )?;

    let put_result = put_result.ok_or(IndexifyAPIError::bad_request("Code is required"))?;

    let compute_graph = compute_graph_definition.into_data_model(
        &put_result.url,
        &put_result.sha256_hash,
        put_result.size_bytes,
        &state.config.executor,
    )?;
    let name = compute_graph.name.clone();

    validate_placement_constraints_against_executor_label_sets(&compute_graph, &state).await?;

    info!(
        "creating compute graph {}, upgrade existing tasks and invocations: {}",
        name,
        upgrade_tasks_to_current_version.unwrap_or(false)
    );
    let request = RequestPayload::CreateOrUpdateComputeGraph(CreateOrUpdateComputeGraphRequest {
        namespace,
        compute_graph,
        upgrade_tasks_to_current_version: upgrade_tasks_to_current_version.unwrap_or(false),
    });
    let result = state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            processed_state_changes: vec![],
        })
        .await;
    if let Err(err) = result {
        return match err.root_cause().downcast_ref::<ComputeGraphError>() {
            Some(ComputeGraphError::VersionExists) => Err(IndexifyAPIError::bad_request(
                "This graph version already exists, please update the graph version",
            )),
            _ => Err(IndexifyAPIError::internal_error(err)),
        };
    }

    info!("compute graph created: {}", name);
    Ok(())
}

/// Delete compute graph
#[utoipa::path(
    delete,
    path = "v1/namespaces/{namespace}/compute-graphs/{compute_graph}",
    tag = "operations",
    responses(
        (status = 200, description = "compute graph deleted successfully"),
        (status = BAD_REQUEST, description = "unable to delete compute graph")
    ),
)]
pub async fn delete_compute_graph(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneComputeGraph(DeleteComputeGraphRequest {
        namespace,
        name: compute_graph.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            processed_state_changes: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    info!("compute graph deleted: {}", compute_graph);
    Ok(())
}

/// List compute graphs
#[utoipa::path(
    get,
    path = "v1/namespaces/{namespace}/compute-graphs",
    tag = "operations",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "lists compute graphs", body = http_objects_v1::ComputeGraphsList),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn list_compute_graphs(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::ComputeGraphsList>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let (compute_graphs, cursor) = state
        .indexify_state
        .reader()
        .list_compute_graphs(&namespace, cursor.as_deref(), params.limit)
        .map_err(IndexifyAPIError::internal_error)?;
    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));
    Ok(Json(http_objects_v1::ComputeGraphsList {
        compute_graphs: compute_graphs.into_iter().map(|c| c.into()).collect(),
        cursor,
    }))
}

/// Get a compute graph definition
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute-graphs/{compute_graph}",
    tag = "operations",
    responses(
        (status = 200, description = "compute graph definition", body = http_objects_v1::ComputeGraph),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn get_compute_graph(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::ComputeGraph>, IndexifyAPIError> {
    let compute_graph = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &name)
        .map_err(IndexifyAPIError::internal_error)?;
    if let Some(compute_graph) = compute_graph {
        return Ok(Json(compute_graph.into()));
    }
    Err(IndexifyAPIError::not_found("Compute Graph not found"))
}
