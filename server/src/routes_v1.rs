use std::collections::HashMap;

use anyhow::Result;
use axum::{
    extract::{Path, Query, RawPathParams, Request, State},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{delete, get, post},
    Json,
    Router,
};
use base64::prelude::*;
use compute_graphs::{delete_compute_graph, get_compute_graph, list_compute_graphs};
use download::download_invocation_error;
use invoke::invoke_with_object_v1;
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    http_objects::{
        Allocation,
        CacheKey,
        ComputeFn,
        ComputeGraph,
        ComputeGraphsList,
        CreateNamespace,
        CursorDirection,
        ExecutorMetadata,
        ExecutorsAllocationsResponse,
        GraphInvocations,
        GraphVersion,
        IndexifyAPIError,
        Invocation,
        ListParams,
        Namespace,
        NamespaceList,
        RuntimeInformation,
        StateChangesResponse,
        Task,
        TaskOutcome,
        Tasks,
        UnallocatedTasks,
    },
    http_objects_v1::{self, GraphRequests},
    routes::{
        compute_graphs::{self, create_or_update_compute_graph_v1},
        download::{self, v1_download_fn_output_payload, v1_download_fn_output_payload_simple},
        invoke::{self, progress_stream},
        routes_state::RouteState,
    },
    state_store::{
        self,
        requests::{
            DeleteInvocationRequest,
            NamespaceRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
    },
};

#[derive(OpenApi)]
#[openapi(
        paths(
            invoke::invoke_with_object_v1,
            graph_requests,
            find_invocation,
            compute_graphs::create_or_update_compute_graph_v1,
            compute_graphs::list_compute_graphs,
            compute_graphs::get_compute_graph,
            compute_graphs::delete_compute_graph,
            list_tasks,
            delete_invocation,
            download::v1_download_fn_output_payload,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
                ComputeGraph,
		        CacheKey,
                ComputeFn,
                ListParams,
                ComputeGraphCreateType,
                ComputeGraphsList,
                ExecutorMetadata,
                RuntimeInformation,
                Task,
                TaskOutcome,
                Tasks,
                GraphInvocations,
                GraphVersion,
                Allocation,
                ExecutorsAllocationsResponse,
                UnallocatedTasks,
                StateChangesResponse,
            )
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]
pub struct ApiDoc;

pub fn configure_v1_routes(route_state: RouteState) -> Router {
    Router::new()
        .merge(
            SwaggerUi::new("/docs/public/swagger")
                .url("/docs/public/openapi.json", ApiDoc::openapi()),
        )
        .nest(
            "/v1/namespaces/{namespace}",
            v1_namespace_routes(route_state.clone()),
        )
}

/// Namespace router with namespace specific layers.
fn v1_namespace_routes(route_state: RouteState) -> Router {
    Router::new()
        .route(
            "/compute-graphs",
            post(create_or_update_compute_graph_v1).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs",
            get(list_compute_graphs).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}",
            delete(delete_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}",
            get(get_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}",
            post(invoke_with_object_v1).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests",
            get(graph_requests).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}",
            get(find_invocation).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}/progress",
            get(progress_stream).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}",
            delete(delete_invocation).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}/tasks",
            get(list_tasks).with_state(route_state.clone()),
        )
        // FIXME: remove this route once we migrate tensorlake sdk to this
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}/fn/{fn_name}/outputs/{id}/index/{index}",
            get(v1_download_fn_output_payload).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}/output/{fn_name}/id/{id}/index/{index}",
            get(v1_download_fn_output_payload).with_state(route_state.clone()),
        )
        .route(
            "/compute-graphs/{compute_graph}/requests/{request_id}/output/{fn_name}",
            get(v1_download_fn_output_payload_simple).with_state(route_state.clone()),
        )
        .layer(middleware::from_fn(move |rpp, r, n| {
            namespace_middleware(route_state.clone(), rpp, r, n)
        }))
}

/// Middleware to check if the namespace exists.
async fn namespace_middleware(
    route_state: RouteState,
    params: RawPathParams,
    request: Request,
    next: Next,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    // get the namespace path variable from the path
    let namespace_param = params.iter().find(|(key, _)| *key == "namespace");

    // if the namespace path variable is found, check if the namespace exists
    if let Some((_, namespace)) = namespace_param {
        let reader = route_state.indexify_state.reader();
        let ns = reader
            .get_namespace(namespace)
            .map_err(IndexifyAPIError::internal_error)?;

        if ns.is_none() {
            route_state
                .indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                        name: namespace.to_string(),
                        blob_storage_bucket: None,
                        blob_storage_region: None,
                    }),
                })
                .await
                .map_err(IndexifyAPIError::internal_error)?;

            info!("namespace created: {:?}", namespace);
        }
    }

    Ok(next.run(request).await)
}

#[allow(dead_code)]
#[derive(ToSchema)]
struct ComputeGraphCreateType {
    compute_graph: ComputeGraph,
    #[schema(format = "binary")]
    code: String,
}

/// List requests for a workflow
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute-graphs/{compute_graph}/requests",
    tag = "ingestion",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "List Graph Invocations", body = GraphInvocations),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn graph_requests(
    Path((namespace, compute_graph)): Path<(String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<GraphRequests>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let direction = match params.direction {
        Some(CursorDirection::Forward) => Some(state_store::scanner::CursorDirection::Forward),
        Some(CursorDirection::Backward) => Some(state_store::scanner::CursorDirection::Backward),
        None => None,
    };
    let (invocation_ctxs, prev_cursor, next_cursor) = state
        .indexify_state
        .reader()
        .list_invocations(
            &namespace,
            &compute_graph,
            cursor.as_deref(),
            params.limit.unwrap_or(100),
            direction,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let mut requests = vec![];
    for invocation_ctx in invocation_ctxs {
        let shallow_request = invocation_ctx.clone().into();
        requests.push(shallow_request);
    }
    let prev_cursor = prev_cursor.map(|c| BASE64_STANDARD.encode(c));
    let next_cursor = next_cursor.map(|c| BASE64_STANDARD.encode(c));

    Ok(Json(GraphRequests {
        requests,
        prev_cursor,
        next_cursor,
    }))
}

/// List tasks for a request
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute-graphs/{compute_graph}/requests/{request_id}/tasks",
    tag = "operations",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "list tasks for a given request id", body = Tasks),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
#[axum::debug_handler]
async fn list_tasks(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::Tasks>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let (tasks, cursor) = state
        .indexify_state
        .reader()
        .list_tasks_by_compute_graph(
            &namespace,
            &compute_graph,
            &invocation_id,
            cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let allocations = state
        .indexify_state
        .reader()
        .get_allocations_by_invocation(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?;
    let mut allocations_by_task_id: HashMap<String, Vec<http_objects_v1::Allocation>> =
        HashMap::new();
    for allocation in allocations {
        allocations_by_task_id
            .entry(allocation.task_id.to_string())
            .or_default()
            .push(allocation.into());
    }
    let mut http_tasks = vec![];
    for task in tasks {
        let allocations = allocations_by_task_id
            .get(task.id.get())
            .cloned()
            .clone()
            .unwrap_or_default();
        http_tasks.push(http_objects_v1::Task::from_data_model_task(
            task,
            allocations,
        ));
    }
    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));
    Ok(Json(http_objects_v1::Tasks {
        tasks: http_tasks,
        cursor,
    }))
}

/// Get request status by id
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/compute-graphs/{compute_graph}/requests/{request_id}",
    tag = "retrieve",
    responses(
        (status = 200, description = "Details about a given invocation", body = Invocation),
        (status = NOT_FOUND, description = "Invocation not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn find_invocation(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::Request>, IndexifyAPIError> {
    let invocation_ctx = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("invocation not found"))?;

    let (outputs, _cursor) = state
        .indexify_state
        .reader()
        .list_outputs_by_compute_graph(&namespace, &compute_graph, &invocation_id, None, None)
        .map_err(IndexifyAPIError::internal_error)?;
    let mut http_outputs = vec![];
    for output in outputs {
        http_outputs.push(http_objects_v1::FnOutput {
            id: output.id.clone(),
            num_outputs: output.payloads.len() as u64,
            compute_fn: output.compute_fn_name.clone(),
            created_at: output.created_at.clone().into(),
        });
    }

    let invocation_error = download_invocation_error(
        invocation_ctx.invocation_error.clone(),
        &state.blob_storage.get_blob_store(&namespace),
    )
    .await?;

    let request = http_objects_v1::Request::build(invocation_ctx, http_outputs, invocation_error);

    Ok(Json(request))
}

/// Delete a specific request
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/compute-graphs/{compute_graph}/requests/{request_id}",
    tag = "operations",
    responses(
        (status = 200, description = "request has been deleted"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
#[axum::debug_handler]
async fn delete_invocation(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneInvocation(DeleteInvocationRequest {
        namespace,
        compute_graph,
        invocation_id,
    });
    let req = StateMachineUpdateRequest { payload: request };

    state
        .indexify_state
        .write(req)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}
