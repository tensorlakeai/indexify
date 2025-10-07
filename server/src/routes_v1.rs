use anyhow::Result;
use applications::{applications, delete_application, get_application};
use axum::{
    extract::{Path, Query, RawPathParams, Request, State},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{delete, get, post},
    Json,
    Router,
};
use base64::prelude::*;
use download::download_request_error;
use invoke::invoke_application_with_object_v1;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    http_objects::{
        Allocation,
        CacheKey,
        CreateNamespace,
        CursorDirection,
        ExecutorMetadata,
        ExecutorsAllocationsResponse,
        Function,
        FunctionRunOutcome,
        IndexifyAPIError,
        ListParams,
        Namespace,
        NamespaceList,
        StateChangesResponse,
        UnallocatedFunctionRuns,
    },
    http_objects_v1::{self, Application, ApplicationRequests, ApplicationsList},
    routes::{
        applications::{self, create_or_update_application},
        download::{self, v1_download_fn_output_payload, v1_download_fn_output_payload_simple},
        invoke::{self, progress_stream},
        routes_state::RouteState,
    },
    state_store::{
        self,
        requests::{
            DeleteRequestRequest,
            NamespaceRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
    },
};

#[derive(OpenApi)]
#[openapi(
        paths(
            invoke::invoke_application_with_object_v1,
            list_requests,
            find_request,
            applications::create_or_update_application,
            applications::applications,
            applications::get_application,
            applications::delete_application,
            delete_request,
            download::v1_download_fn_output_payload,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
                Application,
		        CacheKey,
                Function,
                ListParams,
                ApplicationsList,
                ExecutorMetadata,
                FunctionRunOutcome,
                Allocation,
                ExecutorsAllocationsResponse,
                UnallocatedFunctionRuns,
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
            "/applications",
            post(create_or_update_application).with_state(route_state.clone()),
        )
        .route(
            "/applications",
            get(applications).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}",
            delete(delete_application).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}",
            get(get_application).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}",
            post(invoke_application_with_object_v1).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests",
            get(list_requests).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests/{request_id}",
            get(find_request).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests/{request_id}/progress",
            get(progress_stream).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests/{request_id}",
            delete(delete_request).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests/{request_id}/output/{fn_call_id}",
            get(v1_download_fn_output_payload).with_state(route_state.clone()),
        )
        .route(
            "/applications/{application}/requests/{request_id}/output",
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

/// List requests for a workflow
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/requests",
    tag = "ingestion",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "List Application requests", body = http_objects_v1::ApplicationRequests),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_requests(
    Path((namespace, application)): Path<(String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<ApplicationRequests>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let direction = match params.direction {
        Some(CursorDirection::Forward) => Some(state_store::scanner::CursorDirection::Forward),
        Some(CursorDirection::Backward) => Some(state_store::scanner::CursorDirection::Backward),
        None => None,
    };
    let (request_ctxs, prev_cursor, next_cursor) = state
        .indexify_state
        .reader()
        .list_requests(
            &namespace,
            &application,
            cursor.as_deref(),
            params.limit.unwrap_or(100),
            direction,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let mut requests = vec![];
    for request_ctx in request_ctxs {
        let shallow_request = request_ctx.clone().into();
        requests.push(shallow_request);
    }
    let prev_cursor = prev_cursor.map(|c| BASE64_STANDARD.encode(c));
    let next_cursor = next_cursor.map(|c| BASE64_STANDARD.encode(c));

    Ok(Json(ApplicationRequests {
        requests,
        prev_cursor,
        next_cursor,
    }))
}

/// Get request status by id
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/requests/{request_id}",
    tag = "retrieve",
    responses(
        (status = 200, description = "details about a given request", body = http_objects_v1::Request),
        (status = NOT_FOUND, description = "request not found"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
async fn find_request(
    Path((namespace, application, request_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::Request>, IndexifyAPIError> {
    let request_ctx = state
        .indexify_state
        .reader()
        .request_ctx(&namespace, &application, &request_id)
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("request not found"))?;

    let function_run = request_ctx
        .function_runs
        .get(&request_id.as_str().into())
        .ok_or(IndexifyAPIError::not_found("function run not found"))?
        .clone();

    let allocations = state
        .indexify_state
        .reader()
        .get_allocations_by_request_id(&namespace, &application, &request_id)
        .map_err(IndexifyAPIError::internal_error)?;

    let output = function_run.output.clone().map(|output| output.into());
    let request_error = download_request_error(
        request_ctx.request_error.clone(),
        &state.blob_storage.get_blob_store(&namespace),
    )
    .await?;

    let request = http_objects_v1::Request::build(request_ctx, output, request_error, allocations);

    Ok(Json(request))
}

/// Delete a specific request
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/applications/{application}/requests/{request_id}",
    tag = "operations",
    responses(
        (status = 200, description = "request has been deleted"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error"),
        (status = NOT_FOUND, description = "request not found")
    ),
)]
#[axum::debug_handler]
async fn delete_request(
    Path((namespace, application, request_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneRequest(DeleteRequestRequest {
        namespace,
        application,
        request_id,
    });
    let req = StateMachineUpdateRequest { payload: request };

    state
        .indexify_state
        .write(req)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}
