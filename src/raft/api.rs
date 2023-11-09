use axum::extract::Json;
use reqwest::StatusCode;

use crate::api::IndexifyAPIError;
use crate::internal_api::CreateWork;
use crate::internal_api::CreateWorkResponse;
use crate::internal_api::EmbedQueryRequest;
use crate::internal_api::EmbedQueryResponse;
use crate::internal_api::ListExecutors;
use crate::internal_api::SyncExecutor;
use crate::internal_api::SyncWorkerResponse;
use crate::raft::memstore::Request;

use super::coordinator_config::CoordinatorRaftApp as App;

 // TODO: maybe combine with non-raft API so there aren't two separate handlers
 #[axum_macros::debug_handler]
 pub(super) async fn sync_executor(
	app: axum::extract::Extension<App>,
	Json(executor): Json<SyncExecutor>,
 ) -> Result<Json<SyncWorkerResponse>, IndexifyAPIError> {
	let response = match app.raft.client_write(executor.into()).await {
		Ok(res) => res.data.into(),
		Err(e) => {
			return Err(IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("Error writing to Raft: {}", e)))
		}
	};
	Ok(Json(response))
}

#[axum_macros::debug_handler]
pub(super) async fn create_work(
	app: axum::extract::Extension<App>,
    Json(create_work): Json<CreateWork>,
) -> Result<Json<CreateWorkResponse>, IndexifyAPIError> {
	let response = match app.raft.client_write(create_work.into()).await {
		Ok(res) => res.data.into(),
		Err(e) => {
			return Err(IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("Error writing to Raft: {}", e)))
		}
	};
	Ok(Json(response))
}

#[axum_macros::debug_handler]
pub(super) async fn embed_query(
	app: axum::extract::Extension<App>,
    Json(query): Json<EmbedQueryRequest>,
) -> Result<Json<EmbedQueryResponse>, IndexifyAPIError> {
	let response = match app.raft.client_write(query.into()).await {
		Ok(res) => res.data.into(),
		Err(e) => {
			return Err(IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("Error writing to Raft: {}", e)))
		}
	};
	Ok(Json(response))
}

#[axum_macros::debug_handler]
pub(super) async fn list_executors(
	app: axum::extract::Extension<App>,
) -> Result<Json<ListExecutors>, IndexifyAPIError> {
	let response = match app.raft.client_write(Request::ListExecutors).await {
		Ok(res) => res.data.into(),
		Err(e) => {
			return Err(IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("Error reading from Raft: {}", e)))
		}
	};
	Ok(Json(response))
}