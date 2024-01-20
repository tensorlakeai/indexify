use axum::Router;
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use super::{executors::*, extractors::*, repositories::*};
use crate::api::*;

#[derive(OpenApi)]
#[openapi(
        paths(
            create_repository,
            list_repositories,
            get_repository,
            add_texts,
            list_indexes,
            index_search,
            list_extractors,
            bind_extractor,
            attribute_lookup,
            list_executors
        ),
        components(
            schemas(
                CreateRepository,
                CreateRepositoryResponse,
                IndexDistance,
                TextAddRequest,
                TextAdditionResponse,
                Text,
                IndexSearchResponse,
                DocumentFragment,
                ListIndexesResponse,
                ExtractorOutputSchema,
                Index,
                SearchRequest,
                ListRepositoriesResponse,
                ListExtractorsResponse,
                ExtractorDescription,
                DataRepository,
                ExtractorBinding,
                ExtractorBindRequest,
                ExtractorBindResponse,
                Executor,
                AttributeLookupResponse,
                ExtractedAttributes,
                ListExecutorsResponse
            )
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]
struct ApiDoc;

pub(super) fn get_router() -> Router {
    Router::new()
        .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(Redoc::with_url("/redoc", ApiDoc::openapi()))
        .merge(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"))
}
