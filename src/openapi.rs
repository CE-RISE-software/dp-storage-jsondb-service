use utoipa::OpenApi;

use crate::{
    app::{ApiRecord, ErrorResponse, HealthResponse, QueryResponse, WriteResponse},
    query::{QueryRequest, RecordQueryCondition, RecordQueryFilter, RecordQuerySort},
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::app::write_record,
        crate::app::read_record,
        crate::app::query_records,
        crate::app::health,
        crate::app::ready
    ),
    components(
        schemas(
            ApiRecord,
            WriteResponse,
            QueryResponse,
            QueryRequest,
            RecordQueryFilter,
            RecordQueryCondition,
            RecordQuerySort,
            HealthResponse,
            ErrorResponse
        )
    ),
    tags(
        (name = "storage", description = "CE-RISE JSON document storage backend")
    ),
    security(
        ("bearer_auth" = [])
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::Http::new(
                        utoipa::openapi::security::HttpAuthScheme::Bearer,
                    ),
                ),
            );
        }
    }
}
