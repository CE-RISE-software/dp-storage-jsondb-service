use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::HeaderMap,
    routing::{get, post},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;

use crate::{
    auth::AuthService,
    error::AppError,
    openapi::ApiDoc,
    query::QueryRequest,
    repository::{Record, RecordRepository, WriteContext},
};

#[derive(Clone)]
pub struct AppState {
    pub auth: AuthService,
    pub repository: Arc<dyn RecordRepository>,
}

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct ApiRecord {
    pub id: String,
    pub model: String,
    pub version: String,
    pub payload: Value,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WriteResponse {
    pub id: String,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct QueryResponse {
    pub records: Vec<ApiRecord>,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/openapi.json", get(openapi))
        .route("/records", post(write_record))
        .route("/records/query", post(query_records))
        .route("/records/{id}", get(read_record))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

#[utoipa::path(
    post,
    path = "/records",
    request_body = ApiRecord,
    params(
        ("Idempotency-Key" = String, Header, description = "Idempotency key")
    ),
    responses(
        (status = 200, body = WriteResponse),
        (status = 400, body = ErrorResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 409, body = ErrorResponse),
        (status = 503, body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn write_record(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(record): Json<ApiRecord>,
) -> Result<Json<WriteResponse>, AppError> {
    let auth = state.auth.authorize(&headers, "records:write").await?;
    let idempotency_key = header_value(&headers, "Idempotency-Key")?;
    if idempotency_key.trim().is_empty() {
        return Err(AppError::BadRequest(
            "Idempotency-Key header cannot be empty".to_string(),
        ));
    }
    let record = Record {
        id: record.id,
        model: record.model,
        version: record.version,
        payload: record.payload,
        created_by_sub: Some(auth.subject.clone()),
        tenant_id: auth.tenant_id.clone(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let id = state
        .repository
        .write_record(
            &idempotency_key,
            record,
            WriteContext {
                subject: Some(auth.subject),
                tenant_id: auth.tenant_id,
            },
        )
        .await?;
    Ok(Json(WriteResponse { id }))
}

#[utoipa::path(
    get,
    path = "/records/{id}",
    params(
        ("id" = String, Path, description = "Record ID")
    ),
    responses(
        (status = 200, body = ApiRecord),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse),
        (status = 404, body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn read_record(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiRecord>, AppError> {
    let _auth = state.auth.authorize(&headers, "records:read").await?;
    let record = state
        .repository
        .read_record(&id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("record `{id}` not found")))?;
    Ok(Json(to_api_record(record)))
}

#[utoipa::path(
    post,
    path = "/records/query",
    request_body = QueryRequest,
    responses(
        (status = 200, body = QueryResponse),
        (status = 400, body = ErrorResponse),
        (status = 401, body = ErrorResponse),
        (status = 403, body = ErrorResponse)
    ),
    security(
        ("bearer_auth" = [])
    )
)]
pub async fn query_records(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    let _auth = state.auth.authorize(&headers, "records:read").await?;
    let records = state.repository.query_records(&request).await?;
    Ok(Json(QueryResponse {
        records: records.into_iter().map(to_api_record).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/health",
    responses((status = 200, body = HealthResponse))
)]
pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

#[utoipa::path(
    get,
    path = "/ready",
    responses(
        (status = 200, body = HealthResponse),
        (status = 503, body = ErrorResponse)
    )
)]
pub async fn ready(State(state): State<AppState>) -> Result<Json<HealthResponse>, AppError> {
    state.repository.readiness().await?;
    Ok(Json(HealthResponse { status: "ready" }))
}

pub async fn openapi() -> Json<serde_json::Value> {
    Json(serde_json::to_value(ApiDoc::openapi()).expect("openapi serialization"))
}

fn to_api_record(record: Record) -> ApiRecord {
    ApiRecord {
        id: record.id,
        model: record.model,
        version: record.version,
        payload: record.payload,
    }
}

fn header_value(headers: &HeaderMap, name: &'static str) -> Result<String, AppError> {
    headers
        .get(name)
        .ok_or_else(|| AppError::BadRequest(format!("missing required header `{name}`")))?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|_| AppError::BadRequest(format!("invalid header `{name}`")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{self, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    use crate::{
        auth::AuthService,
        config::{AuthConfig, AuthMode},
        query::{QueryOperator, QueryRequest, RecordQueryCondition, RecordQueryFilter},
        repository::InMemoryRecordRepository,
    };

    use super::*;

    fn disabled_auth() -> AuthService {
        AuthService::new(&AuthConfig {
            mode: AuthMode::Disabled,
            jwks_url: None,
            issuer: None,
            audience: None,
        })
        .expect("disabled auth service")
    }

    fn jwt_auth() -> AuthService {
        AuthService::new(&AuthConfig {
            mode: AuthMode::JwtJwks,
            jwks_url: Some("https://example.org/jwks".to_string()),
            issuer: Some("https://issuer.example.org".to_string()),
            audience: Some("ce-rise".to_string()),
        })
        .expect("jwt auth service")
    }

    fn test_app(auth: AuthService) -> Router {
        router(AppState {
            auth,
            repository: Arc::new(InMemoryRecordRepository::default()),
        })
    }

    #[tokio::test]
    async fn post_records_returns_success() {
        let app = test_app(disabled_auth());
        let response = app
            .oneshot(
                Request::post("/records")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "idem-1")
                    .body(Body::from(
                        serde_json::json!({
                            "id": "rec-1",
                            "model": "passport",
                            "version": "1.0.0",
                            "payload": {"record_scope": "product"}
                        })
                        .to_string(),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
        assert_eq!(value["id"], "rec-1");
    }

    #[tokio::test]
    async fn post_records_rejects_active_idempotency_key() {
        let app = test_app(disabled_auth());
        let request_body = serde_json::json!({
            "id": "rec-1",
            "model": "passport",
            "version": "1.0.0",
            "payload": {"record_scope": "product"}
        })
        .to_string();

        let first = app
            .clone()
            .oneshot(
                Request::post("/records")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "idem-1")
                    .body(Body::from(request_body.clone()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(
                Request::post("/records")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "idem-1")
                    .body(Body::from(request_body))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(second.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn get_records_returns_not_found() {
        let app = test_app(disabled_auth());
        let response = app
            .oneshot(
                Request::get("/records/missing")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_records_query_returns_records() {
        let app = test_app(disabled_auth());
        let seed_body = serde_json::json!({
            "id": "rec-1",
            "model": "passport",
            "version": "1.0.0",
            "payload": {"record_scope": "product"}
        })
        .to_string();
        let _ = app
            .clone()
            .oneshot(
                Request::post("/records")
                    .header("content-type", "application/json")
                    .header("idempotency-key", "idem-1")
                    .body(Body::from(seed_body))
                    .expect("request"),
            )
            .await
            .expect("seed response");

        let query = QueryRequest {
            filter: RecordQueryFilter {
                where_conditions: vec![RecordQueryCondition {
                    field: "payload.record_scope".to_string(),
                    op: QueryOperator::Eq,
                    value: serde_json::json!("product"),
                }],
                sort: Vec::new(),
                limit: Some(10),
                offset: Some(0),
            },
        };

        let response = app
            .oneshot(
                Request::post("/records/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&query).expect("query json")))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
        assert_eq!(value["records"].as_array().map(Vec::len), Some(1));
    }

    #[tokio::test]
    async fn protected_routes_require_token_in_jwt_mode() {
        let app = test_app(jwt_auth());
        let response = app
            .oneshot(
                Request::get("/records/rec-1")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
