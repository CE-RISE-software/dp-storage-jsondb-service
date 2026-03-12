use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::{
    config::{DatabaseBackend, DatabaseConfig},
    error::AppError,
    query::{QueryRecord, QueryRequest},
};

mod sql_mysql;
mod sql_postgres;

use self::{sql_mysql::MySqlRecordRepository, sql_postgres::PostgresRecordRepository};

#[derive(Clone, Debug)]
pub struct Record {
    pub id: String,
    pub model: String,
    pub version: String,
    pub payload: Value,
    pub created_by_sub: Option<String>,
    pub tenant_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct AccessContext {
    pub subject: Option<String>,
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ReadGrant {
    pub subject: Option<String>,
    pub tenant_id: Option<String>,
}

#[async_trait]
pub trait RecordRepository: Send + Sync {
    async fn write_record(
        &self,
        idempotency_key: &str,
        record: Record,
        ctx: AccessContext,
    ) -> Result<String, AppError>;
    async fn read_record(&self, id: &str, ctx: &AccessContext) -> Result<Option<Record>, AppError>;
    async fn query_records(
        &self,
        request: &QueryRequest,
        ctx: &AccessContext,
    ) -> Result<Vec<Record>, AppError>;
    async fn grant_read_access(&self, record_id: &str, grant: ReadGrant) -> Result<(), AppError>;
    async fn readiness(&self) -> Result<(), AppError>;
}

#[derive(Clone)]
pub enum SqlRecordRepository {
    MySql(MySqlRecordRepository),
    Postgres(PostgresRecordRepository),
}

impl SqlRecordRepository {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, AppError> {
        match config.backend {
            DatabaseBackend::MySql | DatabaseBackend::MariaDb => {
                Ok(Self::MySql(MySqlRecordRepository::connect(config).await?))
            }
            DatabaseBackend::Postgres => Ok(Self::Postgres(
                PostgresRecordRepository::connect(config).await?,
            )),
        }
    }

    pub async fn run_migrations(&self) -> Result<(), AppError> {
        match self {
            Self::MySql(repository) => repository.run_migrations().await,
            Self::Postgres(repository) => repository.run_migrations().await,
        }
    }
}

#[async_trait]
impl RecordRepository for SqlRecordRepository {
    async fn write_record(
        &self,
        idempotency_key: &str,
        record: Record,
        ctx: AccessContext,
    ) -> Result<String, AppError> {
        match self {
            Self::MySql(repository) => repository.write_record(idempotency_key, record, ctx).await,
            Self::Postgres(repository) => {
                repository.write_record(idempotency_key, record, ctx).await
            }
        }
    }

    async fn read_record(&self, id: &str, ctx: &AccessContext) -> Result<Option<Record>, AppError> {
        match self {
            Self::MySql(repository) => repository.read_record(id, ctx).await,
            Self::Postgres(repository) => repository.read_record(id, ctx).await,
        }
    }

    async fn query_records(
        &self,
        request: &QueryRequest,
        ctx: &AccessContext,
    ) -> Result<Vec<Record>, AppError> {
        match self {
            Self::MySql(repository) => repository.query_records(request, ctx).await,
            Self::Postgres(repository) => repository.query_records(request, ctx).await,
        }
    }

    async fn grant_read_access(&self, record_id: &str, grant: ReadGrant) -> Result<(), AppError> {
        match self {
            Self::MySql(repository) => repository.grant_read_access(record_id, grant).await,
            Self::Postgres(repository) => repository.grant_read_access(record_id, grant).await,
        }
    }

    async fn readiness(&self) -> Result<(), AppError> {
        match self {
            Self::MySql(repository) => repository.readiness().await,
            Self::Postgres(repository) => repository.readiness().await,
        }
    }
}

pub(crate) fn payload_hash(payload: &Value) -> String {
    let bytes = serde_json::to_vec(payload).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

pub(crate) fn scalar_as_string(value: &Value) -> Result<String, AppError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        _ => Err(AppError::BadRequest(
            "operator requires a scalar query value".to_string(),
        )),
    }
}

pub(crate) fn json_literal(value: &Value) -> Result<String, AppError> {
    serde_json::to_string(value)
        .map_err(|err| AppError::Internal(format!("failed to serialize query value: {err}")))
}

#[derive(Clone, Default)]
pub struct InMemoryRecordRepository {
    state: Arc<RwLock<InMemoryState>>,
}

#[derive(Default)]
struct InMemoryState {
    records: HashMap<String, Record>,
    idempotency: HashMap<String, DateTime<Utc>>,
    read_grants: HashMap<String, Vec<ReadGrant>>,
}

#[async_trait]
impl RecordRepository for InMemoryRecordRepository {
    async fn write_record(
        &self,
        idempotency_key: &str,
        mut record: Record,
        ctx: AccessContext,
    ) -> Result<String, AppError> {
        let mut state = self.state.write().await;
        state
            .idempotency
            .retain(|_, expires_at| *expires_at > Utc::now());
        if state.idempotency.contains_key(idempotency_key) {
            return Err(AppError::Conflict(
                "idempotency key is still active".to_string(),
            ));
        }
        record.created_by_sub = ctx.subject;
        record.tenant_id = ctx.tenant_id;
        let id = record.id.clone();
        state.records.insert(id.clone(), record);
        state.idempotency.insert(
            idempotency_key.to_string(),
            Utc::now() + Duration::seconds(120),
        );
        Ok(id)
    }

    async fn read_record(&self, id: &str, ctx: &AccessContext) -> Result<Option<Record>, AppError> {
        let state = self.state.read().await;
        let Some(record) = state.records.get(id).cloned() else {
            return Ok(None);
        };
        let grants = state.read_grants.get(id).map(Vec::as_slice).unwrap_or(&[]);
        Ok(if record_is_visible(&record, grants, ctx) {
            Some(record)
        } else {
            None
        })
    }

    async fn query_records(
        &self,
        request: &QueryRequest,
        ctx: &AccessContext,
    ) -> Result<Vec<Record>, AppError> {
        request.filter.validate()?;
        let state = self.state.read().await;
        let mut records: Vec<Record> = state.records.values().cloned().collect();
        records.retain(|record| {
            let grants = state
                .read_grants
                .get(&record.id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            record_is_visible(record, grants, ctx)
        });
        records.retain(|record| {
            let query_record = QueryRecord {
                id: &record.id,
                model: &record.model,
                version: &record.version,
                payload: &record.payload,
                created_at: record.created_at,
                updated_at: record.updated_at,
            };
            request.filter.matches(&query_record).unwrap_or(false)
        });
        let offset = request.filter.offset.unwrap_or(0) as usize;
        let limit = request.filter.limit.unwrap_or(100) as usize;
        Ok(records.into_iter().skip(offset).take(limit).collect())
    }

    async fn grant_read_access(&self, record_id: &str, grant: ReadGrant) -> Result<(), AppError> {
        if grant.subject.is_none() && grant.tenant_id.is_none() {
            return Err(AppError::BadRequest(
                "read grant must target a subject or tenant".to_string(),
            ));
        }
        let mut state = self.state.write().await;
        if !state.records.contains_key(record_id) {
            return Err(AppError::NotFound(format!(
                "record `{record_id}` not found"
            )));
        }
        let grants = state.read_grants.entry(record_id.to_string()).or_default();
        grants.push(grant);
        let Some(record) = state.records.get_mut(record_id) else {
            return Err(AppError::NotFound(format!(
                "record `{record_id}` not found"
            )));
        };
        record.updated_at = Utc::now();
        Ok(())
    }

    async fn readiness(&self) -> Result<(), AppError> {
        Ok(())
    }
}

pub(crate) fn record_is_visible(
    record: &Record,
    grants: &[ReadGrant],
    ctx: &AccessContext,
) -> bool {
    let owner_subject_ok = match &ctx.subject {
        Some(subject) => record.created_by_sub.as_ref() == Some(subject),
        None => false,
    };
    let owner_tenant_ok = match &ctx.tenant_id {
        Some(tenant_id) => record.tenant_id.as_ref() == Some(tenant_id),
        None => false,
    };
    if owner_subject_ok && owner_tenant_ok {
        return true;
    }

    grants.iter().any(|grant| {
        let subject_ok = match (&grant.subject, &ctx.subject) {
            (Some(grant_subject), Some(subject)) => grant_subject == subject,
            _ => false,
        };
        let tenant_ok = match (&grant.tenant_id, &ctx.tenant_id) {
            (Some(grant_tenant), Some(tenant_id)) => grant_tenant == tenant_id,
            _ => false,
        };
        subject_ok || tenant_ok
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(id: &str, subject: &str, tenant_id: &str) -> Record {
        Record {
            id: id.to_string(),
            model: "passport".to_string(),
            version: "1.0.0".to_string(),
            payload: serde_json::json!({"record_scope":"product"}),
            created_by_sub: Some(subject.to_string()),
            tenant_id: Some(tenant_id.to_string()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn in_memory_repository_filters_records_by_owner_context() {
        let repository = InMemoryRecordRepository::default();
        repository
            .write_record(
                "idem-1",
                sample_record("rec-1", "owner-a", "tenant-a"),
                AccessContext {
                    subject: Some("owner-a".to_string()),
                    tenant_id: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("seed record");

        let visible = repository
            .read_record(
                "rec-1",
                &AccessContext {
                    subject: Some("owner-a".to_string()),
                    tenant_id: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("visible read");
        assert!(visible.is_some());

        let hidden = repository
            .read_record(
                "rec-1",
                &AccessContext {
                    subject: Some("owner-b".to_string()),
                    tenant_id: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("hidden read");
        assert!(hidden.is_none());

        repository
            .grant_read_access(
                "rec-1",
                ReadGrant {
                    subject: Some("owner-b".to_string()),
                    tenant_id: None,
                },
            )
            .await
            .expect("grant read access");

        let shared = repository
            .read_record(
                "rec-1",
                &AccessContext {
                    subject: Some("owner-b".to_string()),
                    tenant_id: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("shared read");
        assert!(shared.is_some());
    }
}
