use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{MySqlPool, Row, mysql::MySqlPoolOptions, types::Json};
use tokio::sync::RwLock;

use crate::{
    config::DatabaseConfig,
    error::AppError,
    query::{QueryRecord, QueryRequest},
};

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
pub struct WriteContext {
    pub subject: Option<String>,
    pub tenant_id: Option<String>,
}

#[async_trait]
pub trait RecordRepository: Send + Sync {
    async fn write_record(
        &self,
        idempotency_key: &str,
        record: Record,
        ctx: WriteContext,
    ) -> Result<String, AppError>;
    async fn read_record(&self, id: &str) -> Result<Option<Record>, AppError>;
    async fn query_records(&self, request: &QueryRequest) -> Result<Vec<Record>, AppError>;
    async fn readiness(&self) -> Result<(), AppError>;
}

#[derive(Clone)]
pub struct SqlRecordRepository {
    pool: MySqlPool,
    idempotency_ttl_seconds: i64,
}

impl SqlRecordRepository {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, AppError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(config.pool_size)
            .acquire_timeout(std::time::Duration::from_millis(config.timeout_ms))
            .connect(&config.url())
            .await?;
        Ok(Self {
            pool,
            idempotency_ttl_seconds: 120,
        })
    }
}

#[async_trait]
impl RecordRepository for SqlRecordRepository {
    async fn write_record(
        &self,
        idempotency_key: &str,
        record: Record,
        ctx: WriteContext,
    ) -> Result<String, AppError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM idempotency_keys WHERE expires_at <= UTC_TIMESTAMP()")
            .execute(&mut *tx)
            .await?;

        let expires_at = Utc::now() + Duration::seconds(self.idempotency_ttl_seconds);
        let payload_hash = payload_hash(&record.payload);
        let insert_key = sqlx::query(
            r#"
            INSERT INTO idempotency_keys (idempotency_key, payload_hash, record_id, expires_at)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(idempotency_key)
        .bind(payload_hash)
        .bind(&record.id)
        .bind(expires_at.naive_utc())
        .execute(&mut *tx)
        .await;

        if let Err(err) = insert_key {
            if matches!(err, sqlx::Error::Database(_)) {
                return Err(AppError::Conflict(
                    "idempotency key is still active".to_string(),
                ));
            }
            return Err(AppError::from(err));
        }

        let payload = Json(record.payload);
        sqlx::query(
            r#"
            INSERT INTO records (
                id, model, version, payload_json, created_by_sub, tenant_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(), UTC_TIMESTAMP())
            "#,
        )
        .bind(&record.id)
        .bind(&record.model)
        .bind(&record.version)
        .bind(payload)
        .bind(ctx.subject.or(record.created_by_sub))
        .bind(ctx.tenant_id.or(record.tenant_id))
        .execute(&mut *tx)
        .await
        .map_err(|err| match err {
            sqlx::Error::Database(_) => AppError::Conflict("record id already exists".to_string()),
            other => AppError::from(other),
        })?;

        tx.commit().await?;
        Ok(record.id)
    }

    async fn read_record(&self, id: &str) -> Result<Option<Record>, AppError> {
        let row = sqlx::query(
            r#"
            SELECT id, model, version, payload_json, created_by_sub, tenant_id, created_at, updated_at
            FROM records
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(row_to_record))
    }

    async fn query_records(&self, request: &QueryRequest) -> Result<Vec<Record>, AppError> {
        let rows = sqlx::query(
            r#"
            SELECT id, model, version, payload_json, created_by_sub, tenant_id, created_at, updated_at
            FROM records
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut records: Vec<Record> = rows.into_iter().map(row_to_record).collect();
        request.filter.validate()?;
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
        records.sort_by(|left, right| {
            let left_record = QueryRecord {
                id: &left.id,
                model: &left.model,
                version: &left.version,
                payload: &left.payload,
                created_at: left.created_at,
                updated_at: left.updated_at,
            };
            let right_record = QueryRecord {
                id: &right.id,
                model: &right.model,
                version: &right.version,
                payload: &right.payload,
                created_at: right.created_at,
                updated_at: right.updated_at,
            };
            request
                .filter
                .compare(&left_record, &right_record)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let offset = request.filter.offset.unwrap_or(0) as usize;
        let limit = request.filter.limit.unwrap_or(100) as usize;
        Ok(records.into_iter().skip(offset).take(limit).collect())
    }

    async fn readiness(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

fn row_to_record(row: sqlx::mysql::MySqlRow) -> Record {
    Record {
        id: row.get("id"),
        model: row.get("model"),
        version: row.get("version"),
        payload: row.get::<Json<Value>, _>("payload_json").0,
        created_by_sub: row.get("created_by_sub"),
        tenant_id: row.get("tenant_id"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn payload_hash(payload: &Value) -> String {
    let bytes = serde_json::to_vec(payload).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    format!("{digest:x}")
}

#[derive(Clone, Default)]
pub struct InMemoryRecordRepository {
    state: Arc<RwLock<InMemoryState>>,
}

#[derive(Default)]
struct InMemoryState {
    records: HashMap<String, Record>,
    idempotency: HashMap<String, DateTime<Utc>>,
}

#[async_trait]
impl RecordRepository for InMemoryRecordRepository {
    async fn write_record(
        &self,
        idempotency_key: &str,
        mut record: Record,
        ctx: WriteContext,
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

    async fn read_record(&self, id: &str) -> Result<Option<Record>, AppError> {
        Ok(self.state.read().await.records.get(id).cloned())
    }

    async fn query_records(&self, request: &QueryRequest) -> Result<Vec<Record>, AppError> {
        request.filter.validate()?;
        let state = self.state.read().await;
        let mut records: Vec<Record> = state.records.values().cloned().collect();
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

    async fn readiness(&self) -> Result<(), AppError> {
        Ok(())
    }
}
