use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{MySql, MySqlPool, QueryBuilder, Row, mysql::MySqlPoolOptions, types::Json};
use tokio::sync::RwLock;

use crate::{
    config::DatabaseConfig,
    error::AppError,
    query::{
        CompiledField, QueryOperator, QueryRecord, QueryRequest, RecordQueryCondition,
        RecordQuerySort, compile_field,
    },
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
pub struct AccessContext {
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

    pub async fn run_migrations(&self) -> Result<(), AppError> {
        const MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
        MIGRATIONS
            .run(&self.pool)
            .await
            .map_err(|err| AppError::Internal(format!("database migration failed: {err}")))
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
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM idempotency_keys WHERE expires_at <= UTC_TIMESTAMP()")
            .execute(&mut *tx)
            .await?;

        let existing = sqlx::query(
            r#"
            SELECT idempotency_key
            FROM idempotency_keys
            WHERE idempotency_key = ?
            LIMIT 1
            "#,
        )
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;

        if existing.is_some() {
            return Err(AppError::Conflict(
                "idempotency key is still active".to_string(),
            ));
        }

        let expires_at = Utc::now() + Duration::seconds(self.idempotency_ttl_seconds);
        let payload_hash = payload_hash(&record.payload);
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

        sqlx::query(
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
        .await?;

        tx.commit().await?;
        Ok(record.id)
    }

    async fn read_record(&self, id: &str, ctx: &AccessContext) -> Result<Option<Record>, AppError> {
        let mut builder = QueryBuilder::<MySql>::new(
            r#"
            SELECT id, model, version, payload_json, created_by_sub, tenant_id, created_at, updated_at
            FROM records
            WHERE id =
            "#,
        );
        builder.push_bind(id);
        push_access_scope(&mut builder, ctx);
        let row = builder.build().fetch_optional(&self.pool).await?;

        Ok(row.map(row_to_record))
    }

    async fn query_records(
        &self,
        request: &QueryRequest,
        ctx: &AccessContext,
    ) -> Result<Vec<Record>, AppError> {
        request.filter.validate()?;
        let mut builder = QueryBuilder::<MySql>::new(
            r#"
            SELECT id, model, version, payload_json, created_by_sub, tenant_id, created_at, updated_at
            FROM records
            WHERE 1 = 1
            "#,
        );
        push_access_scope(&mut builder, ctx);

        for condition in &request.filter.where_conditions {
            builder.push(" AND ");
            push_condition(&mut builder, condition)?;
        }

        if !request.filter.sort.is_empty() {
            builder.push(" ORDER BY ");
            for (index, sort) in request.filter.sort.iter().enumerate() {
                if index > 0 {
                    builder.push(", ");
                }
                push_sort(&mut builder, sort)?;
            }
        }

        builder.push(" LIMIT ");
        builder.push_bind(request.filter.limit.unwrap_or(100) as i64);
        builder.push(" OFFSET ");
        builder.push_bind(request.filter.offset.unwrap_or(0) as i64);

        let rows = builder.build().fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(row_to_record).collect())
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

fn push_condition(
    builder: &mut QueryBuilder<'_, MySql>,
    condition: &RecordQueryCondition,
) -> Result<(), AppError> {
    match compile_field(&condition.field)? {
        CompiledField::Root(column) => push_root_condition(builder, column, condition),
        CompiledField::Payload { json_path } => {
            push_payload_condition(builder, &json_path, condition)
        }
    }
}

fn push_root_condition(
    builder: &mut QueryBuilder<'_, MySql>,
    column: &'static str,
    condition: &RecordQueryCondition,
) -> Result<(), AppError> {
    match condition.op {
        QueryOperator::Eq => {
            builder.push(column).push(" = ");
            push_scalar_bind(builder, &condition.value);
        }
        QueryOperator::Ne => {
            builder.push(column).push(" <> ");
            push_scalar_bind(builder, &condition.value);
        }
        QueryOperator::In => {
            let values = condition.value.as_array().ok_or_else(|| {
                AppError::BadRequest(format!(
                    "operator `in` requires an array value for field `{}`",
                    condition.field
                ))
            })?;
            if values.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "operator `in` requires at least one candidate value for field `{}`",
                    condition.field
                )));
            }
            builder.push(column).push(" IN (");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    builder.push(", ");
                }
                push_scalar_bind(builder, value);
            }
            builder.push(")");
        }
        QueryOperator::Contains => {
            builder.push("CAST(").push(column).push(" AS CHAR) LIKE ");
            builder.push_bind(format!("%{}%", scalar_as_string(&condition.value)?));
        }
        QueryOperator::Exists => {
            let should_exist = condition.value.as_bool().unwrap_or(false);
            builder.push(column).push(if should_exist {
                " IS NOT NULL"
            } else {
                " IS NULL"
            });
        }
        QueryOperator::Gt | QueryOperator::Gte | QueryOperator::Lt | QueryOperator::Lte => {
            builder.push(column).push(match condition.op {
                QueryOperator::Gt => " > ",
                QueryOperator::Gte => " >= ",
                QueryOperator::Lt => " < ",
                QueryOperator::Lte => " <= ",
                _ => unreachable!(),
            });
            push_scalar_bind(builder, &condition.value);
        }
    }
    Ok(())
}

fn push_payload_condition(
    builder: &mut QueryBuilder<'_, MySql>,
    json_path: &str,
    condition: &RecordQueryCondition,
) -> Result<(), AppError> {
    match condition.op {
        QueryOperator::Eq => {
            builder.push("COALESCE(JSON_CONTAINS(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push("), CAST(");
            builder.push_bind(json_literal(&condition.value)?);
            builder.push(" AS JSON)), 0) = 1");
        }
        QueryOperator::Ne => {
            builder.push("COALESCE(JSON_CONTAINS(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push("), CAST(");
            builder.push_bind(json_literal(&condition.value)?);
            builder.push(" AS JSON)), 0) = 0");
        }
        QueryOperator::In => {
            let values = condition.value.as_array().ok_or_else(|| {
                AppError::BadRequest(format!(
                    "operator `in` requires an array value for field `{}`",
                    condition.field
                ))
            })?;
            if values.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "operator `in` requires at least one candidate value for field `{}`",
                    condition.field
                )));
            }
            builder.push("(");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    builder.push(" OR ");
                }
                builder.push("COALESCE(JSON_CONTAINS(JSON_EXTRACT(payload_json, ");
                builder.push_bind(json_path.to_string());
                builder.push("), CAST(");
                builder.push_bind(json_literal(value)?);
                builder.push(" AS JSON)), 0) = 1");
            }
            builder.push(")");
        }
        QueryOperator::Contains => {
            builder.push("(");
            builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push(")) LIKE ");
            builder.push_bind(format!("%{}%", scalar_as_string(&condition.value)?));
            builder.push(" OR COALESCE(JSON_CONTAINS(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push("), CAST(");
            builder.push_bind(json_literal(&condition.value)?);
            builder.push(" AS JSON)), 0) = 1");
            builder.push(")");
        }
        QueryOperator::Exists => {
            let should_exist = condition.value.as_bool().unwrap_or(false);
            builder.push("JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push(if should_exist {
                ") IS NOT NULL"
            } else {
                ") IS NULL"
            });
        }
        QueryOperator::Gt | QueryOperator::Gte | QueryOperator::Lt | QueryOperator::Lte => {
            let operator = match condition.op {
                QueryOperator::Gt => ">",
                QueryOperator::Gte => ">=",
                QueryOperator::Lt => "<",
                QueryOperator::Lte => "<=",
                _ => unreachable!(),
            };
            if condition.value.is_number() {
                builder.push("CAST(JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
                builder.push_bind(json_path.to_string());
                builder.push(")) AS DECIMAL(30,10)) ");
                builder.push(operator);
                builder.push(" ");
                if let Some(number) = condition.value.as_f64() {
                    builder.push_bind(number);
                } else {
                    return Err(AppError::BadRequest(format!(
                        "numeric comparison is invalid for field `{}`",
                        condition.field
                    )));
                }
            } else if condition.value.is_string() {
                builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
                builder.push_bind(json_path.to_string());
                builder.push(")) ");
                builder.push(operator);
                builder.push(" ");
                builder.push_bind(scalar_as_string(&condition.value)?);
            } else {
                return Err(AppError::BadRequest(format!(
                    "range comparisons for field `{}` require a numeric or string value",
                    condition.field
                )));
            }
        }
    }
    Ok(())
}

fn push_sort(
    builder: &mut QueryBuilder<'_, MySql>,
    sort: &RecordQuerySort,
) -> Result<(), AppError> {
    match compile_field(&sort.field)? {
        CompiledField::Root(column) => {
            builder.push(column);
        }
        CompiledField::Payload { json_path } => {
            builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path);
            builder.push("))");
        }
    }
    builder.push(match sort.direction {
        crate::query::SortDirection::Asc => " ASC",
        crate::query::SortDirection::Desc => " DESC",
    });
    Ok(())
}

fn push_scalar_bind(builder: &mut QueryBuilder<'_, MySql>, value: &Value) {
    match value {
        Value::String(value) => {
            builder.push_bind(value.clone());
        }
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                builder.push_bind(integer);
            } else if let Some(unsigned) = value.as_u64() {
                builder.push_bind(unsigned as i64);
            } else if let Some(float) = value.as_f64() {
                builder.push_bind(float);
            } else {
                builder.push_bind(value.to_string());
            }
        }
        Value::Bool(value) => {
            builder.push_bind(*value);
        }
        Value::Null => {
            builder.push_bind(Option::<String>::None);
        }
        other => {
            builder.push_bind(other.to_string());
        }
    }
}

fn scalar_as_string(value: &Value) -> Result<String, AppError> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        _ => Err(AppError::BadRequest(
            "operator requires a scalar query value".to_string(),
        )),
    }
}

fn json_literal(value: &Value) -> Result<String, AppError> {
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
        Ok(if record_is_visible(&record, ctx) {
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
        records.retain(|record| record_is_visible(record, ctx));
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

fn push_access_scope(builder: &mut QueryBuilder<'_, MySql>, ctx: &AccessContext) {
    if let Some(subject) = &ctx.subject {
        builder.push(" AND created_by_sub = ");
        builder.push_bind(subject.clone());
    }
    if let Some(tenant_id) = &ctx.tenant_id {
        builder.push(" AND tenant_id = ");
        builder.push_bind(tenant_id.clone());
    }
}

fn record_is_visible(record: &Record, ctx: &AccessContext) -> bool {
    let subject_ok = match &ctx.subject {
        Some(subject) => record.created_by_sub.as_ref() == Some(subject),
        None => true,
    };
    let tenant_ok = match &ctx.tenant_id {
        Some(tenant_id) => record.tenant_id.as_ref() == Some(tenant_id),
        None => true,
    };
    subject_ok && tenant_ok
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
    }
}
