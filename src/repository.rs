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

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
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

    async fn grant_read_access(&self, record_id: &str, grant: ReadGrant) -> Result<(), AppError> {
        if grant.subject.is_none() && grant.tenant_id.is_none() {
            return Err(AppError::BadRequest(
                "read grant must target a subject or tenant".to_string(),
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO record_read_grants (record_id, grantee_sub, grantee_tenant_id)
            VALUES (?, ?, ?)
            "#,
        )
        .bind(record_id)
        .bind(grant.subject)
        .bind(grant.tenant_id)
        .execute(&self.pool)
        .await?;
        Ok(())
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
            push_payload_scalar_comparison(builder, json_path, "=", &condition.value)?;
        }
        QueryOperator::Ne => {
            push_payload_scalar_comparison(builder, json_path, "<>", &condition.value)?;
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
                push_payload_scalar_comparison(builder, json_path, "=", value)?;
            }
            builder.push(")");
        }
        QueryOperator::Contains => {
            builder.push("(");
            let mut has_clause = false;
            if let Ok(needle) = scalar_as_string(&condition.value) {
                builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
                builder.push_bind(json_path.to_string());
                builder.push(")) LIKE ");
                builder.push_bind(format!("%{}%", needle));
                has_clause = true;
            }
            if has_clause {
                builder.push(" OR ");
            }
            builder.push("JSON_CONTAINS(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push("), ");
            builder.push_bind(json_literal(&condition.value)?);
            builder.push(")");
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

fn push_payload_scalar_comparison(
    builder: &mut QueryBuilder<'_, MySql>,
    json_path: &str,
    operator: &'static str,
    value: &Value,
) -> Result<(), AppError> {
    match value {
        Value::String(string) => {
            builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push(")) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(string.clone());
        }
        Value::Number(number) => {
            builder.push("CAST(JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push(")) AS DECIMAL(30,10)) ");
            builder.push(operator);
            builder.push(" ");
            if let Some(integer) = number.as_i64() {
                builder.push_bind(integer as f64);
            } else if let Some(unsigned) = number.as_u64() {
                builder.push_bind(unsigned as f64);
            } else if let Some(float) = number.as_f64() {
                builder.push_bind(float);
            } else {
                return Err(AppError::BadRequest(
                    "numeric query value is invalid".to_string(),
                ));
            }
        }
        Value::Bool(boolean) => {
            builder.push("JSON_UNQUOTE(JSON_EXTRACT(payload_json, ");
            builder.push_bind(json_path.to_string());
            builder.push(")) ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(boolean.to_string());
        }
        _ => {
            return Err(AppError::BadRequest(
                "payload comparisons currently support scalar query values only".to_string(),
            ));
        }
    }
    Ok(())
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

fn push_access_scope(builder: &mut QueryBuilder<'_, MySql>, ctx: &AccessContext) {
    builder.push(" AND (");
    let mut has_clause = false;

    if let Some(subject) = &ctx.subject {
        builder.push("created_by_sub = ");
        builder.push_bind(subject.clone());
        has_clause = true;
    }
    if let Some(tenant_id) = &ctx.tenant_id {
        if has_clause {
            builder.push(" AND ");
        }
        builder.push("tenant_id = ");
        builder.push_bind(tenant_id.clone());
        has_clause = true;
    }

    if has_clause {
        builder.push(" OR ");
    }

    builder.push("EXISTS (SELECT 1 FROM record_read_grants rrg WHERE rrg.record_id = records.id");
    let mut has_grant_clause = false;
    if ctx.subject.is_none() && ctx.tenant_id.is_none() {
        builder.push(" AND 1 = 0");
    } else {
        builder.push(" AND (");
        if let Some(subject) = &ctx.subject {
            builder.push("rrg.grantee_sub = ");
            builder.push_bind(subject.clone());
            has_grant_clause = true;
        }
        if let Some(tenant_id) = &ctx.tenant_id {
            if has_grant_clause {
                builder.push(" OR ");
            }
            builder.push("rrg.grantee_tenant_id = ");
            builder.push_bind(tenant_id.clone());
            has_grant_clause = true;
        }
        if !has_grant_clause {
            builder.push("1 = 0");
        }
        builder.push(")");
    }
    builder.push("))");
}

fn record_is_visible(record: &Record, grants: &[ReadGrant], ctx: &AccessContext) -> bool {
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
