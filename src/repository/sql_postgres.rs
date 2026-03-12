use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgPoolOptions, types::Json};

use crate::{
    config::DatabaseConfig,
    error::AppError,
    query::{
        CompiledField, QueryOperator, QueryRequest, RecordQueryCondition, RecordQuerySort,
        compile_field,
    },
};

use super::{
    AccessContext, ReadGrant, Record, RecordRepository, json_literal, payload_hash,
    scalar_as_string,
};

#[derive(Clone)]
pub struct PostgresRecordRepository {
    pool: PgPool,
    idempotency_ttl_seconds: i64,
}

impl PostgresRecordRepository {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, AppError> {
        let pool = PgPoolOptions::new()
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
        const MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/postgres");
        MIGRATIONS
            .run(&self.pool)
            .await
            .map_err(|err| AppError::Internal(format!("database migration failed: {err}")))
    }
}

#[async_trait]
impl RecordRepository for PostgresRecordRepository {
    async fn write_record(
        &self,
        idempotency_key: &str,
        record: Record,
        ctx: AccessContext,
    ) -> Result<String, AppError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM idempotency_keys WHERE expires_at <= CURRENT_TIMESTAMP")
            .execute(&mut *tx)
            .await?;

        let existing = sqlx::query(
            r#"
            SELECT idempotency_key
            FROM idempotency_keys
            WHERE idempotency_key = $1
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
            ) VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(idempotency_key)
        .bind(payload_hash)
        .bind(&record.id)
        .bind(expires_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(record.id)
    }

    async fn read_record(&self, id: &str, ctx: &AccessContext) -> Result<Option<Record>, AppError> {
        let mut builder = QueryBuilder::<Postgres>::new(
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
        let mut builder = QueryBuilder::<Postgres>::new(
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
            VALUES ($1, $2, $3)
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

fn row_to_record(row: sqlx::postgres::PgRow) -> Record {
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

fn push_condition(
    builder: &mut QueryBuilder<'_, Postgres>,
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
    builder: &mut QueryBuilder<'_, Postgres>,
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
            builder.push("CAST(").push(column).push(" AS TEXT) LIKE ");
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
    builder: &mut QueryBuilder<'_, Postgres>,
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
                push_json_text_extract(builder, "payload_json", json_path);
                builder.push(" LIKE ");
                builder.push_bind(format!("%{}%", needle));
                has_clause = true;
            }
            if has_clause {
                builder.push(" OR ");
            }
            push_json_array_contains(builder, "payload_json", json_path, &condition.value)?;
            builder.push(")");
        }
        QueryOperator::Exists => {
            let should_exist = condition.value.as_bool().unwrap_or(false);
            push_json_extract(builder, "payload_json", json_path);
            builder.push(if should_exist {
                " IS NOT NULL"
            } else {
                " IS NULL"
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
                builder.push("CAST(");
                push_json_text_extract(builder, "payload_json", json_path);
                builder.push(" AS DECIMAL(30,10)) ");
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
                push_json_text_extract(builder, "payload_json", json_path);
                builder.push(" ");
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
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: &RecordQuerySort,
) -> Result<(), AppError> {
    match compile_field(&sort.field)? {
        CompiledField::Root(column) => {
            builder.push(column);
        }
        CompiledField::Payload { json_path } => {
            push_json_text_extract(builder, "payload_json", &json_path);
        }
    }
    builder.push(match sort.direction {
        crate::query::SortDirection::Asc => " ASC",
        crate::query::SortDirection::Desc => " DESC",
    });
    Ok(())
}

fn push_scalar_bind(builder: &mut QueryBuilder<'_, Postgres>, value: &Value) {
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

fn push_payload_scalar_comparison(
    builder: &mut QueryBuilder<'_, Postgres>,
    json_path: &str,
    operator: &'static str,
    value: &Value,
) -> Result<(), AppError> {
    match value {
        Value::String(string) => {
            push_json_text_extract(builder, "payload_json", json_path);
            builder.push(" ");
            builder.push(operator);
            builder.push(" ");
            builder.push_bind(string.clone());
        }
        Value::Number(number) => {
            builder.push("CAST(");
            push_json_text_extract(builder, "payload_json", json_path);
            builder.push(" AS DECIMAL(30,10)) ");
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
            push_json_text_extract(builder, "payload_json", json_path);
            builder.push(" ");
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

fn push_json_extract(
    builder: &mut QueryBuilder<'_, Postgres>,
    column: &'static str,
    json_path: &str,
) {
    builder.push("(").push(column).push(" #> ");
    builder.push_bind(postgres_path_segments(json_path));
    builder.push(")");
}

fn push_json_text_extract(
    builder: &mut QueryBuilder<'_, Postgres>,
    column: &'static str,
    json_path: &str,
) {
    builder.push("(").push(column).push(" #>> ");
    builder.push_bind(postgres_path_segments(json_path));
    builder.push(")");
}

fn push_json_array_contains(
    builder: &mut QueryBuilder<'_, Postgres>,
    column: &'static str,
    json_path: &str,
    value: &Value,
) -> Result<(), AppError> {
    builder.push("EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(");
    push_json_extract(builder, column, json_path);
    builder.push(", 'null'::jsonb)) = 'array' THEN COALESCE(");
    push_json_extract(builder, column, json_path);
    builder.push(", '[]'::jsonb) ELSE '[]'::jsonb END) AS elem WHERE elem = ");
    builder.push_bind(json_literal(value)?);
    builder.push("::jsonb)");
    Ok(())
}

fn postgres_path_segments(json_path: &str) -> Vec<String> {
    if json_path == "$" {
        return Vec::new();
    }

    let mut segments = Vec::new();
    let mut chars = json_path.chars().peekable();
    if chars.peek() == Some(&'$') {
        chars.next();
    }

    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                let mut key = String::new();
                while let Some(next) = chars.peek() {
                    if *next == '.' || *next == '[' {
                        break;
                    }
                    key.push(*next);
                    chars.next();
                }
                if !key.is_empty() {
                    segments.push(key);
                }
            }
            '[' => {
                let mut index = String::new();
                while let Some(next) = chars.peek() {
                    if *next == ']' {
                        break;
                    }
                    index.push(*next);
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                }
                if !index.is_empty() {
                    segments.push(index);
                }
            }
            _ => {}
        }
    }

    segments
}

fn push_access_scope(builder: &mut QueryBuilder<'_, Postgres>, ctx: &AccessContext) {
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
