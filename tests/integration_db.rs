use std::env;

use chrono::Utc;
use dp_storage_jsondb_service::{
    config::DatabaseConfig,
    query::{
        QueryOperator, QueryRequest, RecordQueryCondition, RecordQueryFilter, RecordQuerySort,
        SortDirection,
    },
    repository::{AccessContext, Record, RecordRepository, SqlRecordRepository},
};
use sqlx::{Executor, mysql::MySqlPoolOptions};

fn test_db_config() -> Option<DatabaseConfig> {
    let host = env::var("TEST_DB_HOST").ok()?;
    let port = env::var("TEST_DB_PORT").ok()?.parse().ok()?;
    let name = env::var("TEST_DB_NAME").ok()?;
    let user = env::var("TEST_DB_USER").ok()?;
    let password = env::var("TEST_DB_PASSWORD").unwrap_or_default();
    Some(DatabaseConfig {
        host,
        port,
        name,
        user,
        password,
        pool_size: 5,
        timeout_ms: 5_000,
    })
}

async fn reset_database(config: &DatabaseConfig) {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&config.url())
        .await
        .expect("connect reset pool");
    pool.execute("DELETE FROM idempotency_keys")
        .await
        .expect("clear idempotency_keys");
    pool.execute("DELETE FROM records")
        .await
        .expect("clear records");
}

fn sample_record(id: &str, scope: &str) -> Record {
    Record {
        id: id.to_string(),
        model: "passport".to_string(),
        version: "1.0.0".to_string(),
        payload: serde_json::json!({
            "record_scope": scope,
            "weight": 12,
            "applied_schemas": [{"schema_url":"urn:test"}]
        }),
        created_by_sub: None,
        tenant_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn owner_ctx(subject: &str, tenant_id: &str) -> AccessContext {
    AccessContext {
        subject: Some(subject.to_string()),
        tenant_id: Some(tenant_id.to_string()),
    }
}

#[tokio::test]
async fn sql_repository_supports_write_read_query_and_idempotency() {
    let Some(config) = test_db_config() else {
        eprintln!("skipping integration_db test; TEST_DB_* env vars are not set");
        return;
    };

    let repository = SqlRecordRepository::connect(&config)
        .await
        .expect("connect repository");
    repository.run_migrations().await.expect("run migrations");
    reset_database(&config).await;

    repository
        .write_record(
            "idem-1",
            sample_record("rec-1", "product"),
            owner_ctx("owner-a", "tenant-a"),
        )
        .await
        .expect("write record");

    let read = repository
        .read_record("rec-1", &owner_ctx("owner-a", "tenant-a"))
        .await
        .expect("read record")
        .expect("record exists");
    assert_eq!(read.id, "rec-1");

    let hidden = repository
        .read_record("rec-1", &owner_ctx("owner-b", "tenant-a"))
        .await
        .expect("hidden read");
    assert!(hidden.is_none());

    let conflict = repository
        .write_record(
            "idem-1",
            sample_record("rec-2", "product"),
            owner_ctx("owner-a", "tenant-a"),
        )
        .await
        .expect_err("idempotency conflict");
    assert!(matches!(
        conflict,
        dp_storage_jsondb_service::error::AppError::Conflict(_)
    ));

    repository
        .write_record(
            "idem-2",
            sample_record("rec-2", "material"),
            owner_ctx("owner-a", "tenant-a"),
        )
        .await
        .expect("write second record");
    repository
        .write_record(
            "idem-3",
            sample_record("rec-3", "product"),
            owner_ctx("owner-b", "tenant-a"),
        )
        .await
        .expect("write third record");

    let records = repository
        .query_records(
            &QueryRequest {
                filter: RecordQueryFilter {
                    where_conditions: vec![RecordQueryCondition {
                        field: "payload.record_scope".to_string(),
                        op: QueryOperator::Eq,
                        value: serde_json::json!("product"),
                    }],
                    sort: vec![RecordQuerySort {
                        field: "id".to_string(),
                        direction: SortDirection::Asc,
                    }],
                    limit: Some(10),
                    offset: Some(0),
                },
            },
            &owner_ctx("owner-a", "tenant-a"),
        )
        .await
        .expect("query records");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, "rec-1");
}
