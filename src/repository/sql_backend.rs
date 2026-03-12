use crate::config::DatabaseBackend;

use super::sql_runtime::SqlQueryBuilder;

pub trait SqlBackendExt {
    fn delete_expired_idempotency_sql(self) -> &'static str;
    fn push_json_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    );
    fn push_json_text_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    );
    fn push_json_contains_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    );
}

impl SqlBackendExt for DatabaseBackend {
    fn delete_expired_idempotency_sql(self) -> &'static str {
        match self {
            DatabaseBackend::MySql | DatabaseBackend::MariaDb => {
                "DELETE FROM idempotency_keys WHERE expires_at <= CURRENT_TIMESTAMP"
            }
        }
    }

    fn push_json_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    ) {
        match self {
            DatabaseBackend::MySql | DatabaseBackend::MariaDb => {
                builder.push("JSON_EXTRACT(").push(column).push(", ");
                builder.push_bind(json_path.to_string());
                builder.push(")");
            }
        }
    }

    fn push_json_text_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    ) {
        match self {
            DatabaseBackend::MySql | DatabaseBackend::MariaDb => {
                builder.push("JSON_UNQUOTE(");
                self.push_json_extract(builder, column, json_path);
                builder.push(")");
            }
        }
    }

    fn push_json_contains_extract(
        self,
        builder: &mut SqlQueryBuilder<'_>,
        column: &'static str,
        json_path: &str,
    ) {
        match self {
            DatabaseBackend::MySql | DatabaseBackend::MariaDb => {
                builder.push("JSON_CONTAINS(");
                self.push_json_extract(builder, column, json_path);
            }
        }
    }
}
