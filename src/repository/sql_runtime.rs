use sqlx::{MySql, MySqlPool, QueryBuilder, mysql::MySqlPoolOptions};

pub type SqlDb = MySql;
pub type SqlPool = MySqlPool;
pub type SqlPoolOptions = MySqlPoolOptions;
pub type SqlQueryBuilder<'args> = QueryBuilder<'args, SqlDb>;
pub type SqlRow = sqlx::mysql::MySqlRow;
