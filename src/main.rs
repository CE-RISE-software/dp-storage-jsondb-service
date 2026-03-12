use std::sync::Arc;

use dp_storage_jsondb_service::{
    app::{AppState, router},
    auth::AuthService,
    config::AppConfig,
    repository::SqlRecordRepository,
};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .without_time()
        .init();

    let config = AppConfig::from_env()?;
    let auth = AuthService::new(&config.auth)?;
    let repository = SqlRecordRepository::connect(&config.db).await?;
    repository.run_migrations().await?;
    let repository = Arc::new(repository);
    let state = AppState { auth, repository };

    let listener = TcpListener::bind(config.bind_addr()?).await?;
    info!("listening on {}", listener.local_addr()?);
    axum::serve(listener, router(state)).await?;
    Ok(())
}
