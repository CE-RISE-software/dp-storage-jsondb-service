use std::{env, net::SocketAddr, str::FromStr};

use crate::error::ConfigError;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub server_host: String,
    pub server_port: u16,
    pub db: DatabaseConfig,
    pub auth: AuthConfig,
}

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub user: String,
    pub password: String,
    pub pool_size: u32,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub jwks_url: Option<String>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthMode {
    Disabled,
    JwtJwks,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            server_host: get_env("SERVER_HOST").unwrap_or_else(|| "0.0.0.0".to_string()),
            server_port: parse_env("SERVER_PORT").unwrap_or(8080),
            db: DatabaseConfig {
                host: get_env("DB_HOST").unwrap_or_else(|| "127.0.0.1".to_string()),
                port: parse_env("DB_PORT").unwrap_or(3306),
                name: get_env("DB_NAME").unwrap_or_else(|| "dp_storage".to_string()),
                user: get_env("DB_USER").unwrap_or_else(|| "dp_storage".to_string()),
                password: get_env("DB_PASSWORD").unwrap_or_default(),
                pool_size: parse_env("DB_POOL_SIZE").unwrap_or(10),
                timeout_ms: parse_env("DB_TIMEOUT_MS").unwrap_or(5_000),
            },
            auth: AuthConfig {
                mode: parse_auth_mode(get_env("AUTH_MODE").as_deref().unwrap_or("jwt_jwks"))?,
                jwks_url: get_env("AUTH_JWKS_URL"),
                issuer: get_env("AUTH_ISSUER"),
                audience: get_env("AUTH_AUDIENCE"),
            },
        })
    }

    pub fn bind_addr(&self) -> Result<SocketAddr, ConfigError> {
        SocketAddr::from_str(&format!("{}:{}", self.server_host, self.server_port))
            .map_err(|err| ConfigError::InvalidValue(format!("invalid bind address: {err}")))
    }
}

impl DatabaseConfig {
    pub fn url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.name
        )
    }
}

fn get_env(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn parse_env<T>(key: &str) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let value = env::var(key).map_err(|_| ConfigError::Missing(key.to_string()))?;
    value
        .parse()
        .map_err(|err| ConfigError::InvalidValue(format!("{key}: {err}")))
}

fn parse_auth_mode(value: &str) -> Result<AuthMode, ConfigError> {
    match value {
        "disabled" => Ok(AuthMode::Disabled),
        "jwt_jwks" => Ok(AuthMode::JwtJwks),
        other => Err(ConfigError::InvalidValue(format!(
            "AUTH_MODE must be `disabled` or `jwt_jwks`, got `{other}`"
        ))),
    }
}
