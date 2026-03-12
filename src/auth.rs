use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::{
    config::{AuthConfig, AuthMode},
    error::AppError,
};

#[derive(Clone)]
pub struct AuthService {
    mode: AuthMode,
    validator: Option<JwtValidator>,
}

#[derive(Clone)]
struct JwtValidator {
    client: reqwest::Client,
    jwks_url: String,
    issuer: String,
    audience: String,
    allowed_algorithms: Vec<Algorithm>,
    cache: Arc<RwLock<Option<CachedJwks>>>,
}

#[derive(Clone)]
struct CachedJwks {
    keys: HashMap<String, DecodingKey>,
    expires_at: Instant,
}

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub subject: String,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Claims {
    sub: String,
    iss: String,
    aud: Audience,
    exp: usize,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    scp: Option<ScopeClaim>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum Audience {
    Single(String),
    Multi(Vec<String>),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum ScopeClaim {
    Single(String),
    Multi(Vec<String>),
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
    kty: String,
}

impl AuthService {
    pub fn new(config: &AuthConfig) -> Result<Self, AppError> {
        let validator =
            match config.mode {
                AuthMode::Disabled => None,
                AuthMode::JwtJwks => Some(JwtValidator {
                    client: reqwest::Client::new(),
                    jwks_url: config.jwks_url.clone().ok_or_else(|| {
                        AppError::Internal("AUTH_JWKS_URL is required".to_string())
                    })?,
                    issuer: config
                        .issuer
                        .clone()
                        .ok_or_else(|| AppError::Internal("AUTH_ISSUER is required".to_string()))?,
                    audience: config.audience.clone().ok_or_else(|| {
                        AppError::Internal("AUTH_AUDIENCE is required".to_string())
                    })?,
                    allowed_algorithms: vec![Algorithm::RS256, Algorithm::RS384, Algorithm::RS512],
                    cache: Arc::new(RwLock::new(None)),
                }),
            };
        Ok(Self {
            mode: config.mode,
            validator,
        })
    }

    pub async fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: &'static str,
    ) -> Result<AuthContext, AppError> {
        match self.mode {
            AuthMode::Disabled => Ok(AuthContext {
                subject: "dev-user".to_string(),
                tenant_id: Some("dev-tenant".to_string()),
            }),
            AuthMode::JwtJwks => {
                let token = bearer_token(headers)?;
                let claims = self
                    .validator
                    .as_ref()
                    .expect("validator configured for jwt_jwks")
                    .validate(&token)
                    .await?;
                let scopes = extract_scopes(&claims);
                if !scopes.contains(required_scope) {
                    return Err(AppError::Forbidden(format!(
                        "missing required scope `{required_scope}`"
                    )));
                }
                Ok(AuthContext {
                    subject: claims.sub,
                    tenant_id: claims.tenant_id.or(claims.tenant),
                })
            }
        }
    }

    #[cfg(test)]
    pub fn new_test_hmac(secret: &[u8], issuer: &str, audience: &str) -> Self {
        let mut keys = HashMap::new();
        keys.insert("test-kid".to_string(), DecodingKey::from_secret(secret));
        Self {
            mode: AuthMode::JwtJwks,
            validator: Some(JwtValidator {
                client: reqwest::Client::new(),
                jwks_url: "http://test.invalid/jwks".to_string(),
                issuer: issuer.to_string(),
                audience: audience.to_string(),
                allowed_algorithms: vec![Algorithm::HS256],
                cache: Arc::new(RwLock::new(Some(CachedJwks {
                    keys,
                    expires_at: Instant::now() + Duration::from_secs(3600),
                }))),
            }),
        }
    }
}

impl JwtValidator {
    async fn validate(&self, token: &str) -> Result<Claims, AppError> {
        let header = decode_header(token)
            .map_err(|err| AppError::Unauthorized(format!("invalid token header: {err}")))?;
        let kid = header
            .kid
            .ok_or_else(|| AppError::Unauthorized("token missing `kid` header".to_string()))?;
        let key = self.decoding_key(&kid).await?;

        if !self.allowed_algorithms.contains(&header.alg) {
            return Err(AppError::Unauthorized(
                "unsupported token signing algorithm".to_string(),
            ));
        }
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);

        let token = decode::<Claims>(token, &key, &validation)
            .map_err(|err| AppError::Unauthorized(format!("token validation failed: {err}")))?;

        validate_claims(&token.claims, &self.issuer, &self.audience)?;
        Ok(token.claims)
    }

    async fn decoding_key(&self, kid: &str) -> Result<DecodingKey, AppError> {
        {
            let cache = self.cache.read().await;
            if let Some(cache) = cache.as_ref() {
                if cache.expires_at > Instant::now() {
                    if let Some(key) = cache.keys.get(kid) {
                        return Ok(key.clone());
                    }
                }
            }
        }

        let mut cache = self.cache.write().await;
        let jwks = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(|err| AppError::Unavailable(format!("jwks fetch failed: {err}")))?
            .error_for_status()
            .map_err(|err| AppError::Unavailable(format!("jwks fetch failed: {err}")))?
            .json::<JwksResponse>()
            .await
            .map_err(|err| AppError::Unavailable(format!("jwks decode failed: {err}")))?;

        let mut keys = HashMap::new();
        for jwk in jwks.keys {
            if jwk.kty != "RSA" {
                continue;
            }
            let key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
                .map_err(|err| AppError::Unauthorized(format!("invalid jwk: {err}")))?;
            keys.insert(jwk.kid, key);
        }

        let expires_at = Instant::now() + Duration::from_secs(300);
        *cache = Some(CachedJwks { keys, expires_at });
        cache
            .as_ref()
            .and_then(|cached| cached.keys.get(kid).cloned())
            .ok_or_else(|| AppError::Unauthorized("signing key not found in jwks".to_string()))
    }
}

fn bearer_token(headers: &HeaderMap) -> Result<String, AppError> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| AppError::Unauthorized("missing bearer token".to_string()))?;
    let header = header
        .to_str()
        .map_err(|_| AppError::Unauthorized("invalid authorization header".to_string()))?;
    header
        .strip_prefix("Bearer ")
        .map(ToString::to_string)
        .ok_or_else(|| AppError::Unauthorized("invalid bearer token".to_string()))
}

fn extract_scopes(claims: &Claims) -> HashSet<String> {
    let mut scopes = HashSet::new();
    if let Some(scope) = &claims.scope {
        for value in scope.split_whitespace() {
            scopes.insert(value.to_string());
        }
    }
    if let Some(scope) = &claims.scp {
        match scope {
            ScopeClaim::Single(value) => {
                for item in value.split_whitespace() {
                    scopes.insert(item.to_string());
                }
            }
            ScopeClaim::Multi(values) => {
                for value in values {
                    scopes.insert(value.to_string());
                }
            }
        }
    }
    scopes
}

fn validate_claims(claims: &Claims, issuer: &str, audience: &str) -> Result<(), AppError> {
    if claims.iss != issuer {
        return Err(AppError::Unauthorized("token issuer mismatch".to_string()));
    }
    let valid_audience = match &claims.aud {
        Audience::Single(value) => value == audience,
        Audience::Multi(values) => values.iter().any(|value| value == audience),
    };
    if !valid_audience {
        return Err(AppError::Unauthorized(
            "token audience mismatch".to_string(),
        ));
    }
    if claims.exp <= chrono::Utc::now().timestamp() as usize {
        return Err(AppError::Unauthorized("token expired".to_string()));
    }
    Ok(())
}
