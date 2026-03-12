# Configuration

## Overview

`dp-storage-jsondb-service` is configured entirely through environment variables.

This keeps the container image generic so the same build can be deployed with different database backends, credentials, hostnames, and auth settings without rebuilding the image.

## Server Settings

### `SERVER_HOST`

Bind address for the HTTP server.

Typical value:

```text
0.0.0.0
```

### `SERVER_PORT`

Bind port for the HTTP server.

Typical value:

```text
8080
```

## Database Settings

### `DB_BACKEND`

Selects the database backend implementation.

Supported values:

- `mysql`
- `mariadb`
- `postgres`

### `DB_HOST`

Database host name or IP address.

### `DB_PORT`

Database port.

Default expectation depends on backend:

- MySQL: `3306`
- MariaDB: `3306`
- PostgreSQL: `5432`

### `DB_NAME`

Database name.

### `DB_USER`

Database user.

### `DB_PASSWORD`

Database password.

### `DB_POOL_SIZE`

Maximum number of database connections maintained in the SQL pool.

### `DB_TIMEOUT_MS`

Database connection and acquisition timeout in milliseconds.

## Authentication Settings

### `AUTH_MODE`

Supported values:

- `jwt_jwks`
- `disabled`

Use `jwt_jwks` in normal operation.

Use `disabled` only for local development and testing.

### `AUTH_JWKS_URL`

JWKS endpoint used to validate bearer tokens in `jwt_jwks` mode.

### `AUTH_ISSUER`

Expected JWT issuer.

### `AUTH_AUDIENCE`

Expected JWT audience.

## Example

```text
SERVER_HOST=0.0.0.0
SERVER_PORT=8080

DB_BACKEND=postgres
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=dp_storage
DB_USER=dp_storage
DB_PASSWORD=change-me
DB_POOL_SIZE=10
DB_TIMEOUT_MS=5000

AUTH_MODE=jwt_jwks
AUTH_JWKS_URL=https://example.org/.well-known/jwks.json
AUTH_ISSUER=https://example.org/
AUTH_AUDIENCE=ce-rise
```

## Startup Migrations

The service runs its migrations on startup.

This means the configured database user must have the privileges required to apply the backend-specific schema for the selected `DB_BACKEND`.

## Configuration Errors

Startup should fail if required configuration is invalid. Typical configuration failures include:

- unsupported `DB_BACKEND`
- unsupported `AUTH_MODE`
- invalid numeric values for ports, pool size, or timeout
- invalid bind address resolution
