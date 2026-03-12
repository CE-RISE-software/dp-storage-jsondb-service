# Operations

## Health and Readiness

The service exposes two operational endpoints.

### `/health`

Use this endpoint to check whether the service process is alive.

This is a liveness indicator.

### `/ready`

Use this endpoint to check whether the service is ready to handle real traffic.

This endpoint verifies database connectivity, so readiness can fail even when liveness succeeds.

## Startup Behavior

On startup, the service:

1. loads runtime configuration
2. initializes authentication behavior
3. connects to the configured database backend
4. runs backend-specific migrations
5. starts the HTTP server

If any of those steps fail, startup fails.

## Migrations

The service runs migrations automatically during startup.

Operationally, this means:

- first deployment can initialize the schema automatically
- normal restarts do not reapply already-applied migrations blindly
- future schema changes must remain carefully written and non-destructive unless an intentional breaking migration is planned

## Access and Visibility Behavior

Reads and queries are filtered by storage-side visibility rules.

If an operator sees a `404` or an empty query result where data is expected, the cause may be:

- record really does not exist
- owner subject mismatch
- tenant mismatch
- missing explicit read grant

## Common Failure Classes

### Database connectivity failure

Symptoms:

- startup fails
- `/ready` fails
- repository operations return unavailable or internal errors

Typical causes:

- wrong host or port
- bad credentials
- backend container not ready
- network policy blocking connectivity

### Auth configuration failure

Symptoms:

- startup or request-time auth errors
- all protected routes return `401`

Typical causes:

- wrong JWKS URL
- wrong issuer
- wrong audience
- unexpected token format from the identity provider

### Scope mismatch

Symptoms:

- authenticated requests return `403`

Typical cause:

- valid token does not include `records:read` or `records:write`

### Query validation failure

Symptoms:

- `POST /records/query` returns `400`

Typical causes:

- unsupported field path
- invalid operator usage
- wrong value type for `in` or `exists`
- unsupported comparison type

## Logging Considerations

Operational logs should help identify failures without leaking sensitive material.

In particular:

- raw bearer tokens must never be logged
- request failures may be classified and described
- database errors may be surfaced in sanitized form

## Local Troubleshooting

For backend-specific issues, the fastest checks are usually:

```bash
cargo test
bash scripts/test-mysql.sh
bash scripts/test-mariadb.sh
bash scripts/test-postgres.sh
```

This separates pure Rust regressions from backend-specific SQL issues.
