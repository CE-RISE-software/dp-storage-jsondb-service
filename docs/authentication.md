# Authentication

## Overview

`dp-storage-jsondb-service` enforces authentication and authorization on `/records*` endpoints in normal operation.

The service supports two auth modes:

- `jwt_jwks`
- `disabled`

`jwt_jwks` is the normal deployment mode. `disabled` exists only for local development and test scenarios.

## JWT and JWKS Mode

When `AUTH_MODE=jwt_jwks`, the service validates bearer tokens against the configured identity provider settings.

### Required runtime settings

- `AUTH_JWKS_URL`
- `AUTH_ISSUER`
- `AUTH_AUDIENCE`

### Validation behavior

The service validates:

- presence of a bearer token
- signature against the configured JWKS
- issuer
- audience

If validation fails, the request is rejected.

## Scopes

The service accepts scopes from both:

- `scope`
- `scp`

This is intentional so the backend remains compatible with common identity provider claim conventions.

### Required scopes

- `POST /records` requires `records:write`
- `GET /records/{id}` requires `records:read`
- `POST /records/query` requires `records:read`

## Error Semantics

### `401 Unauthorized`

Returned when:

- the bearer token is missing in normal mode
- the token is malformed
- the token signature is invalid
- the issuer or audience is invalid

### `403 Forbidden`

Returned when:

- the token is valid
- but the required scope is not present

This distinction is important because it separates authentication failures from authorization failures.

## Disabled Mode

When `AUTH_MODE=disabled`, the service bypasses JWT validation.

This mode is for:

- local development
- local testing
- manual testing without an identity provider

It must not be treated as a production deployment mode.

### Behavior in disabled mode

Requests are accepted without an `Authorization` header, and the service uses a fixed development identity internally.

This allows local execution without a live JWKS endpoint.

## Token Handling Safety

The service must never log raw bearer tokens.

Operational logs may report that authentication failed, but must not expose the token contents.

## Identity Context Used By Storage

The service captures and uses identity context for storage-side enforcement:

- subject (`sub`)
- tenant identifier when present through the authenticated context

These values are used to populate record ownership metadata and to evaluate read visibility rules.
