# HTTP Contract

## Purpose

This page documents the storage-side HTTP contract implemented by `dp-storage-jsondb-service`.

This contract must remain compatible with the `hex-core-service` `io-http` adapter. The service is therefore not free to invent a different wire format casually. Any incompatible change to these endpoints is a breaking change for the adapter relationship.

## Base Assumption

In the normal CE-RISE deployment model, these endpoints are called by `hex-core-service`, not directly by end users.

## Record Shape

The service stores and returns records compatible with the `hex-core-service` domain.

```json
{
  "id": "string",
  "model": "string",
  "version": "string",
  "payload": { "any": "json" }
}
```

Additional persistence-side metadata such as owner subject, tenant, timestamps, and access grants are handled internally and are not part of the main record payload returned by the storage API.

## `POST /records`

Creates a record.

### Headers

- `Authorization: Bearer <token>`
- `Idempotency-Key: <key>`

### Request body

Full `Record` JSON.

### Success response

Status: `200 OK`

```json
{ "id": "record-id" }
```

### Required behavior

- `Idempotency-Key` is mandatory
- missing or empty idempotency key returns `400 Bad Request`
- reuse of an active idempotency key returns `409 Conflict`
- idempotency keys are globally scoped
- idempotency keys are short-lived replay protection, not permanent deduplication records
- the active TTL target is `120` seconds after successful processing

### Error cases

- `400 Bad Request` for invalid input or missing idempotency key
- `401 Unauthorized` for missing or invalid bearer token in normal auth mode
- `403 Forbidden` for valid token without `records:write`
- `409 Conflict` for active idempotency reuse or record-id conflict
- `5xx` class behavior is surfaced as service-side internal or unavailable errors depending on the failure

## `GET /records/{id}`

Reads a record by id.

### Headers

- `Authorization: Bearer <token>`

### Success response

Status: `200 OK`

Body: full `Record` JSON.

### Error cases

- `401 Unauthorized` for missing or invalid bearer token in normal auth mode
- `403 Forbidden` for valid token without `records:read`
- `404 Not Found` when the record does not exist or is not visible through the storage-side access rules

## `POST /records/query`

Queries records using the canonical filter structure.

### Headers

- `Authorization: Bearer <token>`

### Request body

```json
{
  "filter": {
    "where": [
      { "field": "id", "op": "eq", "value": "record-001" }
    ],
    "sort": [
      { "field": "created_at", "direction": "desc" }
    ],
    "limit": 50,
    "offset": 0
  }
}
```

### Success response

Status: `200 OK`

```json
{
  "records": [
    {
      "id": "record-001",
      "model": "passport",
      "version": "1.0.0",
      "payload": {}
    }
  ]
}
```

### Required behavior

- at least one `where` condition is required
- `sort`, `limit`, and `offset` are supported
- payload field paths are supported through the canonical query field syntax
- storage-side access rules are enforced before records are returned

### Error cases

- `400 Bad Request` for invalid query shape or unsupported field/operator combinations
- `401 Unauthorized` for missing or invalid bearer token in normal auth mode
- `403 Forbidden` for valid token without `records:read`

## `GET /health`

Liveness endpoint.

### Purpose

Confirms the service process is alive.

### Expected behavior

Returns a successful response when the HTTP service is running.

This endpoint is not intended to perform deep backend verification.

## `GET /ready`

Readiness endpoint.

### Purpose

Confirms the service is ready to serve real traffic.

### Expected behavior

The service checks database connectivity before reporting readiness.

If the database backend is unavailable, readiness must fail even if the process itself is alive.

## `GET /openapi.json`

Returns the generated OpenAPI description for this backend service.

This describes the storage-side HTTP contract implemented here, aligned to the adapter expectations used by `hex-core-service`.
