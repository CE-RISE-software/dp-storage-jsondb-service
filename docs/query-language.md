# Query Language

## Overview

`POST /records/query` uses the canonical query filter structure aligned with `hex-core-service`.

The filter language allows the backend to evaluate conditions on:

- root record fields
- selected payload JSON paths
- sort order
- limit
- offset

The service translates this query structure into backend-specific SQL for MySQL, MariaDB, or PostgreSQL.

## Filter Shape

```json
{
  "filter": {
    "where": [
      { "field": "payload.record_scope", "op": "eq", "value": "product" },
      { "field": "model", "op": "eq", "value": "passport" }
    ],
    "sort": [
      { "field": "created_at", "direction": "desc" }
    ],
    "limit": 50,
    "offset": 0
  }
}
```

## `where`

`where` is required and must contain at least one condition.

Each condition contains:

- `field`
- `op`
- `value`

All conditions are currently combined with logical `AND`.

## Supported Operators

The service supports:

- `eq`
- `ne`
- `in`
- `contains`
- `exists`
- `gt`
- `gte`
- `lt`
- `lte`

## Supported Fields

### Root fields

Supported root fields are:

- `id`
- `model`
- `version`
- `created_at`
- `updated_at`

### Payload fields

Payload fields use the `payload.` prefix.

Examples:

- `payload.record_scope`
- `payload.metadata.supported_models`
- `payload.applied_schemas[0].schema_url`

## Payload Path Rules

Payload field paths are validated before execution.

### Allowed forms

- dot notation for object keys
- bracket notation for array indexes

Examples:

- `payload.metadata.type`
- `payload.sections[0].name`
- `payload.applied_schemas[1].schema_url`

### Key restrictions

Payload key segments must contain only:

- ASCII letters
- digits
- underscore

This restriction is deliberate. It keeps the generated backend SQL predictable and avoids unsafe path handling.

## Operator Semantics

### `eq` and `ne`

Exact equality and inequality comparisons.

### `in`

Requires an array query value.

The candidate field matches if it equals any of the provided array values.

### `contains`

Used for:

- string containment on string fields
- element containment on array fields

For array fields, the intended semantics are exact element match, including object elements inside JSON arrays.

### `exists`

Requires a boolean query value.

- `true` means the field must be present
- `false` means the field must be absent

### Range operators

- `gt`
- `gte`
- `lt`
- `lte`

These require a comparable numeric or string query value.

## Sorting

`sort` is optional.

Each sort entry contains:

- `field`
- `direction`

Supported directions:

- `asc`
- `desc`

Sorting supports both root fields and payload paths.

## Limit and Offset

`limit` and `offset` are supported as part of the canonical filter shape.

If omitted, the service applies its internal defaults.

## Access Enforcement During Query

Query evaluation is not performed over all stored records without restriction.

The service first applies storage-side visibility rules, then returns only records visible to the authenticated access context.

That means the same query can return different results for different callers depending on ownership, tenant association, and stored read grants.

## Validation Failures

The service returns `400 Bad Request` for invalid query usage, including cases such as:

- empty `where`
- unsupported field names
- invalid payload path syntax
- `in` with a non-array value
- `exists` with a non-boolean value
- invalid range comparison types
