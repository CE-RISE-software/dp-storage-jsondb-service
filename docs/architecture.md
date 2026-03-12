# Architecture

## Service Role

`dp-storage-jsondb-service` is the persistence backend for CE-RISE `hex-core-service`.

It exists to provide a stable storage-side HTTP contract behind the `hex-core-service` `io-http` adapter. Its responsibility is not to understand model semantics in depth, resolve model artifacts, or validate domain rules. Its responsibility is to accept already-shaped records from `hex-core-service`, persist them, retrieve them, and evaluate storage-side queries over record metadata and JSON payloads.

This narrow role is intentional. It keeps the persistence layer deployable, replaceable, and testable without mixing orchestration concerns into the storage service.

## Logical View

```text
+------------------+
| Client / Caller  |
+--------+---------+
         |
         | public CE-RISE API
         v
+-----------------------------+
| hex-core-service            |
|-----------------------------|
| - authentication context    |
| - model artifact resolution |
| - validation                |
| - orchestration             |
| - io-http adapter           |
+--------------+--------------+
               |
               | backend storage contract
               | POST /records
               | GET  /records/{id}
               | POST /records/query
               v
+-----------------------------+
| dp-storage-jsondb-service   |
|-----------------------------|
| - auth enforcement          |
| - idempotency control       |
| - query translation         |
| - access enforcement        |
| - persistence               |
| - health/readiness          |
+--------------+--------------+
               |
               | SQL
               v
+-----------------------------------------------+
| MariaDB / MySQL / PostgreSQL                  |
|-----------------------------------------------|
| - records                                     |
| - idempotency_keys                            |
| - record_read_grants                          |
+-----------------------------------------------+
```

## Responsibility Boundaries

### Responsibilities of `hex-core-service`

`hex-core-service` remains responsible for:

- exposing the primary CE-RISE API surface to callers
- resolving model and version artifacts
- validating payloads against model rules
- deciding when records should be created or queried
- calling the storage backend through the configured outbound adapter

### Responsibilities of `dp-storage-jsondb-service`

This service is responsible for:

- receiving the storage adapter HTTP calls from `hex-core-service`
- authenticating and authorizing those requests
- enforcing idempotency on `POST /records`
- storing complete records as JSON documents
- retrieving stored records by id
- translating canonical query filters into backend SQL
- enforcing storage-side access rules during reads and queries
- reporting health and readiness

### Responsibilities of the database backend

The database backend is responsible for:

- durable storage of records and access metadata
- integrity constraints
- persistence of idempotency windows
- supporting the SQL and JSON operations required by this service

## Data Model Approach

The storage design deliberately avoids model-specific relational decomposition.

Each record is stored as one full JSON payload, with only a small set of operational metadata columns exposed separately. This makes the service suitable as a generic persistence backend for many digital passport data models without requiring table redesign for each model family.

The current persistent structures are:

- `records`
- `idempotency_keys`
- `record_read_grants`

### `records`

Stores the record id, model, version, full payload JSON, creator identity, tenant identity, and timestamps.

### `idempotency_keys`

Stores short-lived replay-protection keys for `POST /records` so duplicate submissions within the active TTL can be rejected safely.

### `record_read_grants`

Stores explicit read grants used by storage-side access enforcement. This service enforces stored grants, but it does not expose an HTTP grant-management API.

## Access Control Model

Access enforcement currently combines:

- owner subject
- owner tenant
- explicit subject read grants
- explicit tenant read grants

The service stores and enforces these rules when reading or querying records.

Grant creation or governance workflows are outside the scope of this service.

## Backend Strategy

The current implementation supports three SQL backends:

- MySQL
- MariaDB
- PostgreSQL

The codebase separates:

- shared repository contract and record types
- MySQL/MariaDB SQL implementation
- PostgreSQL SQL implementation

This allows backend-specific SQL behavior where necessary without changing the external HTTP contract.

## Operational Design Choices

### Startup migrations

The service runs its migrations on startup. This keeps first deployment and normal restarts simple as long as migrations remain additive and non-destructive.

### Health and readiness separation

- `/health` answers whether the process is alive
- `/ready` answers whether the service can currently reach and use its database backend

This distinction matters for orchestrated deployments and container restarts.

### Auth modes

Normal operation uses JWT and JWKS validation.

A disabled auth mode exists for local development and testing only.
