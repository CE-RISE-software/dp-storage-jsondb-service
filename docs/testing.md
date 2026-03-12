# Testing

## Overview

The service uses two testing layers:

- Rust-only tests that do not require a live database
- local live-database integration tests against real SQL containers

This separation is deliberate.

## Rust Test Suite

Run the normal Rust test suite with:

```bash
cargo test
```

This covers:

- query logic
- contract behavior at the HTTP layer
- auth behavior
- disabled-auth behavior
- in-memory repository behavior
- integration test harness logic when no live DB is configured

## Local Live-Database Integration Tests

The repository SQL implementations are verified locally against real database containers.

### MySQL

```bash
bash scripts/test-mysql.sh
```

### MariaDB

```bash
bash scripts/test-mariadb.sh
```

### PostgreSQL

```bash
bash scripts/test-postgres.sh
```

These scripts:

- start the matching test database container
- wait for the backend to become ready
- set `TEST_DB_*` environment variables
- run `cargo test --test integration_db`
- tear the database stack down afterward

## What The Integration Test Covers

The live integration test verifies backend behavior for:

- schema migrations
- record creation
- record retrieval
- idempotency conflict handling
- canonical query execution
- payload containment and payload path queries
- sorting, limit, and offset behavior
- tenant and ownership visibility behavior
- explicit read grants

## Why Live Backend Testing Matters

JSON behavior and SQL expression details differ across backends.

The service therefore does not rely only on mocked repository tests. Real-engine checks are necessary to verify that the generated SQL and migration assumptions actually work on the supported databases.

## CI Boundary

The CI workflow runs the Rust test suite on every push.

Live database containers are intended for local verification in the current workflow model.
