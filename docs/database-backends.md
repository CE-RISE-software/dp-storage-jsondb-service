# Database Backends

## Overview

`dp-storage-jsondb` currently supports three SQL backends:

- MySQL
- MariaDB
- PostgreSQL

The external HTTP contract is the same for all of them.

The internal SQL implementation differs where required for:

- connection setup
- migrations
- JSON extraction
- JSON containment
- type handling

## MySQL

### Support status

Supported.

### Notes

MySQL provides native JSON support and is one of the reference backends for this service.

The service uses backend-specific SQL for:

- `JSON_EXTRACT`
- `JSON_UNQUOTE`
- `JSON_CONTAINS`

## MariaDB

### Support status

Supported.

### Notes

MariaDB is supported explicitly and is tested separately from MySQL.

Even where MariaDB appears similar to MySQL at the schema level, JSON behavior is not identical under the hood, so this backend is treated as a real compatibility target rather than assumed to behave exactly like MySQL.

## PostgreSQL

### Support status

Supported.

### Notes

PostgreSQL uses a separate SQL implementation and migration path.

Its JSON behavior is implemented through PostgreSQL JSONB operators and functions rather than MySQL-style JSON functions.

This backend is now part of the supported runtime surface and is exercised through the local live-database integration path.

## Migrations By Backend

The service keeps backend-specific migration directories:

- `migrations/mysql`
- `migrations/postgres`

The MySQL migration path is used for both MySQL and MariaDB.

## Why Backend-Specific SQL Exists

A fully generic SQL layer would hide too much of the real behavior that matters here.

This service needs correct JSON extraction, containment, sorting, and comparison behavior on multiple engines. Those features are not identical across SQL backends.

The implementation therefore uses:

- a shared repository contract
- one SQL repository for MySQL and MariaDB
- one SQL repository for PostgreSQL

That is a more honest design than pretending one SQL text generator can safely cover all backends with no differences.
