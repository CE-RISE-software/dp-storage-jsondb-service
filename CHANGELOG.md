# Changelog

All notable changes to the CE-RISE `dp-storage-jsondb` project will be documented in this file.

## [0.0.1] - 2026-03-12

### Added
- Initial Rust implementation of the `dp-storage-jsondb` storage backend service
- HTTP contract compatible with the `hex-core-service` `io-http` adapter
- Record persistence and retrieval with full JSON payload storage
- Canonical `/records/query` support with payload-path filtering, sorting, limit, and offset
- JWT/JWKS authentication with `scope` and `scp` claim support
- Disabled auth mode for local development and testing
- Idempotency-key enforcement for `POST /records`
- Health, readiness, and OpenAPI endpoints
- Storage-side ownership, tenant visibility, and read-grant enforcement
- Database backend support for MySQL, MariaDB, and PostgreSQL
- Backend-specific migrations and deployment compose files
- Rust test suite plus local live-database integration scripts for all supported backends
- mdBook-based service documentation and Codeberg Pages publishing workflow
