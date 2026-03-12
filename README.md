# dp-storage-jsondb-service

Standalone HTTP storage backend for CE-RISE `hex-core-service`.

This service is intended to be used through the `io-http` adapter in `hex-core`. It stores full record payloads as JSON documents in MariaDB or MySQL and exposes the backend contract expected by `hex-core`:

- `POST /records`
- `GET /records/{id}`
- `POST /records/query`
- `GET /health`
- `GET /ready`
- `GET /openapi.json`

## Backend support

Supported backends in v1 are MariaDB and MySQL.

The service image runs alongside an external SQL server.

## Development

Use the example environment file as a starting point:

```bash
cp .env.example .env
```

Run the local test suite:

```bash
cargo test
```

Start the service:

```bash
cargo run
```

## License

Licensed under the [European Union Public Licence v1.2 (EUPL-1.2)](LICENSE).

## Contributing

This repository is maintained on Codeberg. The GitHub repository is a mirror for release and archival workflows.
