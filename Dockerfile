FROM rust:1.93 AS builder
WORKDIR /app

COPY Cargo.toml Cargo.toml
COPY src src
COPY migrations migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN useradd --system --create-home appuser
WORKDIR /app
COPY --from=builder /app/target/release/dp-storage-jsondb-service /usr/local/bin/dp-storage-jsondb-service
USER appuser
EXPOSE 8080
CMD ["dp-storage-jsondb-service"]
