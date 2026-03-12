#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker-compose.mysql.yml up -d
trap 'docker compose -f docker-compose.mysql.yml down -v' EXIT

until docker compose -f docker-compose.mysql.yml ps --format json | grep -q '"health":"healthy"'; do
  sleep 2
done

export TEST_DB_HOST=127.0.0.1
export TEST_DB_PORT=3306
export TEST_DB_NAME=dp_storage_test
export TEST_DB_USER=dp_storage
export TEST_DB_PASSWORD=dp_storage

cargo test --test integration_db
