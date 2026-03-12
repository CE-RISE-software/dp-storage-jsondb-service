#!/usr/bin/env bash
set -euo pipefail

project_name="dp-storage-jsondb-mysql-test"

docker compose -p "${project_name}" -f docker-compose.mysql.yml down -v >/dev/null 2>&1 || true
docker compose -p "${project_name}" -f docker-compose.mysql.yml up -d
trap 'docker compose -p "${project_name}" -f docker-compose.mysql.yml down -v' EXIT

container_id="$(docker compose -p "${project_name}" -f docker-compose.mysql.yml ps -q | head -n 1)"
if [ -z "${container_id}" ]; then
  echo "mysql container id not found"
  exit 1
fi

for _ in $(seq 1 60); do
  health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
  if [ "${health}" = "healthy" ] || [ "${health}" = "running" ]; then
    break
  fi
  sleep 2
done

health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
if [ "${health}" != "healthy" ] && [ "${health}" != "running" ]; then
  echo "mysql container did not become ready: ${health}"
  docker logs "${container_id}" || true
  exit 1
fi

export TEST_DB_HOST=127.0.0.1
export TEST_DB_PORT=3306
export TEST_DB_NAME=dp_storage_test
export TEST_DB_USER=dp_storage
export TEST_DB_PASSWORD=dp_storage

cargo test --test integration_db
