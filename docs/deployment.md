# Deployment

## Overview

The service is deployed as its own container and connects to an external SQL database backend.

It does not embed the database server inside the service image.

That means a normal deployment consists of:

- one `dp-storage-jsondb-service` container
- one SQL backend instance

## Container Deployment Model

The service image is backend-agnostic at the container level.

Database selection is made through environment configuration, primarily:

- `DB_BACKEND`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`

## Supported Compose Deployments

Canonical deployment-oriented compose files are provided for each supported backend:

- `docker-compose.mysql.yml`
- `docker-compose.mariadb.yml`
- `docker-compose.postgres.yml`

These files pair the service container with the matching database container.

## MySQL Deployment

Use the MySQL compose file when you want the storage backend to run against MySQL.

Key characteristics:

- `DB_BACKEND=mysql`
- MySQL service image
- MySQL-oriented health check

## MariaDB Deployment

Use the MariaDB compose file when you want the storage backend to run against MariaDB.

Key characteristics:

- `DB_BACKEND=mariadb`
- MariaDB service image
- MariaDB-oriented health check

## PostgreSQL Deployment

Use the PostgreSQL compose file when you want the storage backend to run against PostgreSQL.

Key characteristics:

- `DB_BACKEND=postgres`
- PostgreSQL service image
- PostgreSQL-oriented health check

## Required Deployment Inputs

Before starting a deployment, replace placeholder values for:

- database password
- root or admin password where required by the compose stack
- auth issuer, audience, and JWKS URL
- image tag if you are deploying a specific released version

## Startup Sequence

A normal startup sequence is:

1. database container becomes healthy
2. service container starts
3. service runs backend-specific migrations
4. service begins accepting HTTP traffic
5. readiness succeeds when DB connectivity is confirmed

## Networking

The service must be able to reach the database hostname configured through `DB_HOST`.

In compose deployments this is typically the service name of the paired database container.

## Single Image Principle

The service image remains the same regardless of backend.

Backend-specific differences belong in:

- environment variables
- compose manifests
- database server runtime

not in separate application builds.
