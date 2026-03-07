# AGENTS.md — infra

## Purpose
Infrastructure for the local Data Lakehouse stack:
MinIO, Hive Metastore, metastore DB, Trino, WrenAI.

## Scope
These instructions apply only to files under `infra/`.

## Commands
- Start stack: `docker compose -f infra/docker-compose.yml up -d`
- Stop stack: `docker compose -f infra/docker-compose.yml down`
- Logs: `docker compose -f infra/docker-compose.yml logs <service>`
- Recreate service: `docker compose -f infra/docker-compose.yml up -d --force-recreate <service>`

## Rules
- Keep local developer experience stable.
- Do not hardcode secrets.
- Preserve existing bucket names unless the task explicitly requires migration.
- If infra config changes, update `README.md`, `ARCHITECTURE.md`, and `.env.example`.

## Allowed without extra confirmation
- Adjust existing service environment variables.
- Fix broken mounts or service wiring.
- Update WrenAI/Trino config for already approved project requirements.

## Ask first
- Add/remove services.
- Change host ports, persistent volume paths, Docker networks, service names, or bucket names.
- Change major image versions.
- Make destructive changes that can invalidate local data.

## Never
- Commit real credentials.
- Introduce breaking infra changes without documenting migration steps.