# AGENTS.md — repository-wide instructions

## Project
Учебный групповой проект: Data Lakehouse + SQL BI Agent.
Stack: MinIO, Hive Metastore, Apache Iceberg, Trino, WrenAI, Python 3.11+.

## Scope
These instructions apply to the whole repository unless a deeper AGENTS.md overrides them.

## Commands
- Start stack: `docker compose docker-compose.yml up -d`
- Stop stack: `docker compose docker-compose.yml down`
- Run raw ingestion: `python ingestion/load_raw.py`
- Run Bronze load: `python ingestion/load_bronze.py`
- Run Silver load: `python ingestion/load_silver.py`
- Run tests: `pytest tests/ -v`
- Lint: `ruff check . && ruff format .`

## Working rules
- Read `README.md`, `ARCHITECTURE.md`, and the closest `AGENTS.md` before making changes.
- Keep diffs minimal and scoped to the task.
- Do not perform broad refactors unless explicitly requested.
- Run relevant tests/checks after changes.
- If commands, configs, env vars, or architecture change, update docs in the same task.

## Always
- Read any files in the repository.
- Add new files in `ingestion/`, `marts/`, `bi-agent/`, `tests/`, `docs/`.
- Update `.env.example` when adding new environment variables.
- Check and update `\doc` files or add new if nessesarily after changes

## Ask first
- Change files in `infra/` unless the task explicitly concerns infrastructure.
- Change existing Iceberg table schemas or column types.
- Add new external dependencies or new Docker services.
- Change prompts / semantic layer unless the task is about BI-agent quality or behavior.

## Never
- Edit `.env`.
- Edit `docs/data/schema.md` manually; regenerate it via `python scripts/gen_schema.py`.
- Commit large local datasets from `data/raw/`.
- Remove tests or reduce coverage without explicit reason.