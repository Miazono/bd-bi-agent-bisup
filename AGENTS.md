# AGENTS.md — repository-wide instructions

## Project
Учебный групповой проект: Data Lakehouse + SQL BI Agent.

Target stack:
- MinIO
- Hive Metastore
- Apache Iceberg
- Trino
- WrenAI
- Python 3.11+

## Scope
These instructions apply to the whole repository unless a deeper `AGENTS.md` overrides them.

## Source of truth
Before making changes, read files in this order:
1. `README.md` — project overview and current scope
2. `ARCHITECTURE.md` — target architecture and layer model
3. `docs/data/schema.md` — table catalog, grains, and layer structure
4. `docs/data/catalog_generated.md` — schema from Trino metadata
5. `docs/data/marts.md` — analytical marts and BI-facing logic
6. the closest local `AGENTS.md` in the directory you work in

If documents conflict, prefer the most specific file for that scope.

## Commands
- Start stack: `docker compose up -d`
- Stop stack: `docker compose down`
- Run raw ingestion: `python ingestion/load_raw.py`
- Run bronze load: `python ingestion/load_bronze.py`
- Run silver load: `python ingestion/load_silver.py`
- Run tests: `pytest tests/ -v`
- Lint: `ruff check . && ruff format .`

## Working rules
- Keep diffs minimal and scoped to the task.
- Do not perform broad refactors unless explicitly requested.
- Run relevant checks after changes when possible.
- If architecture, configs, env vars, data model, or commands change, update docs in the same task.
- Treat this repository as a project under active design: not every planned component is fully implemented yet.

## Data model conventions
- The target layer model is: `raw -> bronze -> silver -> marts -> BI agent`
- Use the H&M dataset as the primary reference dataset.
- `silver.fact_customer_period_stats` is out of scope and should not be introduced.
- `silver.fact_customer_article_stats` is allowed as a derived silver aggregate.
- Primary BI / semantic exposure should be built on marts, not on bronze tables.

## Always
- Read any files in the repository.
- Add new files in `ingestion/`, `sql/`, `bi-agent/`, `tests/`, `docs/`, and `scripts/`.
- Update `.env.example` when adding new environment variables.
- Update relevant docs when changing data model, file names, commands, or architecture.

## Ask first
- Change infrastructure files unless the task explicitly concerns infrastructure.
- Change existing table grains, business keys, or semantic definitions.
- Add new external dependencies or new Docker services.
- Change prompts or semantic layer unless the task is about BI-agent quality or behavior.

## Never
- Edit `.env`.
- Commit real secrets or credentials.
- Commit large local datasets from `data/raw/`.
- Remove tests or reduce coverage without explicit reason.

## Notes
- `docs/data/schema.md` may be edited manually until automated schema generation is implemented.
- Once `scripts/gen_schema.py` becomes the source of truth, this rule can be tightened.
