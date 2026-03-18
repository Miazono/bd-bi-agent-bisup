# AGENTS.md — sql/queries/bronze

## Purpose
This directory contains SQL transformation logic used to load bronze tables from raw external tables.

## Scope
- `INSERT INTO ... SELECT ...` queries for bronze load
- type casting and null normalization close to source
- technical load columns such as `ingest_ts`, `source_file_name`, `batch_id`

## Rules
- Keep business-heavy logic out of bronze queries.
- Prefer one main load query per bronze table.
- Parameter placeholders should stay simple and explicit, for example `__BATCH_ID__`.
- Operational orchestration, small checks, and highly dynamic SQL should remain in Python loaders.
