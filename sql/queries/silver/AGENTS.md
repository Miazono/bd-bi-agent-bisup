# AGENTS.md — sql/queries/silver

## Purpose
This directory contains SQL transformation logic used to build silver tables from bronze and reference tables.

## Scope
- `DELETE`, `INSERT`, and `MERGE` queries for silver refresh
- reusable analytical SQL with explicit table grain
- complex transformations that are easier to review as SQL than as Python strings

## Rules
- Keep orchestration, chunk routing, and small checks in Python.
- Prefer simple placeholder tokens such as `__BATCH_ID__`, `__PREFIX_LEN__`, `__PREFIX_VALUE__`.
- Keep one main logical step per file when practical.
- If physical schema changes, update `sql/ddl/silver/` and docs together.
