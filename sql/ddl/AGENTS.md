# AGENTS.md — sql/ddl

## Purpose
This directory contains SQL definitions for physical lakehouse tables.

Scope of this directory:
- table creation SQL
- CTAS / DDL-like table build statements
- physical schema definitions for bronze and silver layers

This directory is not the primary place for BI-facing marts.

## Source of truth
Before editing files in this directory, read:
1. `ARCHITECTURE.md`
2. `docs/data/schema.md`
3. the closest `AGENTS.md`
4. this file

## Directory intent
- `bronze/` — DDL or CTAS definitions for bronze tables
- `silver/` — DDL or CTAS definitions for silver tables

If marts are stored separately, they should live in `sql/marts/` or `marts/`, not here.

## Rules
- Keep one table definition per file when practical.
- File names should clearly map to table names.
- Prefer stable, explicit column names and types.
- Keep table grain explicit in comments or file headers.
- Keep physical modeling concerns here; keep BI-facing aggregation logic out of this directory.

## Layer rules

### Bronze
- Stay close to source structure.
- Allow type normalization and technical metadata fields.
- Avoid business-heavy transformations.

### Silver
- Represent cleaned analytical entities.
- Allow deduplication, standardization, derived business flags, and fact/dimension modeling.
- Keep silver reusable across multiple marts when possible.

## Current planned tables

### Bronze
- `bronze.hm_articles`
- `bronze.hm_customers`
- `bronze.hm_transactions`

### Silver
- `silver.dim_article`
- `silver.dim_customer`
- `silver.dim_date`
- `silver.fact_sales_line`
- `silver.fact_customer_article_stats`

## Update docs together
If a table name, grain, partitioning rule, or physical schema changes, also update:
- `docs/data/schema.md`
- `docs/data/lineage.md` if lineage changes
- `ARCHITECTURE.md` if architectural intent changes