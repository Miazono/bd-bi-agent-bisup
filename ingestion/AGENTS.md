# AGENTS.md — ingestion

## Purpose
This directory contains ingestion scripts for the H&M dataset and related helpers.

The target ingestion flow is:
`raw -> bronze -> silver`

## Source of truth
Before changing ingestion logic, read:
1. `ARCHITECTURE.md`
2. `docs/data/schema.md`
3. this file

## Expected scripts
- `load_raw.py` — load original source files into MinIO raw storage without business transformations
- `load_bronze.py` — build technical Iceberg bronze tables close to source structure
- `load_silver.py` — build cleaned analytical silver tables from bronze

## Layer rules

### Raw
- Preserve source files as-is.
- Do not rename business fields in raw storage.
- Store source metadata when practical: file name, load timestamp, checksum, batch id.

### Bronze
- Bronze tables should stay close to the original source shape.
- Allowed operations:
  - type casting
  - null normalization
  - technical load columns
  - basic DQ checks
- Do not introduce business-heavy aggregations in bronze.

### Silver
- Silver tables should represent cleaned analytical entities.
- Allowed operations:
  - standardization
  - deduplication
  - derived business flags
  - analytical fact / dimension modeling
- Silver should be built only from bronze or generated reference tables such as `dim_date`.

## Planned tables

### Bronze
- `bronze.hm_articles`
- `bronze.hm_customers`
- `bronze.hm_transactions`

### Silver
- `silver.dim_article`
- `silver.dim_customer`
- `silver.dim_date`
- `silver.fact_sales_line`
- `silver.fct_customer_article_stats`

Do not create:
- `silver.fct_customer_period_stats`

## Quality rules
- Keep table grain explicit in code comments or SQL.
- Add or preserve technical metadata fields when relevant.
- Avoid silently changing table names, grains, or partitioning rules without updating docs.

## Update docs together
If ingestion logic changes, also update:
- `ARCHITECTURE.md`
- `docs/data/schema.md`
- `docs/data/lineage.md` if lineage changes