# AGENTS.md — sql/queries/mart

## Purpose
This directory contains SQL transformation logic used to populate mart tables.

Marts are the primary BI-facing layer for Trino and the BI agent.

## Source of truth
Before editing mart queries, read:
1. `docs/data/schema.md`
2. `docs/data/marts.md`
3. this file

## Naming conventions
- Prefer one SQL file per mart.
- Prefer consistent names such as:
  - `mart_sales_daily_channel.sql`
  - `mart_sales_monthly_category.sql`
  - `mart_customer_segment_monthly.sql`
  - `mart_repeat_purchase_category.sql`
  - `mart_customer_rfm_monthly.sql`

## Mart design rules
- Every mart must have an explicitly documented grain.
- Every mart must answer a clear business question.
- Every mart must be based on silver tables, not bronze.
- Prefer stable, BI-friendly field names and metrics.
- Avoid mixing multiple unrelated business questions in one mart.

## Planned marts
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.repeat_purchase_category`
- `mart.customer_rfm_monthly`

## Metric rules
- Use metric names that are understandable for BI and NL2SQL.
- Document ambiguous metrics explicitly.
- For this dataset, `items_sold` should mean transaction line count unless a different definition is explicitly documented.

## Update docs together
If a mart is added, removed, renamed, or its grain changes, also update if needs:
- `docs/data/schema.md`
- `docs/data/marts.md`
- `docs/data/lineage.md` if lineage changes
