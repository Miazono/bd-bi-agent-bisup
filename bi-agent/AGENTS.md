# AGENTS.md — bi-agent

## Purpose
This directory contains the BI-agent layer:
- semantic layer definitions
- prompts
- evaluation assets
- helper scripts related to NL2SQL quality

## Source of truth
Before changing BI-agent logic, read:
1. `docs/data/schema.md`
2. `docs/data/marts.md`
3. `ARCHITECTURE.md`
4. this file

## Exposure rules
- Primary semantic exposure should be based on marts.
- Silver tables may be exposed only when needed for explanation or advanced modeling.
- Bronze tables must not be exposed to the BI-agent as primary semantic entities.

## Preferred exposed entities
Expose first:
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.repeat_purchase_category`
- `mart.customer_rfm_monthly`

Expose selectively if needed:
- `silver.dim_article`
- `silver.dim_customer`
- `silver.fact_sales_line`

## Prompt rules
- Prefer prompts that guide the agent toward marts first.
- Prefer explicit metric definitions when the dataset is ambiguous.
- Keep prompts aligned with documented grains and metric meanings.

## Evaluation rules
- If semantic layer changes, update evaluation questions.
- If a mart is renamed or removed, update eval assets in the same task.
- Prefer evaluation questions that check both SQL correctness and table selection.

## Update docs together
If semantic entities, prompts, or BI-facing model names change, also update:
- `docs/data/marts.md`
- `docs/data/schema.md`
- `ARCHITECTURE.md` if architectural intent changes