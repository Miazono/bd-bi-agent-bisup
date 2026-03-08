# Data lineage

## Purpose

Этот документ показывает происхождение данных и зависимость таблиц между слоями.

## Source files

- `articles.csv`
- `customers.csv`
- `transactions_train.csv`

## Layer-by-layer lineage

### Articles flow

`articles.csv`
→ `raw.hm_articles_csv`
→ `bronze.hm_articles`
→ `silver.dim_article`
→ используется в:
- `mart.sales_monthly_category`
- `mart.repeat_purchase_category`

### Customers flow

`customers.csv`
→ `raw.hm_customers_csv`
→ `bronze.hm_customers`
→ `silver.dim_customer`
→ используется в:
- `mart.customer_segment_monthly`
- `mart.customer_rfm_monthly`

### Transactions flow

`transactions_train.csv`
→ `raw.hm_transactions_csv`
→ `bronze.hm_transactions`
→ `silver.fact_sales_line`
→ используется в:
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.customer_rfm_monthly`

### Derived aggregate flow

`silver.fact_sales_line`
→ `silver.fact_customer_article_stats`
→ `mart.repeat_purchase_category`

## Join logic

Основные связи модели:
- `silver.fact_sales_line.customer_id` → `silver.dim_customer.customer_id`
- `silver.fact_sales_line.article_id` → `silver.dim_article.article_id`
- `silver.fact_sales_line.sale_date` → `silver.dim_date.date_day`