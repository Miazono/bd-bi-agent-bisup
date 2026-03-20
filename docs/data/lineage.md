# Происхождение данных

## Назначение

Этот документ показывает происхождение данных и зависимость таблиц между слоями.

## Исходные файлы

- `articles.csv`
- `customers.csv`
- `transactions_train.csv`

## Поток по слоям

### Поток товаров

`articles.csv`
→ `s3a://lakehouse/raw/hm/articles/load_date=YYYY-MM-DD/articles.csv`
→ `hive.raw.hm_articles_raw`
→ `iceberg.bronze.hm_articles`
→ `iceberg.silver.dim_article`
→ используется в:

- `iceberg.mart.sales_monthly_category`
- `iceberg.mart.repeat_purchase_category`

### Поток клиентов

`customers.csv`
→ `s3a://lakehouse/raw/hm/customers/load_date=YYYY-MM-DD/customers.csv`
→ `hive.raw.hm_customers_raw`
→ `iceberg.bronze.hm_customers`
→ `iceberg.silver.dim_customer`
→ используется в:

- `iceberg.mart.customer_segment_monthly`

### Поток транзакций

`transactions_train.csv`
→ `s3a://lakehouse/raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv`
→ `hive.raw.hm_transactions_raw`
→ `iceberg.bronze.hm_transactions`
→ `iceberg.silver.fact_sales_line`
→ используется в:

- `iceberg.mart.sales_daily_channel`
- `iceberg.mart.sales_monthly_category`
- `iceberg.mart.customer_segment_monthly`
- `iceberg.mart.customer_rfm_monthly`

### Поток производного агрегата

`iceberg.silver.fact_sales_line`
→ `iceberg.silver.fact_customer_article_stats`
→ `iceberg.mart.repeat_purchase_category`

## Примечания по обновлению

- Временные таблицы `hive.raw.*` создаются на этапе загрузки bronze и читают директории raw-файлов как внешний CSV-источник.
- Для нового `batch_id` агрегат `iceberg.silver.fact_customer_article_stats` обновляется через `MERGE`.
- Для повторной загрузки уже существующего `batch_id` пересобираются только затронутые префиксы `customer_id`.
- Витрины слоя `marts` в текущей версии пересобираются полностью из `silver`.

## Основные связи

- `iceberg.silver.fact_sales_line.customer_id` → `iceberg.silver.dim_customer.customer_id`
- `iceberg.silver.fact_sales_line.article_id` → `iceberg.silver.dim_article.article_id`
- `iceberg.silver.fact_sales_line.sale_date` → `iceberg.silver.dim_date.date_day`
