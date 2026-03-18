# Analytical marts

## Purpose

Mart-слой предназначен для:
- стабильной бизнес-аналитики поверх silver-таблиц;
- упрощения SQL для Trino;
- публикации понятных сущностей и метрик для BI-агента.

Primary semantic exposure для BI-agent планируется строить именно на marts.

В текущей реализации marts материализуются как физические Iceberg-таблицы в схеме `mart`.
Обновление выполняется через полный rebuild из silver-слоя.
DDL mart-таблиц хранится в `sql/ddl/mart/`, а SQL-логика наполнения — в `sql/queries/mart/`.
Для единообразия query-файлы mart теперь тоже содержат полноценные исполняемые `INSERT INTO ... SELECT ...`, а не только `SELECT`.

---

## `mart.sales_daily_channel`

### Purpose
Показывает дневные продажи по каналам продаж.

### Grain
1 строка = `sale_date + sales_channel_id`

### Sources
- `silver.fact_sales_line`

### Metrics
- `revenue`
- `items_sold`
- `buyers_cnt`
- `avg_item_price`

### Notes
`items_sold` в v1 интерпретируется как количество transaction lines, а не как количество единиц в заказе.

### Example BI questions
- Show daily revenue by sales channel for the last 60 days
- Which sales channel had more buyers last month?
- How did average item price change by channel?

---

## `mart.sales_monthly_category`

### Purpose
Показывает динамику продаж по товарным категориям по месяцам.

### Grain
1 строка = `sale_month + category`

### Sources
- `silver.fact_sales_line`
- `silver.dim_article`

### Metrics
- `revenue`
- `items_sold`
- `buyers_cnt`
- `active_sku_cnt`

### Example BI questions
- Top product groups by revenue in September
- Which garment groups grew month over month?
- Show monthly revenue by index group

---

## `mart.customer_segment_monthly`

### Purpose
Показывает ценность клиентских сегментов по месяцам.

### Grain
1 строка = `sale_month + customer_segment`

### Sources
- `silver.fact_sales_line`
- `silver.dim_customer`

### Metrics
- `revenue`
- `buyers_cnt`
- `purchase_lines_cnt`
- `revenue_per_buyer`
- `avg_item_price`

### Example BI questions
- Do club members spend more than non-members?
- Revenue per buyer by age band
- Which fashion-news segment buys most often?

---

## `mart.repeat_purchase_category`

### Purpose
Показывает повторные покупки по товарным категориям.

### Grain
1 строка = `category`

### Sources
- `silver.fact_customer_article_stats`
- `silver.dim_article`

### Metrics
- `repeat_pairs_cnt`
- `repeat_customers_cnt`
- `avg_purchase_cnt`
- `repeat_revenue`

### Example BI questions
- Which garment groups have the highest repeat purchase rate?
- Show product groups with the strongest repeat behavior
- What categories are most re-purchased by the same customers?

---

## `mart.customer_rfm_monthly`

### Purpose
Показывает RFM-профиль клиентов по месяцам.

### Grain
1 строка = `customer_id + snapshot_month`

### Sources
- `silver.fact_sales_line`
- `silver.dim_customer`

### Metrics
- `recency_days`
- `frequency_365d`
- `monetary_365d`
- `rfm_segment`

### Example BI questions
- How many champions do we have by month?
- Which age bands contain the most at-risk customers?
- Show revenue share by RFM segment
