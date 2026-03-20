# Каталог физических таблиц

> Этот файл сгенерирован скриптом `scripts/gen_schema.py` на основе DDL из `sql/ddl/`.
> Он описывает только физические таблицы Iceberg и не включает временные внешние таблицы `hive.raw.*`.

## Что входит в каталог

- схемы `bronze`, `silver` и `mart`;
- расположение таблиц в объектном хранилище;
- формат хранения и партиционирование;
- полный список столбцов и типов.

## Схема `bronze`

### `bronze.hm_articles`

- DDL: `sql/ddl/bronze/bronze_hm_articles.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/bronze/hm_articles/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `article_id` | `BIGINT` |
| `product_code` | `BIGINT` |
| `prod_name` | `VARCHAR` |
| `product_type_no` | `INTEGER` |
| `product_type_name` | `VARCHAR` |
| `product_group_name` | `VARCHAR` |
| `graphical_appearance_no` | `INTEGER` |
| `graphical_appearance_name` | `VARCHAR` |
| `colour_group_code` | `INTEGER` |
| `colour_group_name` | `VARCHAR` |
| `perceived_colour_value_id` | `INTEGER` |
| `perceived_colour_value_name` | `VARCHAR` |
| `perceived_colour_master_id` | `INTEGER` |
| `perceived_colour_master_name` | `VARCHAR` |
| `department_no` | `INTEGER` |
| `department_name` | `VARCHAR` |
| `index_code` | `VARCHAR` |
| `index_name` | `VARCHAR` |
| `index_group_no` | `INTEGER` |
| `index_group_name` | `VARCHAR` |
| `section_no` | `INTEGER` |
| `section_name` | `VARCHAR` |
| `garment_group_no` | `INTEGER` |
| `garment_group_name` | `VARCHAR` |
| `detail_desc` | `VARCHAR` |
| `ingest_ts` | `TIMESTAMP(6)` |
| `source_file_name` | `VARCHAR` |
| `batch_id` | `VARCHAR` |

### `bronze.hm_customers`

- DDL: `sql/ddl/bronze/bronze_hm_customers.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/bronze/hm_customers/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `customer_id` | `VARCHAR` |
| `fn` | `INTEGER` |
| `active` | `INTEGER` |
| `club_member_status` | `VARCHAR` |
| `fashion_news_frequency` | `VARCHAR` |
| `age` | `INTEGER` |
| `postal_code` | `VARCHAR` |
| `ingest_ts` | `TIMESTAMP(6)` |
| `source_file_name` | `VARCHAR` |
| `batch_id` | `VARCHAR` |

### `bronze.hm_transactions`

- DDL: `sql/ddl/bronze/bronze_hm_transactions.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/bronze/hm_transactions/`
- Партиционирование: `month(t_dat)`

| Столбец | Тип |
| --- | --- |
| `t_dat` | `DATE` |
| `customer_id` | `VARCHAR` |
| `article_id` | `BIGINT` |
| `price` | `DECIMAL(12,4)` |
| `sales_channel_id` | `INTEGER` |
| `ingest_ts` | `TIMESTAMP(6)` |
| `source_file_name` | `VARCHAR` |
| `batch_id` | `VARCHAR` |

## Схема `silver`

### `silver.dim_article`

- DDL: `sql/ddl/silver/silver_dim_article.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/silver/dim_article/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `article_id` | `BIGINT` |
| `product_code` | `BIGINT` |
| `prod_name` | `VARCHAR` |
| `product_type_no` | `INTEGER` |
| `product_type_name` | `VARCHAR` |
| `product_group_name` | `VARCHAR` |
| `graphical_appearance_no` | `INTEGER` |
| `graphical_appearance_name` | `VARCHAR` |
| `colour_group_code` | `INTEGER` |
| `colour_group_name` | `VARCHAR` |
| `perceived_colour_value_id` | `INTEGER` |
| `perceived_colour_value_name` | `VARCHAR` |
| `perceived_colour_master_id` | `INTEGER` |
| `perceived_colour_master_name` | `VARCHAR` |
| `department_no` | `INTEGER` |
| `department_name` | `VARCHAR` |
| `index_code` | `VARCHAR` |
| `index_name` | `VARCHAR` |
| `index_group_no` | `INTEGER` |
| `index_group_name` | `VARCHAR` |
| `section_no` | `INTEGER` |
| `section_name` | `VARCHAR` |
| `garment_group_no` | `INTEGER` |
| `garment_group_name` | `VARCHAR` |
| `detail_desc` | `VARCHAR` |
| `is_ladieswear` | `BOOLEAN` |
| `is_menswear` | `BOOLEAN` |
| `is_kids` | `BOOLEAN` |
| `color_family` | `VARCHAR` |

### `silver.dim_customer`

- DDL: `sql/ddl/silver/silver_dim_customer.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/silver/dim_customer/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `customer_id` | `VARCHAR` |
| `fn` | `INTEGER` |
| `active` | `INTEGER` |
| `club_member_status` | `VARCHAR` |
| `fashion_news_frequency` | `VARCHAR` |
| `age` | `INTEGER` |
| `postal_code` | `VARCHAR` |
| `age_band` | `VARCHAR` |
| `is_active_customer` | `BOOLEAN` |
| `is_fn_flag_present` | `BOOLEAN` |

### `silver.dim_date`

- DDL: `sql/ddl/silver/silver_dim_date.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/silver/dim_date/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `date_day` | `DATE` |
| `date_year` | `INTEGER` |
| `date_month` | `INTEGER` |
| `date_day_of_month` | `INTEGER` |
| `date_day_of_week` | `INTEGER` |
| `week_of_year` | `INTEGER` |

### `silver.fact_customer_article_stats`

- DDL: `sql/ddl/silver/silver_fact_customer_article_stats.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/silver/fact_customer_article_stats/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `customer_id` | `VARCHAR` |
| `article_id` | `BIGINT` |
| `first_purchase_date` | `DATE` |
| `last_purchase_date` | `DATE` |
| `purchase_cnt` | `BIGINT` |
| `total_revenue` | `DECIMAL(12,4)` |
| `avg_price` | `DECIMAL(12,4)` |

### `silver.fact_sales_line`

- DDL: `sql/ddl/silver/silver_fact_sales_line.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/silver/fact_sales_line/`
- Партиционирование: `month(sale_date)`

| Столбец | Тип |
| --- | --- |
| `sale_date` | `DATE` |
| `customer_id` | `VARCHAR` |
| `article_id` | `BIGINT` |
| `price` | `DECIMAL(12,4)` |
| `sales_channel_id` | `INTEGER` |
| `ingest_ts` | `TIMESTAMP(6)` |
| `source_file_name` | `VARCHAR` |
| `batch_id` | `VARCHAR` |

## Схема `mart`

### `mart.customer_rfm_monthly`

- DDL: `sql/ddl/mart/mart_customer_rfm_monthly.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/marts/customer_rfm_monthly/`
- Партиционирование: `month(snapshot_month)`

| Столбец | Тип |
| --- | --- |
| `customer_id` | `VARCHAR` |
| `snapshot_month` | `DATE` |
| `recency_days` | `INTEGER` |
| `frequency_365d` | `BIGINT` |
| `monetary_365d` | `DECIMAL(12,4)` |
| `rfm_segment` | `VARCHAR` |

### `mart.customer_segment_monthly`

- DDL: `sql/ddl/mart/mart_customer_segment_monthly.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/marts/customer_segment_monthly/`
- Партиционирование: `month(sale_month)`

| Столбец | Тип |
| --- | --- |
| `sale_month` | `DATE` |
| `customer_segment` | `VARCHAR` |
| `revenue` | `DECIMAL(12,4)` |
| `buyers_cnt` | `BIGINT` |
| `purchase_lines_cnt` | `BIGINT` |
| `revenue_per_buyer` | `DECIMAL(12,4)` |
| `avg_item_price` | `DECIMAL(12,4)` |

### `mart.repeat_purchase_category`

- DDL: `sql/ddl/mart/mart_repeat_purchase_category.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/marts/repeat_purchase_category/`
- Партиционирование: `не задано`

| Столбец | Тип |
| --- | --- |
| `category` | `VARCHAR` |
| `repeat_pairs_cnt` | `BIGINT` |
| `repeat_customers_cnt` | `BIGINT` |
| `avg_purchase_cnt` | `DECIMAL(12,4)` |
| `repeat_revenue` | `DECIMAL(12,4)` |

### `mart.sales_daily_channel`

- DDL: `sql/ddl/mart/mart_sales_daily_channel.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/marts/sales_daily_channel/`
- Партиционирование: `month(sale_date)`

| Столбец | Тип |
| --- | --- |
| `sale_date` | `DATE` |
| `sales_channel_id` | `INTEGER` |
| `revenue` | `DECIMAL(12,4)` |
| `items_sold` | `BIGINT` |
| `buyers_cnt` | `BIGINT` |
| `avg_item_price` | `DECIMAL(12,4)` |

### `mart.sales_monthly_category`

- DDL: `sql/ddl/mart/mart_sales_monthly_category.sql`
- Формат: `PARQUET`
- Расположение: `s3a://lakehouse/marts/sales_monthly_category/`
- Партиционирование: `month(sale_month)`

| Столбец | Тип |
| --- | --- |
| `sale_month` | `DATE` |
| `category` | `VARCHAR` |
| `revenue` | `DECIMAL(12,4)` |
| `items_sold` | `BIGINT` |
| `buyers_cnt` | `BIGINT` |
| `active_sku_cnt` | `BIGINT` |
