# Data schema

## Dataset overview

Проект использует H&M Fashion Recommendations dataset.

Основные входные источники:
- `articles.csv` — товарный справочник;
- `customers.csv` — клиентский справочник;
- `transactions_train.csv` — исторические транзакции.

## Raw layer

### `raw.hm_articles_csv`
- **Source:** `articles.csv`
- **Grain:** 1 строка = 1 строка исходного файла
- **Format:** CSV
- **Purpose:** сохранить оригинальный товарный справочник без изменений
- **Key business fields:** `article_id`, `product_code`, `prod_name`, `product_type_name`, `product_group_name`, `department_name`, `index_name`, `section_name`, `garment_group_name`

### `raw.hm_customers_csv`
- **Source:** `customers.csv`
- **Grain:** 1 строка = 1 строка исходного файла
- **Format:** CSV
- **Purpose:** сохранить оригинальный клиентский справочник без изменений
- **Key business fields:** `customer_id`, `fn`, `active`, `club_member_status`, `fashion_news_frequency`, `age`, `postal_code`

### `raw.hm_transactions_csv`
- **Source:** `transactions_train.csv`
- **Grain:** 1 строка = 1 строка исходного файла
- **Format:** CSV
- **Purpose:** сохранить оригинальные транзакции без изменений
- **Key business fields:** `t_dat`, `customer_id`, `article_id`, `price`, `sales_channel_id`

## Bronze layer

### `bronze.hm_articles`
- **Source:** `raw.hm_articles_csv`
- **Grain:** 1 строка = 1 article record из исходного файла
- **Primary business key:** `article_id`
- **Technical fields:** `ingest_ts`, `source_file_name`, `batch_id`
- **Partitioning:** none
- **Purpose:** технически нормализованный товарный справочник для lakehouse

### `bronze.hm_customers`
- **Source:** `raw.hm_customers_csv`
- **Grain:** 1 строка = 1 customer record из исходного файла
- **Primary business key:** `customer_id`
- **Technical fields:** `ingest_ts`, `source_file_name`, `batch_id`
- **Partitioning:** none
- **Purpose:** технически нормализованный клиентский справочник для lakehouse

### `bronze.hm_transactions`
- **Source:** `raw.hm_transactions_csv`
- **Grain:** 1 строка = 1 transaction line из исходного файла
- **Primary business grain:** `t_dat + customer_id + article_id + sales_channel_id`
- **Technical fields:** `ingest_ts`, `source_file_name`, `batch_id`
- **Partitioning:** `month(t_dat)`
- **Purpose:** технически нормализованный транзакционный факт

#### Planned typed fields for `bronze.hm_transactions`
- `t_dat` → `date`
- `customer_id` → `varchar`
- `article_id` → `bigint`
- `price` → `decimal(12,4)`
- `sales_channel_id` → `integer`

## Silver layer

### `silver.dim_article`
- **Source:** `bronze.hm_articles`
- **Grain:** 1 строка = 1 `article_id`
- **Primary key:** `article_id`
- **Purpose:** основной товарный справочник для аналитики и JOIN-ов
- **Typical derived fields:** `is_ladieswear`, `is_menswear`, `is_kids`, `color_family`

### `silver.dim_customer`
- **Source:** `bronze.hm_customers`
- **Grain:** 1 строка = 1 `customer_id`
- **Primary key:** `customer_id`
- **Purpose:** клиентский справочник и сегментация
- **Typical derived fields:** `age_band`, `is_active_customer`, `is_fn_flag_present`

### `silver.dim_date`
- **Source:** generated calendar table
- **Grain:** 1 строка = 1 календарная дата
- **Primary key:** `date_day`
- **Purpose:** календарное измерение для time analytics

### `silver.fact_sales_line`
- **Source:** `bronze.hm_transactions`
- **Grain:** 1 строка = 1 purchase line
- **Keys:** `sale_date`, `customer_id`, `article_id`
- **Partitioning:** `month(sale_date)`
- **Purpose:** главный факт продаж для построения витрин

### `silver.fact_customer_article_stats`
- **Source:** aggregate from `silver.fact_sales_line`
- **Grain:** 1 строка = `customer_id + article_id`
- **Purpose:** агрегат по повторным покупкам и customer-product behavior
- **Main fields:** `first_purchase_date`, `last_purchase_date`, `purchase_cnt`, `total_revenue`, `avg_price`

## Mart layer

### `mart.sales_daily_channel`
- **Grain:** 1 строка = `sale_date + sales_channel_id`
- **Purpose:** дневные продажи по каналу

### `mart.sales_monthly_category`
- **Grain:** 1 строка = `sale_month + category`
- **Purpose:** месячные продажи по товарным категориям

### `mart.customer_segment_monthly`
- **Grain:** 1 строка = `sale_month + customer_segment`
- **Purpose:** месячная аналитика по клиентским сегментам

### `mart.repeat_purchase_category`
- **Grain:** 1 строка = `category`
- **Purpose:** аналитика повторных покупок по категориям

### `mart.customer_rfm_monthly`
- **Grain:** 1 строка = `customer_id + snapshot_month`
- **Purpose:** RFM-профиль клиента на конец месяца