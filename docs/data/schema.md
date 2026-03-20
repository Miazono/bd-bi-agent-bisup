# Схема данных

## Как читать этот документ

Этот документ описывает логические слои данных и ключевые характеристики таблиц:

- источник данных;
- grain;
- ключевые поля;
- назначение;
- правила обновления.

Полный перечень столбцов и типов находится в `docs/data/catalog_generated.md`.

## Исходные файлы

Проект использует три исходных CSV-файла датасета H&M:

- `articles.csv` — справочник товаров;
- `customers.csv` — справочник клиентов;
- `transactions_train.csv` — исторические транзакции продаж.

## Raw

### Хранение файлов

Слой `raw` хранится в MinIO как исходные CSV-файлы без бизнес-преобразований:

- `s3a://lakehouse/raw/hm/articles/load_date=YYYY-MM-DD/articles.csv`;
- `s3a://lakehouse/raw/hm/customers/load_date=YYYY-MM-DD/customers.csv`;
- `s3a://lakehouse/raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv`.

### Временные внешние таблицы

Для загрузки в bronze создаются временные внешние таблицы в каталоге `hive` и схеме `raw`.

#### `hive.raw.hm_articles_raw`

- Источник: директория `raw/hm/articles/load_date=.../`
- Grain: 1 строка = 1 строка исходного файла
- Формат: CSV
- Назначение: техническое чтение файла `articles.csv` перед загрузкой в bronze

#### `hive.raw.hm_customers_raw`

- Источник: директория `raw/hm/customers/load_date=.../`
- Grain: 1 строка = 1 строка исходного файла
- Формат: CSV
- Назначение: техническое чтение файла `customers.csv` перед загрузкой в bronze

#### `hive.raw.hm_transactions_raw`

- Источник: директория `raw/hm/transactions_train/load_date=.../`
- Grain: 1 строка = 1 строка исходного файла
- Формат: CSV
- Назначение: техническое чтение файла `transactions_train.csv` перед загрузкой в bronze

## Bronze

### `iceberg.bronze.hm_articles`

- Источник: `hive.raw.hm_articles_raw`
- Grain: 1 строка = 1 товарная запись
- Бизнес-ключ: `article_id`
- Технические поля: `ingest_ts`, `source_file_name`, `batch_id`
- Партиционирование: не задано
- Назначение: технически нормализованный справочник товаров, максимально близкий к источнику

### `iceberg.bronze.hm_customers`

- Источник: `hive.raw.hm_customers_raw`
- Grain: 1 строка = 1 клиентская запись
- Бизнес-ключ: `customer_id`
- Технические поля: `ingest_ts`, `source_file_name`, `batch_id`
- Партиционирование: не задано
- Назначение: технически нормализованный справочник клиентов, максимально близкий к источнику

### `iceberg.bronze.hm_transactions`

- Источник: `hive.raw.hm_transactions_raw`
- Grain: 1 строка = 1 строка покупки
- Бизнес-grain: `t_dat + customer_id + article_id + sales_channel_id`
- Технические поля: `ingest_ts`, `source_file_name`, `batch_id`
- Партиционирование: `month(t_dat)`
- Назначение: технически нормализованный факт транзакций

## Silver

### `iceberg.silver.dim_article`

- Источник: `iceberg.bronze.hm_articles`
- Grain: 1 строка = 1 `article_id`
- Первичный ключ: `article_id`
- Производные поля: `is_ladieswear`, `is_menswear`, `is_kids`, `color_family`
- Назначение: основной товарный справочник для аналитики и соединений

### `iceberg.silver.dim_customer`

- Источник: `iceberg.bronze.hm_customers`
- Grain: 1 строка = 1 `customer_id`
- Первичный ключ: `customer_id`
- Производные поля: `age_band`, `is_active_customer`, `is_fn_flag_present`
- Назначение: основной клиентский справочник для аналитики

### `iceberg.silver.dim_date`

- Источник: календарь, генерируемый из данных bronze
- Grain: 1 строка = 1 календарная дата
- Первичный ключ: `date_day`
- Назначение: календарное измерение для аналитики по времени

### `iceberg.silver.fact_sales_line`

- Источник: `iceberg.bronze.hm_transactions`
- Grain: 1 строка = 1 строка покупки
- Ключевые поля: `sale_date`, `customer_id`, `article_id`, `sales_channel_id`
- Партиционирование: `month(sale_date)`
- Назначение: основной факт продаж для построения витрин
- Стратегия обновления: пересборка только затронутых месячных частей для выбранного `batch_id`

### `iceberg.silver.fact_customer_article_stats`

- Источник: агрегат поверх `iceberg.silver.fact_sales_line`
- Grain: 1 строка = `customer_id + article_id`
- Основные поля: `first_purchase_date`, `last_purchase_date`, `purchase_cnt`, `total_revenue`, `avg_price`
- Назначение: производный агрегат для анализа повторных покупок
- Стратегия обновления:
  - новый `batch_id` обновляется через `MERGE`;
  - повторная загрузка уже существующего `batch_id` использует безопасную частичную пересборку по префиксам `customer_id`

## Marts

Логический слой называется `marts`, а физически витрины лежат в схеме `iceberg.mart`.

### `iceberg.mart.sales_daily_channel`

- Источник: `iceberg.silver.fact_sales_line`
- Grain: 1 строка = `sale_date + sales_channel_id`
- Партиционирование: `month(sale_date)`
- Назначение: дневные продажи по каналу продаж

### `iceberg.mart.sales_monthly_category`

- Источник: `iceberg.silver.fact_sales_line`, `iceberg.silver.dim_article`
- Grain: 1 строка = `sale_month + category`
- Партиционирование: `month(sale_month)`
- Назначение: месячные продажи по товарным категориям
- Примечание: `category` заполняется из `product_group_name`

### `iceberg.mart.customer_segment_monthly`

- Источник: `iceberg.silver.fact_sales_line`, `iceberg.silver.dim_customer`
- Grain: 1 строка = `sale_month + customer_segment`
- Партиционирование: `month(sale_month)`
- Назначение: месячная аналитика по клиентским сегментам
- Примечание: текущий `customer_segment` строится как `COALESCE(club_member_status, 'unknown')`

### `iceberg.mart.repeat_purchase_category`

- Источник: `iceberg.silver.fact_customer_article_stats`, `iceberg.silver.dim_article`
- Grain: 1 строка = `category`
- Партиционирование: не задано
- Назначение: аналитика повторных покупок по товарным категориям
- Примечание: в витрину попадают только пары с `purchase_cnt > 1`

### `iceberg.mart.customer_rfm_monthly`

- Источник: `iceberg.silver.fact_sales_line`
- Grain: 1 строка = `customer_id + snapshot_month`
- Партиционирование: `month(snapshot_month)`
- Назначение: RFM-профиль клиента на конец месяца
- Примечание: `snapshot_month` — последний календарный день месяца, для которого в данных есть продажи
