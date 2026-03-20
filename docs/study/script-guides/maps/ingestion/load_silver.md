# `load_silver.py` в краткой схеме

## Назначение

Оркестрация слоя `silver`: проверка batch, создание таблиц, обновление размерностей, пересборка факта продаж и агрегата повторных покупок.

## Входы

- `--batch-id`
- `--stats-prefix-len`
- `--months`
- `--skip-stats`
- данные из `iceberg.bronze.*`
- SQL-файлы из `sql/ddl/silver/` и `sql/queries/silver/`

## Ключевые константы

- `LAKEHOUSE_BUCKET`
- `SILVER_PREFIX`
- `SQL_ASSETS`

## Основные функции

- `q`
- `silver_ddl_paths`
- `silver_query_paths`
- `execute_step`
- `log_row_count`
- `parse_fqn`
- `log_iceberg_files_summary`
- `log_batch_row_count`
- `validate_bronze_batch`
- `ensure_silver_schema`
- `ensure_silver_tables`
- `refresh_dim_article`
- `refresh_dim_customer`
- `upsert_dim_date`
- `get_batch_months`
- `parse_months_arg`
- `resolve_months_to_process`
- `month_filter`
- `refresh_fact_sales_line_month`
- `refresh_fact_sales_line_by_month`
- `fact_sales_line_batch_exists`
- `merge_fact_customer_article_stats_batch_delta`
- `get_impacted_prefixes`
- `delete_impacted_stats_prefix`
- `insert_impacted_stats_prefix`
- `refresh_fact_customer_article_stats_incremental`
- `refresh_fact_customer_article_stats`
- `parse_args`
- `main`

## Порядок выполнения

1. Разбор аргументов.
2. Проверка batch в `bronze`.
3. Создание схемы `iceberg.silver`.
4. Создание таблиц `silver`.
5. Обновление `dim_article`.
6. Обновление `dim_customer`.
7. Пополнение `dim_date`.
8. Пересборка `fact_sales_line` по месяцам.
9. Обновление `fact_customer_article_stats`.
10. Логирование строк и Iceberg-файлов.

## Выходы

- обновлённые таблицы `iceberg.silver.*`
- лог по количеству строк
- лог по физической упаковке Iceberg-файлов

## Зависимости

- `config/settings.py`
- `ingestion/utils/sql_assets.py`
- `ingestion/utils/trino_client.py`
- DDL и SQL-файлы слоя `silver`

## Где может сломаться

- batch не найден в `bronze`
- отсутствует DDL-файл
- не совпали месяцы в `--months` и batch
- неправильный `batch-id`
- проблемы доступа к Trino
- проблемы с путями `sql/queries/silver/`
