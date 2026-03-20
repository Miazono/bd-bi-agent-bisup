# `load_marts.py` в краткой схеме

## Назначение

Полная пересборка витрин `marts` поверх готового `silver`.

## Входы

- данные из `silver`
- SQL-файлы из `sql/ddl/mart/`
- SQL-файлы из `sql/queries/mart/`
- доступный Trino

## Ключевые константы

- `LAKEHOUSE_BUCKET`
- `MART_PREFIX`
- `SQL_ASSETS`

## Основные функции

- `mart_ddl_paths`
- `mart_query_specs`
- `execute_step`
- `parse_fqn`
- `log_row_count`
- `log_iceberg_files_summary`
- `ensure_mart_schema`
- `ensure_mart_tables`
- `rebuild_mart_table`
- `main`

## Порядок выполнения

1. Создание схемы `iceberg.mart`.
2. Применение DDL витрин.
3. Полная очистка каждой витрины.
4. Повторное наполнение из SQL-файлов.
5. Логирование строк.
6. Логирование Iceberg-метаданных.

## Выходы

- пересобранные таблицы `iceberg.mart.*`
- лог по числу строк
- лог по числу файлов и объёму данных

## Зависимости

- `config/settings.py`
- `ingestion/utils/sql_assets.py`
- `ingestion/utils/trino_client.py`
- DDL и SQL-файлы витрин

## Где может сломаться

- нет таблиц `silver`
- нет DDL или SQL-файла витрины
- недоступен Trino
- недоступен бакет `lakehouse`
- нарушен порядок загрузки конвейера
