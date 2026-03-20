# Краткая схема `load_bronze.py`

## Назначение

Перевести raw-файлы из MinIO в физические Iceberg-таблицы `bronze`, через временные внешние таблицы `hive.raw.*`.

## Входы

- `--load-date`
- `--batch-id`
- `--raw-prefix`
- `settings.lakehouse_bucket`
- `settings.raw_prefix`
- `settings.bronze_prefix`
- `settings.s3_table_scheme`
- SQL-файлы из `sql/ddl/bronze/`
- SQL-файлы из `sql/queries/bronze/`

## Ключевые константы

- `RAW_LAYOUT` - ожидаемые raw-пути в MinIO
- `RAW_TABLE_COLUMNS` - схема временных external tables
- `LAKEHOUSE_BUCKET` - имя бакета
- `RAW_PREFIX` - префикс raw
- `BRONZE_PREFIX` - префикс bronze

## Функции

- `q(value)` - экранирование строки для SQL
- `bronze_ddl_paths()` - список DDL-файлов bronze
- `bronze_query_paths()` - карта SQL-запросов bronze
- `validate_raw_files(...)` - проверка наличия raw-файлов
- `create_schemas(...)` - создание `hive.raw` и `iceberg.bronze`
- `create_raw_external_table(...)` - создание одной external table
- `create_all_raw_tables(...)` - создание всех external tables
- `create_bronze_tables(...)` - применение DDL таблиц bronze
- `delete_batch(...)` - удаление старых строк batch
- `load_articles_to_bronze(...)` - загрузка статей
- `load_customers_to_bronze(...)` - загрузка клиентов
- `load_transactions_to_bronze(...)` - загрузка транзакций
- `log_counts(...)` - подсчёт строк после загрузки
- `parse_args()` - разбор CLI
- `main()` - полный сценарий bronze-загрузки

## Порядок Выполнения

1. Создать `S3Client` и `TrinoClient`.
2. Убедиться, что бакет существует.
3. Проверить наличие raw-файлов для нужной даты.
4. Создать схемы `hive.raw` и `iceberg.bronze`.
5. Создать временные внешние таблицы над CSV.
6. Создать физические таблицы bronze из DDL.
7. Удалить старые строки текущего `batch_id`.
8. Выполнить три SQL-загрузки в bronze.
9. Посчитать количество строк в таблицах.

## Выходы

- Физические таблицы `iceberg.bronze.*`
- Временные external tables `hive.raw.*`
- Логи по количеству строк

## Зависимости

- `S3Client`
- `TrinoClient`
- `SqlAssets`
- `config.settings.settings`
- SQL-файлы `sql/ddl/bronze/*` и `sql/queries/bronze/*`

## Где Может Сломаться

- Нет raw-файлов для указанной даты
- Не создан бакет
- Не доступны MinIO или Trino
- Нарушен expected layout raw-слоя
- Отсутствует один из DDL-файлов
- Не удалось выполнить SQL-загрузку или `DELETE` по batch
