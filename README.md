# datalakehouse-bi-agent

Учебный проект по построению локального Data Lakehouse и BI-ассистента поверх аналитических витрин.

## Project goal

Цель проекта:
- загрузить исходные данные в объектное хранилище;
- построить слои `raw -> bronze -> silver -> marts`;
- предоставить SQL-доступ к витринам через Trino;
- подключить BI-агента для natural-language вопросов к данным.

## Dataset

Проект использует H&M Fashion Recommendations dataset.

В scope v1 входят:
- `articles.csv`
- `customers.csv`
- `transactions_train.csv`

Изображения товаров в scope v1 не входят.

## Target stack

- MinIO
- Hive Metastore
- Apache Iceberg
- Trino
- WrenAI (or similar SQL BI agent)
- Python

## Planned data model

### Raw
Хранение исходных CSV без бизнес-трансформаций.

### Bronze
Технически нормализованные Iceberg-таблицы:
- `bronze.hm_articles`
- `bronze.hm_customers`
- `bronze.hm_transactions`

### Silver
Очищенная аналитическая модель:
- `silver.dim_article`
- `silver.dim_customer`
- `silver.dim_date`
- `silver.fact_sales_line`
- `silver.fact_customer_article_stats`

### Marts
Готовые витрины для BI-agent:
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.repeat_purchase_category`
- `mart.customer_rfm_monthly`

## Repository structure

- `infra/` — инфраструктурные конфиги и compose
- `ingestion/` — скрипты загрузки и построения слоёв
- `marts/` — SQL-витрины
- `bi-agent/` — semantic layer, prompts, evaluation
- `docs/` — архитектурная и data-документация
- `scripts/` — служебные скрипты
- `tests/` — тесты

## Local stack

Локальная инфраструктура поднимается через `docker-compose.yml`:

```bash
docker compose up -d
```

Версия MinIO задаётся через `MINIO_IMAGE` в `.env.example`.

## Documentation

- `ARCHITECTURE.md` — целевая архитектура проекта
- `docs/data/schema.md` — каталог таблиц и grain
- `docs/data/marts.md` — описание аналитических витрин
- `docs/data/lineage.md` — происхождение данных и зависимости между слоями

## Current status

Репозиторий находится в стадии проектирования и поэтапного наполнения.
Часть файлов пока является заготовками для последующей реализации
