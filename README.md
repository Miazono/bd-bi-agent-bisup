# lakehouse-bi-agent

Учебный проект по построению локального Data Lakehouse и BI-агента поверх аналитических витрин датасета H&M Fashion Recommendations.

## Цель проекта

Проект покрывает полный путь от исходных CSV-файлов до витрин, доступных через SQL:

- загрузка исходных данных в MinIO;
- построение слоёв `raw -> bronze -> silver -> marts`;
- публикация витрин в Trino поверх Iceberg;
- подключение BI-агента для вопросов на естественном языке.

## Датасет

В проекте используется датасет H&M Fashion Recommendations.

В текущий контур входят три исходных файла:

- `articles.csv`;
- `customers.csv`;
- `transactions_train.csv`.

Изображения товаров в текущую версию не входят.

## Технологический стек

- MinIO;
- Hive Metastore;
- Apache Iceberg;
- Trino;
- WrenAI;
- Python 3.11+.

## Поток данных

### Raw

Исходные CSV-файлы сохраняются в бакете `lakehouse` по пути:

- `raw/hm/articles/load_date=YYYY-MM-DD/articles.csv`;
- `raw/hm/customers/load_date=YYYY-MM-DD/customers.csv`;
- `raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv`.

Слой `raw` хранит файлы без бизнес-преобразований.

### Bronze

Скрипт `ingestion/load_bronze.py` создаёт временные внешние таблицы в `hive.raw`, а затем загружает данные в Iceberg-таблицы:

- `iceberg.bronze.hm_articles`;
- `iceberg.bronze.hm_customers`;
- `iceberg.bronze.hm_transactions`.

### Silver

Скрипт `ingestion/load_silver.py` формирует очищенную аналитическую модель:

- `iceberg.silver.dim_article`;
- `iceberg.silver.dim_customer`;
- `iceberg.silver.dim_date`;
- `iceberg.silver.fact_sales_line`;
- `iceberg.silver.fact_customer_article_stats`.

### Marts

Скрипт `ingestion/load_marts.py` пересобирает физические витрины в схеме `iceberg.mart`:

- `sales_daily_channel`;
- `sales_monthly_category`;
- `customer_segment_monthly`;
- `repeat_purchase_category`;
- `customer_rfm_monthly`.

Логически этот слой называется `marts`, а физически витрины лежат в схеме `mart`.

## Структура репозитория

- `infra/` — конфиги локального стека и сервисов;
- `ingestion/` — Python-скрипты загрузки и построения слоёв;
- `sql/ddl/` — DDL физических таблиц Iceberg;
- `sql/queries/` — SQL-преобразования для слоёв bronze, silver и marts;
- `bi-agent/` — артефакты BI-агента, семантического слоя и оценки качества;
- `docs/` — проектная документация;
- `scripts/` — служебные скрипты;
- `tests/` — автотесты.

## Быстрый запуск

1. Создать виртуальное окружение и установить зависимости:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Подготовить переменные окружения:

   ```bash
   cp .env.local.example .env.local
   cp .env.docker.example .env.docker
   set -a && source .env.local && set +a
   ```

3. Поднять локальный стек:

   ```bash
   make up
   ```

4. Инициализировать бакет и префиксы слоёв:

   ```bash
   make init-storage
   ```

5. Последовательно загрузить данные:

   ```bash
   make load-raw
   make load-bronze
   make load-silver
   make load-marts
   ```

6. Поднять WrenAI:

   ```bash
   make up-wrenai
   ```

Подробные шаги описаны в `docs/setup/local-setup.md`.

## Документация

- `ARCHITECTURE.md` — архитектура проекта и карта сервисов;
- `docs/data/schema.md` —  описание слоёв, таблиц и grain;
- `docs/data/catalog_generated.md` — производный каталог столбцов, генерируемый из DDL;
- `docs/data/marts.md` — описание аналитических витрин;
- `docs/data/lineage.md` — происхождение данных и зависимости между слоями;
- `docs/setup/local-setup.md` — пошаговый локальный запуск;
- `docs/setup/stack-overview.md` — обзор локального стека;
- `docs/decisions/` — архитектурные и проектные решения.

## Текущее состояние

В репозитории уже реализованы:

- локальный стек MinIO + Hive Metastore + Trino;
- загрузка `raw`, `bronze`, `silver` и `marts`;
- DDL и SQL-преобразования для аналитических слоёв;
- набор тестов для загрузки и витрин.

BI-агент и его окружение уже заведены в структуре проекта, но семантический слой, промпты и оценка качества пока находятся на ранней стадии наполнения.
