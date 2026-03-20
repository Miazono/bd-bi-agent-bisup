# lakehouse-bi-agent

Учебный проект по построению локального Data Lakehouse и BI-агента поверх аналитических витрин датасета H&M Fashion Recommendations.

## Навигация по учебным материалам

### Базовая навигация

- [Словарь терминов и обозначений](docs/study/00-glossary.md)
- [Карта обучения и порядок чтения](docs/study/01-learning-map.md)
- [Главная страница учебного комплекта](docs/study/README.md)

### Теория по платформе и подходу

- [Обзор теоретического раздела](docs/study/theory/README.md)
- [Архитектура warehouse и lakehouse](docs/study/theory/01-warehouse-and-lakehouse-architecture.md)
- [Моделирование данных и слоистая организация](docs/study/theory/02-data-modeling-and-layering.md)
- [Синтаксис Trino SQL, используемый в проекте](docs/study/theory/03-trino-sql-syntax-used-in-project.md)
- [Python-библиотеки, используемые в проекте](docs/study/theory/04-python-libraries-used-in-project.md)
- [MinIO, Hive Metastore, Iceberg и Trino](docs/study/theory/05-minio-hive-metastore-iceberg-trino.md)
- [WrenAI и семантический слой](docs/study/theory/06-wrenai-and-semantic-layer.md)

### Разбор реализации проекта

- [Архитектура системы проекта](docs/study/project/01-system-architecture.md)
- [Пайплайн данных по слоям](docs/study/project/02-data-pipeline.md)
- [Слои данных и состав таблиц](docs/study/project/03-data-layers-and-tables.md)
- [Разбор SQL DDL для таблиц](docs/study/project/04-sql-ddl-breakdown.md)
- [Разбор SQL загрузки bronze-слоя](docs/study/project/05-sql-bronze-breakdown.md)
- [Разбор SQL преобразований silver-слоя](docs/study/project/06-sql-silver-breakdown.md)
- [Разбор SQL витрин marts](docs/study/project/07-sql-marts-breakdown.md)
- [Настройки Python и общие утилиты](docs/study/project/08-python-settings-and-utils.md)
- [Скрипт загрузки raw-слоя](docs/study/project/09-python-load-raw.md)
- [Скрипт загрузки bronze-слоя](docs/study/project/10-python-load-bronze.md)
- [Скрипт загрузки silver-слоя](docs/study/project/11-python-load-silver.md)
- [Скрипт загрузки marts-слоя](docs/study/project/12-python-load-marts.md)
- [Инфраструктурная конфигурация проекта](docs/study/project/13-infra-config-breakdown.md)

### BI-агент и семантический слой

- [Обзор раздела BI-части](docs/study/bi-agent/README.md)
- [Что такое WrenAI и где он в проекте](docs/study/bi-agent/01-wrenai-overview.md)
- [Архитектура BI-части проекта](docs/study/bi-agent/02-project-bi-architecture.md)
- [Разбор семантического слоя](docs/study/bi-agent/03-semantic-layer-breakdown.md)
- [Разбор конфигурации WrenAI](docs/study/bi-agent/04-wrenai-config-breakdown.md)
- [Текущее и целевое состояние BI-части](docs/study/bi-agent/05-current-vs-target-state.md)
- [Разбор оценки качества и evaluation](docs/study/bi-agent/06-evaluation-breakdown.md)

### Путеводители по скриптам

- [Обзор формата script guides](docs/study/script-guides/README.md)
- [Карта скрипта загрузки raw](docs/study/script-guides/maps/ingestion/load_raw.md)
- [Карта скрипта загрузки bronze](docs/study/script-guides/maps/ingestion/load_bronze.md)
- [Карта скрипта загрузки silver](docs/study/script-guides/maps/ingestion/load_silver.md)
- [Карта скрипта загрузки marts](docs/study/script-guides/maps/ingestion/load_marts.md)
- [Карта генерации схемы и каталога](docs/study/script-guides/maps/scripts/gen_schema.md)
- [Карта инициализации хранилища](docs/study/script-guides/maps/scripts/init_storage.md)

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
