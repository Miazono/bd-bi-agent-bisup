# ARCHITECTURE

## Purpose

Проект представляет собой локальный учебный Data Lakehouse со слоем BI-ассистента.
Цель — пройти полный путь от загрузки сырых данных до аналитических витрин и natural-language доступа к ним через SQL AI agent.

## Target stack

- MinIO — объектное хранилище для raw-файлов и файлов Iceberg-таблиц.
- Hive Metastore — каталог таблиц Iceberg.
- Trino — SQL-движок для работы с Iceberg-таблицами и витринами.
- WrenAI (или аналог) — BI-агент, который генерирует SQL к Trino.
- Python — ingestion и вспомогательные скрипты.

## Dataset

В качестве основного набора данных используется H&M Fashion Recommendations dataset.

В scope v1 входят только табличные данные:
- `articles.csv`
- `customers.csv`
- `transactions_train.csv`

## Data flow

### 1. Raw layer
Исходные файлы загружаются в MinIO без бизнес-трансформаций.

Назначение raw-слоя:
- сохранить оригинальные данные;
- обеспечить воспроизводимость загрузки;
- отделить landing zone от аналитической модели.

Пример логической структуры хранения:
- `s3://lakehouse/raw/hm/articles/load_date=YYYY-MM-DD/articles.csv`
- `s3://lakehouse/raw/hm/customers/load_date=YYYY-MM-DD/customers.csv`
- `s3://lakehouse/raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv`

### 2. Bronze layer
Bronze — это технически нормализованные Iceberg-таблицы, максимально близкие к источнику.

Планируемые таблицы:
- `bronze.hm_articles`
- `bronze.hm_customers`
- `bronze.hm_transactions`

На этом слое допускаются:
- приведение типов;
- добавление технических полей загрузки;
- нормализация пустых значений;
- базовые DQ-проверки.

### 3. Silver layer
Silver — это очищенная аналитическая модель, пригодная для JOIN, сегментации и расчёта витрин.

Планируемые таблицы:
- `silver.dim_article`
- `silver.dim_customer`
- `silver.dim_date`
- `silver.fact_sales_line`
- `silver.fact_customer_article_stats`

### 4. Marts layer
Marts — это готовые аналитические витрины для Trino и BI-агента.

Планируемые витрины:
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.repeat_purchase_category`
- `mart.customer_rfm_monthly`

Назначение mart-слоя:
- упростить работу BI-агента;
- зафиксировать конечные метрики;
- дать стабильный слой для демо и evaluation.

### 5. BI agent
BI-агент подключается к Trino и использует semantic layer из `bi-agent/semantic_layer/`.
Пользователь задаёт вопрос на естественном языке.
Агент:
1. сопоставляет вопрос с описанными сущностями и метриками;
2. генерирует SQL;
3. выполняет SQL через Trino;
4. возвращает ответ пользователю.

### 6. Evaluation
Качество BI-агента проверяется на тестовом наборе вопросов в `bi-agent/eval/`.
При необходимости используется LLM-as-a-Judge для сравнения ответа агента с эталоном.

## Architectural principles

- Документация описывает целевую архитектуру, даже если часть сервисов пока не реализована.
- Raw, Bronze, Silver и Marts — логически разные слои.
- Основной факт проекта — продажи на уровне purchase line.
- Главные измерения — товар, клиент и дата.
- Iceberg используется как табличный формат lakehouse.
- Trino используется как основной SQL backend для витрин и BI-агента.
- `docs/data/catalog_generated.md` является производной документацией и не редактируется вручную.


## Current implementation status

На текущем этапе репозиторий описывает структуру и целевые артефакты проекта.
Допускается, что часть файлов пока являются заготовками и будут реализованы позже через Codex.