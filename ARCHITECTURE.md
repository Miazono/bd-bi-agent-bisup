    ```mermaid
    flowchart LR
        subgraph Storage
            R[Raw files (CSV/Parquet)<br/>data/raw/]
            M[(MinIO S3<br/>buckets: raw-data, iceberg-warehouse)]
        end

        subgraph Metadata
            H[(Hive Metastore)]
        end

        subgraph Compute
            T[(Trino)]
        end

        subgraph BI
            U[User]
            W[WrenAI / SQL BI Agent]
        end

        R -->|ingestion/load_raw.py| M
        M -->|Iceberg data files| T
        H -->|table metadata| T
        T -->|SQL queries to Iceberg| M

        U -->|NL question| W
        W -->|generated SQL| T
        T -->|query results| W
        W -->|final answer| U
    ```

    # Data flow (end-to-end)
    # Raw ingestion

    Источник: файлы из data/raw/ (или внешнего источника).
    Скрипт ingestion/load_raw.py загружает данные AS IS в MinIO bucket raw-data без трансформаций.

    # Lakehouse tables (Iceberg)

    Скрипт ingestion/load_iceberg.py читает данные из MinIO (raw-data) через Trino.

    Трансформации и очистка выполняются в Python/SQL, результат записывается в Iceberg-таблицы в бакете iceberg-warehouse.

    Hive Metastore хранит метаданные таблиц (схемы, партиции, снапшоты).

    # Analytical marts

    В директории marts/ определены SQL-витрины (views / tables) поверх Iceberg-таблиц через Trino.

    scripts/gen_schema.py опрашивает Trino и генерирует docs/data/schema.md — актуальное описание схем.

    # SQL BI Agent

    BI-агент (WrenAI или аналог) подключается к Trino и использует semantic layer bi-agent/semantic_layer/models.yaml.

    Пользователь задает вопрос на естественном языке (NL).

    Агент превращает вопрос в SQL, выполняет запрос в Trino, агрегирует результат и формирует ответ.

    Качество работы оценивается через тестовые вопросы (bi-agent/eval/questions.json) и LLM-as-a-Judge (bi-agent/eval/llm_judge.py).

    # Components
    MinIO (S3 storage)
    Объектное хранилище для сырых файлов и файлов Iceberg-таблиц. Локальный аналог Amazon S3, поднимается через infra/docker-compose.yml [web:89][web:91].

    Hive Metastore (metadata)
    Центральный каталог метаданных (схемы, партиции, таблицы) для Iceberg. Trino использует его для планирования запросов.

    Apache Iceberg (table format)
    Табличный формат поверх S3 (MinIO), поддерживающий schema evolution, time travel и атомарные операции. Trino обращается к Iceberg как к обычным таблицам.

    Trino (SQL engine)
    Распределённый SQL-движок для аналитических запросов по Iceberg-таблицам. Используется и для построения витрин, и как backend для BI-агента 
    BI Agent (WrenAI / другое)
    Сервис, который принимает естественные запросы пользователя, опирается на semantic layer и генерирует SQL к Trino. Возвращает осмысленные ответы и визуализации на основе витрин 