datalakehouse-bi-agent/
│
├── AGENTS.md                        ← (A)
├── ARCHITECTURE.md                  ← (B)
├── README.md                        ← (C)
├── .env.example                     ← (D)
├── .gitignore
│
├── infra/                           ← (E)
│   ├── AGENTS.md                    ← (E1)
│   ├── docker-compose.yml           ← (E2)
│   ├── minio/
│   │   └── init-buckets.sh          ← (E3)
│   ├── hive/
│   │   └── hive-site.xml            ← (E4)
│   ├── trino/
│   │   ├── config.properties        ← (E5)
│   │   └── catalog/
│   │       └── iceberg.properties   ← (E6)
│   └── wrenai/
│       └── config.yaml              ← (E7)
│
├── ingestion/                       ← (F)
│   ├── AGENTS.md                    ← (F1)
│   ├── load_raw.py                  ← (F2)
│   ├── load_iceberg.py              ← (F3)
│   └── utils/
│       └── s3_client.py             ← (F4)
│
├── marts/                           ← (G)
│   ├── AGENTS.md                    ← (G1)
│   ├── revenue_by_category.sql      ← (G2)
│   ├── orders_summary.sql           ← (G3)
│   └── user_activity.sql            ← (G4)
│
├── bi-agent/                        ← (H)
│   ├── AGENTS.md                    ← (H1)
│   ├── semantic_layer/
│   │   └── models.yaml              ← (H2)
│   ├── prompts/
│   │   └── system_prompt.md         ← (H3)
│   └── eval/
│       ├── questions.json           ← (H4)
│       └── llm_judge.py             ← (H5)
│
├── data/                            ← (I)
│   └── raw/                         ← (I1)
│
├── tests/                           ← (J)
│   ├── test_ingestion.py
│   ├── test_marts.py
│   └── test_bi_agent.py
│
├── scripts/                         ← (K)
│   └── gen_schema.py                ← (K1)
│
└── docs/                            ← (L)
    ├── data/
    │   ├── schema.md                ← (L1) ⚠️ авто-генерируется
    │   └── marts.md                 ← (L2)
    ├── setup/
    │   ├── local-setup.md           ← (L3)
    │   └── stack-overview.md        ← (L4)
    ├── decisions/
    │   └── ADR-001-stack-choice.md  ← (L5)
    └── prompts/
        └── prompt-engineering.md   ← (L6)
Описание каждого артефакта
Корень

(A) AGENTS.md — главный файл инструкций для Codex: стек, команды запуска, структура, запреты. Читается при каждой задаче

(B) ARCHITECTURE.md — одна Mermaid-схема всей системы (MinIO → Iceberg → Trino → WrenAI → пользователь). Codex обращается к нему при навигации по незнакомому коду

(C) README.md — для людей: что проект делает, как запустить за 5 минут

(D) .env.example — шаблон переменных окружения без реальных значений (API ключи, пароли). Реальный .env только локально, в .gitignore

infra/ — Инфраструктура

(E1) AGENTS.md — как запускать стек, какие порты, переменные окружения. Codex читает при любой задаче в этой папке

(E2) docker-compose.yml — поднимает MinIO, Hive Metastore, Trino, WrenAI одной командой

(E3) init-buckets.sh — скрипт создания бакетов raw-data и iceberg-warehouse при первом старте MinIO

(E4) hive-site.xml — конфиг Hive Metastore: подключение к Postgres (хранилище метаданных), thrift-порт

(E5) config.properties — основной конфиг Trino: coordinator/worker, HTTP-порт

(E6) iceberg.properties — каталог Iceberg в Trino: указывает на Hive Metastore и S3/MinIO как хранилище

(E7) config.yaml — конфиг WrenAI: какой LLM использовать, подключение к Trino, semantic layer path

ingestion/ — Загрузка данных

(F1) AGENTS.md — формат входных данных, правила именования бакетов, как запускать скрипты

(F2) load_raw.py — загружает исходные CSV/Parquet файлы в MinIO bucket raw-data без изменений (AS IS, шаг 3)

(F3) load_iceberg.py — читает из raw-data, трансформирует и пишет в Iceberg-таблицы через Trino (шаг 4)

(F4) s3_client.py — переиспользуемый boto3-клиент для MinIO с настройками endpoint

marts/ — Аналитические витрины

(G1) AGENTS.md — соглашения по именованию витрин, список доступных Iceberg-таблиц, примеры запросов

(G2–G4) *.sql — SQL-витрины через Trino: агрегации, фильтры, джойны. Результаты шага 5. Каждый файл — одна витрина, один бизнес-вопрос

bi-agent/ — BI-ассистент

(H1) AGENTS.md — как запускать WrenAI, где semantic layer, как добавлять новые модели

(H2) models.yaml — семантический слой: описание таблиц и колонок человеческим языком для NL2SQL. Это ключевой файл качества агента

(H3) system_prompt.md — зафиксированный system prompt SQL-агента. Версионируется, итеративно улучшается

(H4) questions.json — тестовые вопросы на русском с эталонными SQL-ответами для оценки качества

(H5) llm_judge.py — LLM-as-a-Judge: скрипт, который отправляет вопрос + ответ агента + эталон в GPT-4 и получает оценку качества (бонусный критерий проекта)

data/ и scripts/

(I1) raw/ — исходные датасеты локально. В .gitignore если > 100 МБ; для больших данных — только в MinIO

(K1) gen_schema.py — запрашивает SHOW COLUMNS по всем Iceberg-таблицам через Trino и перезаписывает docs/data/schema.md

docs/ — Документация

(L1) schema.md ⚠️ — авто-генерируется скриптом, никогда не редактируется вручную. Codex и WrenAI читают его как источник истины о структуре данных

(L2) marts.md — описание каждой витрины: бизнес-смысл, метрики, примеры вопросов. Семантический слой для людей

(L3) local-setup.md — пошаговая инструкция поднятия стека с нуля за 30 минут

(L4) stack-overview.md — почему именно этот стек, как компоненты связаны между собой

(L5) ADR-001-stack-choice.md — Architecture Decision Record: почему WrenAI, а не Vanna; почему Iceberg, а не Hudi

(L6) prompt-engineering.md — журнал итераций промптов: что пробовали, что улучшило качество, что ухудшило