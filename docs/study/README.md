# Учебный комплект по проекту

## Навигация по разделам

### Базовая навигация

- [Словарь терминов и обозначений](00-glossary.md)
- [Карта обучения и порядок чтения](01-learning-map.md)

### Теория по платформе и подходу

- [Обзор теоретического раздела](theory/README.md)
- [Архитектура warehouse и lakehouse](theory/01-warehouse-and-lakehouse-architecture.md)
- [Моделирование данных и слоистая организация](theory/02-data-modeling-and-layering.md)
- [Синтаксис Trino SQL, используемый в проекте](theory/03-trino-sql-syntax-used-in-project.md)
- [Python-библиотеки, используемые в проекте](theory/04-python-libraries-used-in-project.md)
- [MinIO, Hive Metastore, Iceberg и Trino](theory/05-minio-hive-metastore-iceberg-trino.md)
- [WrenAI и семантический слой](theory/06-wrenai-and-semantic-layer.md)

### Разбор реализации проекта

- [Архитектура системы проекта](project/01-system-architecture.md)
- [Пайплайн данных по слоям](project/02-data-pipeline.md)
- [Слои данных и состав таблиц](project/03-data-layers-and-tables.md)
- [Разбор SQL DDL для таблиц](project/04-sql-ddl-breakdown.md)
- [Разбор SQL загрузки bronze-слоя](project/05-sql-bronze-breakdown.md)
- [Разбор SQL преобразований silver-слоя](project/06-sql-silver-breakdown.md)
- [Разбор SQL витрин marts](project/07-sql-marts-breakdown.md)
- [Настройки Python и общие утилиты](project/08-python-settings-and-utils.md)
- [Скрипт загрузки raw-слоя](project/09-python-load-raw.md)
- [Скрипт загрузки bronze-слоя](project/10-python-load-bronze.md)
- [Скрипт загрузки silver-слоя](project/11-python-load-silver.md)
- [Скрипт загрузки marts-слоя](project/12-python-load-marts.md)
- [Инфраструктурная конфигурация проекта](project/13-infra-config-breakdown.md)

### BI-агент и семантический слой

- [Обзор раздела BI-части](bi-agent/README.md)
- [Что такое WrenAI и где он в проекте](bi-agent/01-wrenai-overview.md)
- [Архитектура BI-части проекта](bi-agent/02-project-bi-architecture.md)
- [Разбор семантического слоя](bi-agent/03-semantic-layer-breakdown.md)
- [Разбор конфигурации WrenAI](bi-agent/04-wrenai-config-breakdown.md)
- [Текущее и целевое состояние BI-части](bi-agent/05-current-vs-target-state.md)
- [Разбор оценки качества и evaluation](bi-agent/06-evaluation-breakdown.md)

### Путеводители по скриптам

- [Обзор формата script guides](script-guides/README.md)
- [Карта скрипта загрузки raw](script-guides/maps/ingestion/load_raw.md)
- [Карта скрипта загрузки bronze](script-guides/maps/ingestion/load_bronze.md)
- [Карта скрипта загрузки silver](script-guides/maps/ingestion/load_silver.md)
- [Карта скрипта загрузки marts](script-guides/maps/ingestion/load_marts.md)
- [Карта генерации схемы и каталога](script-guides/maps/scripts/gen_schema.md)
- [Карта инициализации хранилища](script-guides/maps/scripts/init_storage.md)

## Назначение

Этот раздел содержит учебные материалы для глубокого понимания проекта `db-bi-agent-bisup`.
Его цель не заменить основную проектную документацию, а помочь разобраться:

- в теории, которая нужна именно для этого проекта;
- в фактической реализации репозитория;
- в том, где проект уже завершён, а где находится в промежуточном состоянии;
- в том, как объяснять устройство решения последовательно и технически корректно.

## Чем этот раздел отличается от основной документации

Основная документация репозитория отвечает в первую очередь на вопрос, как устроен проект и как его запускать.
Раздел `docs/study/` отвечает на другой вопрос: как действительно понять этот проект и связанные с ним технологии.

Поэтому здесь будет больше:

- пояснений терминов;
- разборов по шагам;
- фиксации причин принятых решений;
- разделения между текущим и целевым состоянием;
- связей между инфраструктурой, данными, SQL, Python-скриптами и BI-частью.

## Источники фактов о проекте

Для фактического устройства проекта основными опорными файлами остаются:

- `README.md`;
- `ARCHITECTURE.md`;
- `docs/data/schema.md`;
- `docs/data/catalog_generated.md`;
- `docs/data/lineage.md`;
- `docs/data/marts.md`;
- `docker-compose.yml`;
- `ingestion/`;
- `sql/`;
- `bi-agent/`;
- `infra/`.

Если учебный материал и код расходятся, верить нужно коду и фактической конфигурации.

## Структура комплекта

Основные блоки:

- `00-glossary.md` — единый словарь терминов и соглашений;
- `01-learning-map.md` — карта всего учебного комплекта и порядок изучения;
- `project/` — разбор именно этого проекта: архитектура, пайплайн, таблицы, SQL, скрипты, конфигурация;
- `script-guides/` — вспомогательный формат: аннотированные копии скриптов и короткие схемы для быстрого чтения;
- `theory/` — теория по архитектуре хранилищ, моделированию данных, SQL, Python-инструментам, Trino, Iceberg, MinIO, Hive Metastore и BI-подходу;
- `bi-agent/` — теория и разбор BI-части, включая текущее и целевое состояние.

## Принципы оформления

Во всех документах этого раздела соблюдаются такие правила:

- терминология должна использоваться одинаково во всех файлах;
- теория должна быть релевантна проекту, но не подменять собой разбор проекта;
- разбор проекта должен опираться только на фактическую реализацию репозитория;
- текущее состояние и целевое состояние должны отделяться явно;
- английские термины используются только там, где это название технологии, общепринятое понятие или имя артефакта.

## Рекомендуемый порядок чтения

Если цель — быстро собрать целостную картину, лучше идти так:

1. `00-glossary.md`
2. `01-learning-map.md`
3. `theory/README.md`
4. `theory/01-warehouse-and-lakehouse-architecture.md`
5. `theory/02-data-modeling-and-layering.md`
6. `theory/05-minio-hive-metastore-iceberg-trino.md`
7. `theory/03-trino-sql-syntax-used-in-project.md`
8. `theory/04-python-libraries-used-in-project.md`
9. `theory/06-wrenai-and-semantic-layer.md`
10. `project/01-system-architecture.md`
11. `project/02-data-pipeline.md`
12. `project/03-data-layers-and-tables.md`
13. `project/04-sql-ddl-breakdown.md`
14. `project/05-sql-bronze-breakdown.md`
15. `project/06-sql-silver-breakdown.md`
16. `project/07-sql-marts-breakdown.md`
17. `project/08-python-settings-and-utils.md`
18. `project/09-python-load-raw.md`
19. `project/10-python-load-bronze.md`
20. `project/11-python-load-silver.md`
21. `project/12-python-load-marts.md`
22. `project/13-infra-config-breakdown.md`
23. `bi-agent/README.md`
24. `bi-agent/01-wrenai-overview.md`
25. `bi-agent/02-project-bi-architecture.md`
26. `bi-agent/03-semantic-layer-breakdown.md`
27. `bi-agent/04-wrenai-config-breakdown.md`
28. `bi-agent/05-current-vs-target-state.md`
29. `bi-agent/06-evaluation-breakdown.md`
30. `script-guides/README.md`

Основной маршрут чтения проходит через `theory/`, затем `project/`, затем `bi-agent/`.
Каталог `script-guides/` лучше воспринимать как вспомогательный быстрый формат, а не как основной способ изучения проекта.
