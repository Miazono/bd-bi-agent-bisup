# Разбор инфраструктурных конфигураций

## Назначение

Этот документ подробно разбирает инфраструктурные конфиги проекта `db-bi-agent-bisup`.
Его задача — показать не только состав сервисов, но и то, как именно связи между ними выражены в файлах конфигурации.

Разбор опирается только на фактические файлы репозитория:

- [`docker-compose.yml`](/root/repos/db-bi-agent-bisup/docker-compose.yml)
- [`infra/hive/Dockerfile`](/root/repos/db-bi-agent-bisup/infra/hive/Dockerfile)
- [`infra/hive/render-hive-site.sh`](/root/repos/db-bi-agent-bisup/infra/hive/render-hive-site.sh)
- [`infra/minio/init-buckets.sh`](/root/repos/db-bi-agent-bisup/infra/minio/init-buckets.sh)
- [`infra/trino/catalog/hive.properties`](/root/repos/db-bi-agent-bisup/infra/trino/catalog/hive.properties)
- [`infra/trino/catalog/iceberg.properties`](/root/repos/db-bi-agent-bisup/infra/trino/catalog/iceberg.properties)
- [`infra/trino/config.properties`](/root/repos/db-bi-agent-bisup/infra/trino/config.properties)
- [`infra/trino/jvm.config`](/root/repos/db-bi-agent-bisup/infra/trino/jvm.config)
- [`infra/wrenai/config.yaml`](/root/repos/db-bi-agent-bisup/infra/wrenai/config.yaml)

## Главный принцип инфраструктуры проекта

Проект строит локальный `lakehouse` как связку нескольких сервисов, у каждого из которых своя зона ответственности:

- MinIO хранит файлы;
- PostgreSQL хранит метаданные Hive Metastore;
- Hive Metastore публикует метаданные таблиц;
- Trino выполняет SQL;
- WrenAI работает как BI-слой поверх Trino.

Это не монолитная система, а цепочка отдельных сервисов, соединённых через сеть Docker, переменные окружения и файловые конфиги.

## `docker-compose.yml`

### Роль файла

[`docker-compose.yml`](/root/repos/db-bi-agent-bisup/docker-compose.yml) — это главный файл оркестрации локального стека.
Он определяет:

- какие контейнеры входят в проект;
- какие образы используются;
- какие порты пробрасываются наружу;
- какие тома монтируются;
- какие переменные окружения передаются внутрь сервисов;
- какие сервисы входят в базовый стек, а какие подключаются только по профилю `wrenai`.

## Базовый контур

Без профиля `wrenai` в рабочем контуре участвуют:

- `minio`
- `metastore-db`
- `hive-metastore`
- `trino`

Именно этот набор нужен для загрузки `raw -> bronze -> silver -> marts`.

## BI-контур

По профилю `wrenai` дополнительно подключаются:

- `bootstrap`
- `wren-engine`
- `ibis-server`
- `qdrant`
- `wren-ai-service`
- `wren-ui`

Этот контур не нужен для построения данных.
Он нужен для BI-публикации, семантического слоя и NL2SQL-сценариев.

## Сервис `minio`

### Что делает

`minio` поднимает S3-совместимое объектное хранилище.
В проекте через него хранятся:

- raw-файлы;
- данные Iceberg-таблиц;
- физические файлы витрин.

### Что важно в конфиге

- образ: `minio/minio`;
- команда: `server /data --console-address ":9001"`;
- порты:
  - `9000` — S3 API
  - `9001` — web console
- данные сохраняются в том `minio-data`.

### Практический нюанс

В `docker-compose.yml` значение по умолчанию для `MINIO_ROOT_USER` указано как `adminadmin`, а в `.env.local.example` и `.env.docker.example` используется `admin`.
Это значит, что в проекте лучше опираться не на встроенные дефолты compose-файла, а на реальные `.env`-файлы.

## Сервис `metastore-db`

### Что делает

`metastore-db` — это PostgreSQL, который не хранит бизнес-данные lakehouse.
Он хранит только внутренние метаданные Hive Metastore.

### Что важно в конфиге

- образ: `postgres:15-alpine`;
- база: `metastore`;
- пользователь и пароль берутся из переменных окружения;
- данные сохраняются в том `metastore-db-data`;
- есть `healthcheck` через `pg_isready`.

### Почему это важно

Без этой базы Hive Metastore не сможет хранить сведения о таблицах, схемах и путях данных.

## Сервис `hive-metastore`

### Что делает

`hive-metastore` — это каталог метаданных для Iceberg-таблиц.
Он не хранит сами данные таблиц, а хранит информацию:

- какие таблицы существуют;
- где лежат их файлы;
- какова их схема;
- как к ним должен подключаться SQL-движок.

### Что важно в compose-конфиге

- сервис собирается локально через [`infra/hive/Dockerfile`](/root/repos/db-bi-agent-bisup/infra/hive/Dockerfile);
- зависит от `metastore-db` и `minio`;
- использует кастомный `entrypoint`, который сначала выполняет [`render-hive-site.sh`](/root/repos/db-bi-agent-bisup/infra/hive/render-hive-site.sh), а потом стандартный entrypoint образа;
- внутрь контейнера монтируются:
  - `render-hive-site.sh`
  - `postgresql-42.6.0.jar`
  - том `hive-warehouse`

### Почему нужен собственный образ

Базовый образ Hive не умеет из коробки работать с MinIO через `S3A`.
Поэтому проект добавляет нужные зависимости в Dockerfile.

## `infra/hive/Dockerfile`

### Что делает

Этот Dockerfile строится на базе `apache/hive:4.0.0` и добавляет:

- `curl`;
- `hadoop-aws`;
- `aws-java-sdk-bundle`.

### Зачем это нужно

Эти зависимости нужны, чтобы Hive мог:

- использовать файловую систему `s3a://`;
- работать с S3-совместимым MinIO;
- читать и записывать данные через S3A-клиент Hadoop.

Без них Hive Metastore не смог бы корректно взаимодействовать с MinIO как с объектным хранилищем таблиц.

## `infra/hive/render-hive-site.sh`

### Что делает

Скрипт собирает `hive-site.xml` при старте контейнера Hive Metastore.
Он не хранит готовый XML в репозитории, а создаёт его динамически из переменных окружения.

### Что он настраивает

В generated `hive-site.xml` попадают:

- JDBC URL для PostgreSQL;
- имя пользователя и пароль БД;
- `hive.metastore.uris`;
- warehouse directory;
- S3A endpoint;
- access key и secret key;
- path-style access;
- использование `org.apache.hadoop.fs.s3a.S3AFileSystem`;
- отключение SSL для S3A;
- отключение проверки валидности location при создании схем.

### Две важные детали

#### Warehouse directory

Значение `hive.metastore.warehouse.dir` задано как `s3a://lakehouse/warehouse/`.
Это не то же самое, что логические слои `raw`, `bronze`, `silver`, `marts`.
Это служебная warehouse-директория Metastore, а не префикс конкретного слоя проекта.

#### `hive.metastore.check.valid.location=false`

Эта опция отключает строгую проверку location при создании схем и таблиц.
Для локального учебного проекта это упрощает работу с MinIO и вручную заданными путями.

## Сервис `trino`

### Что делает

`trino` — это SQL-движок проекта.
Он выполняет запросы:

- к временным внешним таблицам `hive.raw.*`;
- к физическим Iceberg-таблицам;
- к витринам `mart`.

### Что важно в compose-конфиге

- образ: `trinodb/trino:414`;
- порт: `8080`;
- внутрь контейнера монтируются:
  - каталог `infra/trino/catalog/`
  - `config.properties`
  - `jvm.config`

### Что передаётся через окружение

В переменных окружения Trino получает:

- `HIVE_METASTORE_URI`
- `MINIO_ENDPOINT`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_REGION`

Эти значения потом используются в файлах каталогов Trino.

## `infra/trino/catalog/hive.properties`

### Назначение

Этот файл настраивает каталог `hive`.
Именно через него проект создаёт и читает временные внешние таблицы над raw-файлами.

### Что в нём важно

- `connector.name=hive`
- `hive.metastore.uri=${ENV:HIVE_METASTORE_URI}`
- настройки S3 endpoint и учётных данных
- `hive.s3.path-style-access=true`
- `hive.s3.ssl.enabled=false`
- `hive.security=allow-all`

### Почему это важно

Каталог `hive` нужен не для основных управляемых таблиц проекта, а именно для внешнего чтения CSV из `raw`.

## `infra/trino/catalog/iceberg.properties`

### Назначение

Этот файл настраивает каталог `iceberg`.
Через него проект работает с физическими таблицами слоёв:

- `bronze`
- `silver`
- `mart`

### Что в нём важно

- `connector.name=iceberg`
- `iceberg.catalog.type=hive_metastore`
- `hive.metastore.uri=${ENV:HIVE_METASTORE_URI}`
- настройки доступа к MinIO
- `iceberg.file-format=PARQUET`

### Почему это важно

Именно этот каталог делает Iceberg-таблицы основным физическим табличным слоем проекта.

## `infra/trino/config.properties`

### Что делает

Это общий конфиг узла Trino.
Он задаёт:

- что этот узел является координатором;
- какой порт слушать;
- URI discovery;
- режим управления каталогами;
- лимиты памяти.

### Что особенно важно

- `coordinator=true`
- `node-scheduler.include-coordinator=true`
- `http-server.http.port=8080`
- `discovery.uri=http://localhost:8080`
- `catalog.management=static`

### Практический нюанс

В `docker-compose.yml` сервису `trino` передаётся переменная `CATALOG_MANAGEMENT=dynamic`, но в самом `config.properties` явно указано `catalog.management=static`.

Для понимания текущей реализации важнее именно файловый конфиг, потому что он задаёт фактическое поведение Trino в этом репозитории.

### Память

В этом же файле повышены лимиты памяти:

- `query.max-memory-per-node=6GB`
- `memory.heap-headroom-per-node=1GB`
- `query.max-memory=6GB`
- `query.max-total-memory=7GB`

Это согласуется с документацией по локальному запуску и помогает выдерживать тяжёлые запросы по `silver` и `mart`.

## `infra/trino/jvm.config`

### Что делает

Этот файл задаёт JVM-параметры Trino.

### Ключевая настройка

Главное значение здесь — `-Xmx8G`.
Оно задаёт размер heap-памяти JVM.

### Почему это важно

Вместе с лимитами из `config.properties` это формирует профиль памяти, достаточный для локального учебного прогона полного набора запросов.

## Служебный скрипт `infra/minio/init-buckets.sh`

### Что делает

Этот скрипт создаёт бакет `lakehouse` через утилиту `mc`.

### Важное наблюдение

В текущем проекте основной рабочий путь инициализации хранилища идёт через Python-скрипт [`scripts/init_storage.py`](/root/repos/db-bi-agent-bisup/scripts/init_storage.py), а не через этот shell-скрипт.

Это значит, что `init-buckets.sh` присутствует как инфраструктурный артефакт, но фактическая рабочая логика подготовки префиксов проекта реализована на Python.

## WrenAI-контур в compose

### `bootstrap`

Подготавливает данные и начальную структуру для Wren-среды.

### `wren-engine`

Служит одним из внутренних движков Wren-контейнера и использует том `wrenai-data`.

### `ibis-server`

Подключён к `wren-engine` и даёт отдельный endpoint для части SQL-функциональности Wren.

### `qdrant`

Используется как векторное хранилище.

### `wren-ai-service`

Это AI-сервис Wren, которому передаётся `infra/wrenai/config.yaml`.
Он зависит от `qdrant`, а параметры для внешних сервисов и модели берёт через `.env.docker`.

### `wren-ui`

Это внешний интерфейс Wren.
Он зависит от `wren-engine`, `ibis-server` и `wren-ai-service`.
Данные интерфейса хранятся в `wrenai-data`.

## `infra/wrenai/config.yaml`

### Общая структура

Файл разбит на несколько логических секций:

- LLM
- embedder
- engine
- document_store
- pipeline
- settings

### LLM

Секция LLM описывает:

- провайдер `litellm_llm`
- модель `gpt-4o`
- `context_window_size=128000`
- `temperature=0`

Это значит, что текущая конфигурация ориентирована на детерминированное поведение без случайности в генерации.

### Embedder

Эмбеддером выбрана модель `text-embedding-3-small`.
Она используется для индексирования и retrieval в Wren-конвейерах.

### Engine

В конфиге указаны два engine:

- `wren_ui`
- `wren_ibis`

Это отражает внутреннее разделение движков внутри WrenAI-контура.

### Document store

В качестве хранилища векторов используется Qdrant:

- `location: http://qdrant:6333`
- `embedding_model_dim: 1536`

### Pipeline

Самая объёмная часть файла — список конвейеров.
В нём перечислены этапы:

- индексирования схемы;
- retrieval;
- генерации SQL;
- коррекции SQL;
- диагностики;
- генерации follow-up SQL;
- индексирования инструкций и SQL-пар;
- SQL-execution;
- генерации графиков и вспомогательных ответов.

### Settings

В секции `settings` задаются:

- размеры retrieval;
- таймауты;
- флаги reasoning и intent classification;
- параметры кэша;
- параметры logging;
- similarity thresholds.

## Что важно помнить о текущем состоянии инфраструктуры

Инфраструктурный стек проекта уже рабочий:

- MinIO, PostgreSQL, Hive Metastore и Trino достаточно настроены для локального lakehouse;
- WrenAI-контур тоже описан и может запускаться отдельным профилем.

Но при этом важно честно отделять:

- фактически работающий контур данных;
- и развиваемую BI-часть, у которой инфраструктурный каркас уже есть, а смысловое наполнение ещё не полностью завершено.

## Краткий итог

Инфраструктурные конфиги проекта образуют последовательную систему:

- `docker-compose.yml` связывает сервисы;
- `infra/hive/` позволяет Hive Metastore работать с PostgreSQL и MinIO;
- `infra/trino/` даёт Trino два каталога — `hive` и `iceberg`;
- `infra/wrenai/config.yaml` описывает BI-контур поверх Trino;
- служебные артефакты вроде `init-buckets.sh` и томов дополняют локальную среду.

Если понимать эти файлы вместе, становится ясно, как проект соединяет объектное хранение, каталог метаданных, SQL-движок и BI-публикацию в единый локальный `lakehouse`.
