# Локальный запуск пайплайна

## Требования

- Python 3.11+
- Docker и Docker Compose
- CSV-файлы `articles.csv`, `customers.csv`, `transactions_train.csv`

## Подготовка окружения

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.local.example .env.local
cp .env.docker.example .env.docker

set -a && source .env.local && set +a
```

`.env.local` используется для локального запуска Python-скриптов, а `.env.docker` — для запуска контейнеров внутри docker-сети.

## Подъём локального стека

```bash
make up
```

Команда поднимает:

- `minio`;
- `metastore-db`;
- `hive-metastore`;
- `trino`.

Проверить состояние контейнеров можно стандартной командой `docker compose ps`.

## Инициализация хранилища

Перед первой загрузкой нужно создать бакет и служебные префиксы слоёв:

```bash
make init-storage
```

Скрипт создаёт бакет `LAKEHOUSE_BUCKET` и префиксы:

- `raw/`;
- `bronze/`;
- `silver/`;
- `marts/`.

## Подготовка исходных файлов

По умолчанию команда `make load-raw` читает файлы из директории `data/raw`.
В этой директории должны лежать:

- `data/raw/articles.csv`;
- `data/raw/customers.csv`;
- `data/raw/transactions_train.csv`.

Если исходные файлы лежат в другом каталоге, путь можно явно передать через `--source-dir`.

## Загрузка raw

### Через Makefile

```bash
make load-raw
```

### Напрямую

```bash
APP_ENV=local python -m ingestion.load_raw \
  --load-date 2026-03-19 \
  --source-dir data/raw
```

Скрипт загружает файлы в MinIO по путям:

- `raw/hm/articles/load_date=YYYY-MM-DD/articles.csv`;
- `raw/hm/customers/load_date=YYYY-MM-DD/customers.csv`;
- `raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv`.

Если MD5 файла не изменился, объект повторно не загружается.

## Загрузка bronze

### Через Makefile

```bash
make load-bronze
```

### Напрямую

```bash
APP_ENV=local python -m ingestion.load_bronze \
  --load-date 2026-03-08 \
  --batch-id hm_20260308_01
```

Во время загрузки скрипт:

- проверяет наличие raw-файлов для указанной даты;
- создаёт внешние таблицы `hive.raw.hm_*_raw`;
- загружает batch в `iceberg.bronze.*`.

## Загрузка silver

### Через Makefile

```bash
make load-silver
```

### Напрямую

```bash
APP_ENV=local python -m ingestion.load_silver \
  --batch-id hm_20260308_01 \
  --stats-prefix-len 1
```

Полезные опции:

- `--months 2020-09-01` — обработать только выбранные месячные части `fact_sales_line`;
- `--skip-stats` — пропустить обновление `silver.fact_customer_article_stats`;
- `--stats-prefix-len 2` — изменить длину префикса для безопасной повторной пересборки агрегата.

## Память Trino для полного локального прогона

Для локального прогона полного batch в репозитории настроен повышенный профиль памяти Trino:

- `infra/trino/jvm.config`: `-Xmx8G`;
- `infra/trino/config.properties`: `query.max-memory-per-node=6GB`;
- `infra/trino/config.properties`: `query.max-memory=6GB`;
- `infra/trino/config.properties`: `query.max-total-memory=7GB`.

Это не отменяет разбивку `fact_sales_line` по месяцам, а только увеличивает допустимый объём памяти для тяжёлых запросов.

Если локальная машина слабее, лучше:

- обрабатывать только часть месяцев через `--months`;
- временно использовать `--skip-stats`;
- уменьшить параметры памяти Trino под доступные ресурсы.

## Загрузка витрин marts

### Через Makefile

```bash
make load-marts
```

### Напрямую

```bash
APP_ENV=local python -m ingestion.load_marts
```

Скрипт создаёт физические Iceberg-таблицы в схеме `iceberg.mart` и полностью пересобирает витрины из `silver`.

## Подъём WrenAI

WrenAI запускается отдельным профилем `wrenai`:

```bash
make up-wrenai
```

После старта доступны:

- интерфейс: `http://localhost:3000`;
- AI-сервис: `http://localhost:5555`.

Для остановки:

```bash
make down-wrenai
```

При подключении к Trino в WrenAI следует ориентироваться на витрины схемы `iceberg.mart`.

## Полезные команды

```bash
pytest tests/ -v
python3 scripts/gen_schema.py
docker compose --env-file .env.docker logs -f trino
```
