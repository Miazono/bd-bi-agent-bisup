# Локальный запуск пайплайна

## Требования

- Python 3.11+
- Запущенный стек (`docker compose up -d`)
- Исходные CSV-файлы: `articles.csv`, `customers.csv`, `transactions_train.csv`

## Первоначальная настройка

```bash
# 1. Создать виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate

# 2. Установить зависимости
pip install -r requirements.txt

# 3. Настроить переменные окружения
cp .env.example .env
# Отредактировать .env под свои значения

# 4. Загрузить переменные (нужно повторять при каждой новой сессии)
set -a && source .env && set +a
```

## Создание бакета в MinIO

Перед первым запуском пайплайна необходимо создать бакет в MinIO.
Имя бакета берётся из переменной `LAKEHOUSE_BUCKET` в `.env` (по умолчанию `lakehouse`).

**Способ 1 — через UI:**
1. Открыть `http://<domain>:9001`
2. Войти с логином/паролем из `.env` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`)
3. **Buckets → Create Bucket** → ввести `lakehouse` → Create

**Способ 2 — из консоли (переменные уже должны быть загружены):**
```bash
python3 -c "
import os
from minio import Minio
from urllib.parse import urlparse

endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
parsed = urlparse(endpoint)
host = parsed.netloc or parsed.path
secure = parsed.scheme == 'https'

client = Minio(host,
    access_key=os.getenv('MINIO_ROOT_USER'),
    secret_key=os.getenv('MINIO_ROOT_PASSWORD'),
    secure=secure
)
client.make_bucket('lakehouse')
print('Bucket lakehouse created')
"
```

## Загрузка raw-данных в MinIO

```bash
python ingestion/load_raw.py --source-dir /data/raw
```

Скрипт загружает три файла в бакет по пути `raw/hm/load_date=YYYY-MM-DD/`.
При повторном запуске файлы с неизменённым MD5 пропускаются автоматически.

> **Важно:** CSV-файлы должны называться точно `articles.csv`, `customers.csv`, `transactions_train.csv`.