# Краткая схема `init_storage.py`

## Назначение

Подготовка бакета MinIO и служебных префиксов слоёв `raw`, `bronze`, `silver`, `marts`.

## Входы

- `APP_ENV`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `MINIO_ENDPOINT`
- `LAKEHOUSE_BUCKET`
- `RAW_PREFIX`
- `BRONZE_PREFIX`
- `SILVER_PREFIX`
- `MART_PREFIX`

## Ключевые объекты

- `settings`
- `S3Client`
- `PREFIXES`
- `logger`

## Функции

- `init_storage()` - создаёт бакет и пустые объекты `.keep` для префиксов слоёв.

## Порядок выполнения

1. Создаёт `S3Client`.
2. Проверяет наличие бакета.
3. Создаёт бакет, если его нет.
4. Проверяет маркер `.keep` для каждого слоя.
5. Создаёт отсутствующие маркеры.

## Выходы

- бакет `lakehouse`;
- объекты `raw/.keep`, `bronze/.keep`, `silver/.keep`, `marts/.keep`;
- логи о созданных и уже существующих объектах.

## Зависимости

- `config/settings.py`
- `ingestion/utils/s3_client.py`

## Где может сломаться

- нет нужных переменных окружения;
- MinIO недоступен;
- бакет не создаётся из-за прав доступа;
- неверный endpoint или учётные данные.
