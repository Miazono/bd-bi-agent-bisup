# Краткая схема `load_raw.py`

## Назначение

Загрузить исходные CSV-файлы в MinIO в слой `raw` и сделать это идемпотентно по MD5.

## Входы

- `--load-date`
- `--source-dir`
- `settings.s3_endpoint`
- `settings.s3_access_key`
- `settings.s3_secret_key`
- `settings.lakehouse_bucket`
- `settings.raw_prefix`

## Ключевые константы

- `SOURCE_FILES` - фиксированный список исходных файлов
- `settings.raw_prefix` - корневой префикс слоя `raw`

## Функции

- `parse_args()` - читает аргументы CLI
- `build_minio_client()` - создаёт MinIO-клиент
- `compute_md5(file_path)` - считает MD5 локального файла
- `get_object_md5(client, bucket, object_name)` - читает MD5 из метаданных объекта
- `upload_file(...)` - загружает один файл с проверкой MD5
- `main()` - собирает весь сценарий загрузки

## Порядок выполнения

1. Разобрать аргументы.
2. Проверить, что указана директория с CSV.
3. Проверить конфигурацию бакета и префикса.
4. Создать MinIO-клиент.
5. Проверить существование бакета.
6. Последовательно загрузить `articles.csv`, `customers.csv`, `transactions_train.csv`.
7. Завершиться логом об успехе.

## Выходы

- Объекты в MinIO по путям вида `raw/hm/.../load_date=YYYY-MM-DD/...`
- Лог о пропущенных и загруженных файлах

## Зависимости

- `Minio`
- `S3Error`
- `config.settings.settings`
- стандартные модули `argparse`, `hashlib`, `logging`, `os`, `sys`, `datetime`, `urllib.parse`

## Где Может Сломаться

- Нет исходного файла в локальной директории
- Нет бакета `lakehouse`
- Не заданы S3/MinIO переменные
- Изменился layout raw-путей
- Файл уже есть, но MD5 читается некорректно из метаданных
