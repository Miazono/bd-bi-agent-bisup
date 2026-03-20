"""
Инициализация структуры бакета в MinIO.

Это учебная, подробно прокомментированная копия исходного скрипта.
Она повторяет структуру `scripts/init_storage.py`, но добавляет пояснения
к каждому шагу, чтобы код можно было читать как конспект.
"""
from __future__ import annotations

import io
import logging
import os

from config.settings import settings
from ingestion.utils.s3_client import S3Client


# Базовая настройка журнала нужна, чтобы при запуске скрипта сразу видеть,
# что именно он сделал: создал бакет, создал маркер или пропустил шаг.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# Маркеры всех слоёв lakehouse.
# Скрипт не создаёт "папки" в файловом смысле, а кладёт пустой объект `.keep`
# в каждый префикс, чтобы структура бакета была видна и стабильна.
PREFIXES = [
    settings.raw_prefix,
    settings.bronze_prefix,
    settings.silver_prefix,
    settings.mart_prefix,
]


def init_storage() -> None:
    """
    Подготавливает бакет и служебные префиксы.

    Смысл функции:
    - обеспечить наличие бакета lakehouse;
    - создать маркеры слоёв raw/bronze/silver/marts;
    - сделать повторный запуск безопасным.
    """
    # MinIO-клиент собирается из централизованных настроек проекта.
    s3 = S3Client()
    bucket = settings.lakehouse_bucket

    # Сначала проверяем бакет.
    # Если он уже есть, не трогаем его повторно.
    if s3.bucket_exists(bucket):
        logger.info("Bucket already exists: %s", bucket)
    else:
        # Если бакета нет, создаём его через обёртку S3Client.
        s3.ensure_bucket(bucket)
        logger.info("Created bucket: %s", bucket)

    # Затем создаём маркеры для каждого префикса слоя.
    # Это не обязательные данные, а технические объекты для удобства структуры.
    for prefix in PREFIXES:
        key = f"{prefix.rstrip('/')}/.keep"

        # Если маркер уже лежит в бакете, повторно его не создаём.
        if s3.object_exists(bucket, key):
            logger.info("Already exists: s3://%s/%s", bucket, key)
        else:
            # Пустой объект длиной 0 байт создаёт видимую "точку" слоя в бакете.
            s3._client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=io.BytesIO(b""),
                length=0,
            )
            logger.info("Created: s3://%s/%s", bucket, key)

    logger.info("Storage initialized successfully")


if __name__ == "__main__":
    # При прямом запуске скрипт просто инициализирует структуру хранилища.
    init_storage()
