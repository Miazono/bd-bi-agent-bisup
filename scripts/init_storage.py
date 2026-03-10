"""
Инициализация структуры бакетов в MinIO.
Создаёт бакет и префиксы-папки для всех слоёв lakehouse.
Безопасно запускать повторно — уже существующие объекты пропускаются.
"""
from __future__ import annotations

import io
import logging
import os

from ingestion.utils.s3_client import S3Client
from config.settings import settings

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

PREFIXES = [
    settings.raw_prefix,
    settings.bronze_prefix,
    settings.silver_prefix,
    settings.mart_prefix,
]


def init_storage() -> None:
    s3 = S3Client()
    bucket = settings.lakehouse_bucket

    # Создаём бакет если не существует
    if s3.bucket_exists(bucket):
        logger.info("Bucket already exists: %s", bucket)
    else:
        s3.ensure_bucket(bucket)
        logger.info("Created bucket: %s", bucket)

    # Создаём маркер-папку для каждого слоя
    for prefix in PREFIXES:
        key = f"{prefix.rstrip('/')}/.keep"
        if s3.object_exists(bucket, key):
            logger.info("Already exists: s3://%s/%s", bucket, key)
        else:
            s3._client.put_object(
                bucket_name=bucket,
                object_name=key,
                data=io.BytesIO(b""),
                length=0,
            )
            logger.info("Created: s3://%s/%s", bucket, key)

    logger.info("Storage initialized successfully")


if __name__ == "__main__":
    init_storage()
