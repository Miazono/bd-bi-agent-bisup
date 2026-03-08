import argparse
import hashlib
import logging
import os
import sys
from datetime import date
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


SOURCE_FILES = [
    "articles.csv",
    "customers.csv",
    "transactions_train.csv",
]


def parse_args():
    """Парсит параметры запуска."""
    parser = argparse.ArgumentParser(
        description="Загрузка raw CSV в MinIO",
    )
    parser.add_argument(
        "--load-date",
        default=date.today().isoformat(),
        help="Дата загрузки в формате YYYY-MM-DD (по умолчанию: сегодня)",
    )
    parser.add_argument(
        "--source-dir",
        default=os.getenv("RAW_SOURCE_DIR"),
        help="Директория с исходными CSV (можно через RAW_SOURCE_DIR)",
    )
    return parser.parse_args()


def build_minio_client():
    """Создаёт MinIO-клиент на основе переменных окружения."""
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    missing = [name for name, value in {
        "MINIO_ENDPOINT": endpoint,
        "MINIO_ROOT_USER": access_key,
        "MINIO_ROOT_PASSWORD": secret_key,
    }.items() if not value]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    parsed = urlparse(endpoint)
    if parsed.scheme:
        secure = parsed.scheme == "https"
        host = parsed.netloc or parsed.path
    else:
        secure = False
        host = endpoint

    return Minio(
        host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def compute_md5(file_path):
    """Считает MD5 файла."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_object_md5(client, bucket, object_name):
    """Возвращает MD5 из метаданных объекта, если он существует."""
    try:
        stat = client.stat_object(bucket, object_name)
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject"}:
            return None
        raise

    metadata = {key.lower(): value for key, value in (stat.metadata or {}).items()}
    return (
        metadata.get("x-amz-meta-md5")
        or metadata.get("md5")
    )


def upload_file(client, bucket, raw_prefix, load_date, source_dir, filename):
    """Загружает файл в raw-слой с проверкой MD5."""
    file_path = os.path.join(source_dir, filename)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")

    object_name = f"{raw_prefix}/hm/load_date={load_date}/{filename}"
    local_md5 = compute_md5(file_path)
    remote_md5 = get_object_md5(client, bucket, object_name)

    if remote_md5 == local_md5:
        logging.info("already uploaded: %s", object_name)
        return

    client.fput_object(
        bucket,
        object_name,
        file_path,
        content_type="text/csv",
        metadata={"md5": local_md5},
    )
    logging.info("uploaded: %s", object_name)


def main():
    """Загружает CSV в raw-слой MinIO."""
    args = parse_args()

    source_dir = args.source_dir
    if not source_dir:
        logging.error("Source dir is required (use --source-dir or RAW_SOURCE_DIR).")
        sys.exit(1)

    bucket = os.getenv("LAKEHOUSE_BUCKET")
    raw_prefix = os.getenv("RAW_PREFIX")
    missing = [name for name, value in {
        "LAKEHOUSE_BUCKET": bucket,
        "RAW_PREFIX": raw_prefix,
    }.items() if not value]
    if missing:
        logging.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)

    client = build_minio_client()

    if not client.bucket_exists(bucket):
        logging.error("Bucket does not exist: %s", bucket)
        sys.exit(1)

    for filename in SOURCE_FILES:
        upload_file(
            client=client,
            bucket=bucket,
            raw_prefix=raw_prefix.strip("/"),
            load_date=args.load_date,
            source_dir=source_dir,
            filename=filename,
        )


if __name__ == "__main__":
    main()
