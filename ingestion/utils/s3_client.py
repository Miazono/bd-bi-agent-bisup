from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


@dataclass
class S3ObjectInfo:
    key: str
    size: int
    etag: Optional[str] = None


class S3Client:
    """
    Env-driven MinIO client.

    Приоритет для кредов:
    1) MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
    2) S3_ACCESS_KEY / S3_SECRET_KEY
    """

    def __init__(self) -> None:
        endpoint_url = _get_env("S3_ENDPOINT", "http://minio:9000")
        access_key = _get_env("S3_ACCESS_KEY")
        secret_key = _get_env("S3_SECRET_KEY")
        region = _get_env("MINIO_REGION", "us-east-1")

        if not access_key or not secret_key:
            raise ValueError("S3 credentials are not set. Check MINIO_ROOT_* or S3_* env vars.")

        parsed = urlparse(endpoint_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid S3/MinIO endpoint: {endpoint_url}")

        self.table_scheme = _get_env("S3_TABLE_SCHEME", "s3a")
        self.endpoint_url = endpoint_url
        self.region = region

        # Minio(...) ждёт endpoint без http://
        endpoint = parsed.netloc
        secure = parsed.scheme == "https"

        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )

    @staticmethod
    def normalize_key(key: str) -> str:
        return key.lstrip("/")

    def bucket_exists(self, bucket: str) -> bool:
        try:
            return self._client.bucket_exists(bucket)
        except S3Error:
            return False

    def ensure_bucket(self, bucket: str) -> None:
        if self.bucket_exists(bucket):
            return
        self._client.make_bucket(bucket, location=self.region)

    def object_exists(self, bucket: str, key: str) -> bool:
        key = self.normalize_key(key)
        try:
            self._client.stat_object(bucket, key)
            return True
        except S3Error:
            return False

    def head_object(self, bucket: str, key: str) -> Optional[S3ObjectInfo]:
        key = self.normalize_key(key)
        try:
            obj = self._client.stat_object(bucket, key)
            etag = getattr(obj, "etag", None)
            size = int(getattr(obj, "size", 0))
            return S3ObjectInfo(
                key=key,
                size=size,
                etag=etag or None,
            )
        except S3Error:
            return None

    def list_objects(self, bucket: str, prefix: str) -> List[S3ObjectInfo]:
        prefix = self.normalize_key(prefix)
        results: List[S3ObjectInfo] = []

        try:
            for obj in self._client.list_objects(bucket, prefix=prefix, recursive=True):
                results.append(
                    S3ObjectInfo(
                        key=obj.object_name,
                        size=int(getattr(obj, "size", 0)),
                        etag=getattr(obj, "etag", None) or None,
                    )
                )
        except S3Error:
            return []

        return results

    def upload_file(self, local_path: str, bucket: str, key: str, content_type: str = "application/octet-stream") -> None:
        key = self.normalize_key(key)
        self._client.fput_object(
            bucket_name=bucket,
            object_name=key,
            file_path=local_path,
            content_type=content_type,
        )

    def build_table_uri(self, bucket: str, key_or_prefix: str) -> str:
        key_or_prefix = self.normalize_key(key_or_prefix)
        return f"{self.table_scheme}://{bucket}/{key_or_prefix}"

    def build_dir_uri(self, bucket: str, prefix: str) -> str:
        prefix = self.normalize_key(prefix).rstrip("/") + "/"
        return self.build_table_uri(bucket, prefix)