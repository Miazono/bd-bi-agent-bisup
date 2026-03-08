import os
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def trino_config():
    return {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": os.getenv("TRINO_PORT", "8080"),
        "catalog": os.getenv("TRINO_CATALOG", "iceberg"),
        "schema": os.getenv("TRINO_SCHEMA", "default"),
    }


@pytest.fixture
def trino_cursor():
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.return_value = []
    return cursor


@pytest.fixture
def trino_conn(trino_cursor, trino_config):
    conn = MagicMock()
    conn.cursor.return_value = trino_cursor
    conn.config = trino_config
    return conn


@pytest.fixture
def minio_config():
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minio"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    }


@pytest.fixture
def minio_client(minio_config):
    client = MagicMock()
    client.bucket_exists.return_value = True
    client.config = minio_config
    return client
