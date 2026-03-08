from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

import trino
from trino.dbapi import connect

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


class TrinoClient:
    """
    Env-driven Trino DBAPI client.
    Поддерживает:
    - TRINO_COORDINATOR=http://host:port
    - либо TRINO_HOST / TRINO_PORT / TRINO_HTTP_SCHEME
    """

    def __init__(self) -> None:
        coordinator = _get_env("TRINO_COORDINATOR")
        parsed = urlparse(coordinator) if coordinator else None

        host = (
            (parsed.hostname if parsed and parsed.hostname else None)
            or _get_env("TRINO_HOST", "localhost")
        )
        port = (
            (parsed.port if parsed and parsed.port else None)
            or int(_get_env("TRINO_PORT", "8080"))
        )
        http_scheme = (
            (parsed.scheme if parsed and parsed.scheme else None)
            or _get_env("TRINO_HTTP_SCHEME", "http")
        )

        self.user = _get_env("TRINO_USER", "lakehouse")
        self.catalog = _get_env("TRINO_CATALOG", "iceberg")
        self.schema = _get_env("TRINO_SCHEMA", "default")

        self._conn = connect(
            host=host,
            port=port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
            http_scheme=http_scheme,
        )

    def execute(self, sql: str) -> list[tuple]:
        sql = sql.strip().rstrip(";")
        cur = self._conn.cursor()
        cur.execute(sql)

        try:
            rows = cur.fetchall()
        except Exception:
            rows = []

        cur.close()
        return rows

    def fetchone(self, sql: str) -> Any:
        rows = self.execute(sql)
        return rows[0] if rows else None

    def fetchall(self, sql: str) -> list[tuple]:
        return self.execute(sql)

    def execute_file(self, path: str | Path) -> list[tuple]:
        sql = Path(path).read_text(encoding="utf-8").strip()
        return self.execute(sql)