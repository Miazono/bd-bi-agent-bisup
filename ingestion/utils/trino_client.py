from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

import trino
from trino.dbapi import connect

from config.settings import settings



class TrinoClient:
    """
    Env-driven Trino DBAPI client.
    Поддерживает:
    - TRINO_COORDINATOR=http://host:port
    - либо TRINO_HOST / TRINO_PORT / TRINO_HTTP_SCHEME
    """

    def __init__(self) -> None:
        coordinator = settings.trino_coordinator
        parsed = urlparse(coordinator) if coordinator else None

        host = (
            (parsed.hostname if parsed and parsed.hostname else None)
            or settings.trino_host
        )
        port = (
            (parsed.port if parsed and parsed.port else None)
            or int(settings.trino_port)
        )
        http_scheme = (
            (parsed.scheme if parsed and parsed.scheme else None)
            or settings.trino_http_schema
        )

        self.user = settings.minio_root_user
        self.catalog = settings.trino_catalog
        self.schema = settings.trino_schema

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