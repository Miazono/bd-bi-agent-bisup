from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Tuple

from config.settings import settings
from ingestion.utils.sql_assets import SqlAssets
from ingestion.utils.trino_client import TrinoClient

# Единая настройка логов для финальной стадии пересборки витрин.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# Общие параметры слоя marts.
LAKEHOUSE_BUCKET = settings.lakehouse_bucket
MART_PREFIX = settings.mart_prefix
SQL_ASSETS = SqlAssets()


def mart_ddl_paths() -> List[Path]:
    # Все физические витрины создаются из явного набора DDL-файлов.
    base = SQL_ASSETS.path("sql", "ddl", "mart")
    return [
        base / "mart_sales_daily_channel.sql",
        base / "mart_sales_monthly_category.sql",
        base / "mart_customer_segment_monthly.sql",
        base / "mart_repeat_purchase_category.sql",
        base / "mart_customer_rfm_monthly.sql",
    ]


def mart_query_specs() -> List[Tuple[str, Path]]:
    # Физическая таблица и SQL-файл наполнения связаны в одном месте.
    base = SQL_ASSETS.path("sql", "queries", "mart")
    return [
        ("iceberg.mart.sales_daily_channel", base / "mart_sales_daily_channel.sql"),
        ("iceberg.mart.sales_monthly_category", base / "mart_sales_monthly_category.sql"),
        ("iceberg.mart.customer_segment_monthly", base / "mart_customer_segment_monthly.sql"),
        ("iceberg.mart.repeat_purchase_category", base / "mart_repeat_purchase_category.sql"),
        ("iceberg.mart.customer_rfm_monthly", base / "mart_customer_rfm_monthly.sql"),
    ]


def execute_step(trino: TrinoClient, sql: str, title: str) -> None:
    # Небольшая обёртка для единообразного логирования всех SQL-шагов.
    logger.info("Start: %s", title)
    trino.execute(sql)
    logger.info("Done: %s", title)


def parse_fqn(table_name: str) -> tuple[str, str, str]:
    # Нужно для обращения к Iceberg metadata tables вида "$files".
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected fully qualified table name catalog.schema.table, got: {table_name}")
    return parts[0], parts[1], parts[2]


def log_row_count(trino: TrinoClient, table_name: str) -> None:
    # После пересборки полезно видеть реальное число строк в витрине.
    row = trino.fetchone(f"SELECT COUNT(*) FROM {table_name}")
    count = row[0] if row else 0
    logger.info("Rows in %s: %s", table_name, count)


def log_iceberg_files_summary(trino: TrinoClient, table_name: str) -> None:
    # Проверяем не только строки, но и то, как таблица материализована в Iceberg.
    catalog, schema, table = parse_fqn(table_name)
    row = trino.fetchone(
        f"""
        SELECT
          COALESCE(SUM(record_count), 0) AS rows_from_files,
          COUNT(*) AS file_count,
          COALESCE(SUM(file_size_in_bytes), 0) AS total_bytes
        FROM {catalog}.{schema}."{table}$files"
        """
    )
    if row:
        logger.info(
            "Iceberg files summary | %s | rows=%s | files=%s | bytes=%s",
            table_name,
            row[0],
            row[1],
            row[2],
        )


def ensure_mart_schema(trino: TrinoClient) -> None:
    # Физическое место хранения витрин в MinIO.
    scheme = settings.s3_table_scheme
    location = f"{scheme}://{LAKEHOUSE_BUCKET}/{MART_PREFIX.rstrip('/')}/"

    execute_step(
        trino,
        f"""
        CREATE SCHEMA IF NOT EXISTS iceberg.mart
        WITH (location = '{location}')
        """,
        "Create schema iceberg.mart",
    )


def ensure_mart_tables(trino: TrinoClient) -> None:
    # Таблицы витрин создаются отдельно от SQL-логики наполнения.
    for ddl_path in mart_ddl_paths():
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_path}")
        logger.info("Applying DDL: %s", ddl_path)
        trino.execute_file(ddl_path)


def rebuild_mart_table(trino: TrinoClient, table_name: str, query_path: Path) -> None:
    # Каждая витрина пересобирается полностью: сначала очистка, потом новый INSERT.
    if not query_path.exists():
        raise FileNotFoundError(f"Mart query file not found: {query_path}")

    insert_sql = query_path.read_text(encoding="utf-8").strip().rstrip(";")
    execute_step(trino, f"DELETE FROM {table_name}", f"Clear {table_name}")
    execute_step(trino, insert_sql, f"Populate {table_name}")


def main() -> None:
    # Скрипт намеренно фиксирован и не принимает CLI-аргументы.
    trino = TrinoClient()

    logger.info("Ensuring mart schema")
    ensure_mart_schema(trino)

    logger.info("Ensuring mart tables")
    ensure_mart_tables(trino)

    for table_name, query_path in mart_query_specs():
        logger.info("Rebuilding mart table from %s", query_path)
        rebuild_mart_table(trino, table_name, query_path)
        log_row_count(trino, table_name)
        log_iceberg_files_summary(trino, table_name)

    logger.info("Mart load finished successfully")


if __name__ == "__main__":
    main()
