from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path
from typing import Dict, List

from config.settings import settings
from ingestion.utils.sql_assets import SqlAssets
from ingestion.utils.trino_client import TrinoClient

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

LAKEHOUSE_BUCKET = settings.lakehouse_bucket
SILVER_PREFIX = settings.silver_prefix
SQL_ASSETS = SqlAssets()


def q(value: str) -> str:
    return value.replace("'", "''")

def silver_ddl_paths() -> List[Path]:
    base = SQL_ASSETS.path("sql", "ddl", "silver")
    return [
        base / "silver_dim_article.sql",
        base / "silver_dim_customer.sql",
        base / "silver_dim_date.sql",
        base / "silver_fact_sales_line.sql",
        base / "silver_fact_customer_article_stats.sql",
    ]


def silver_query_paths() -> Dict[str, Path]:
    base = SQL_ASSETS.path("sql", "queries", "silver")
    return {
        "refresh_dim_article_delete": base / "refresh_dim_article_delete.sql",
        "refresh_dim_article_insert": base / "refresh_dim_article_insert.sql",
        "refresh_dim_customer_delete": base / "refresh_dim_customer_delete.sql",
        "refresh_dim_customer_insert": base / "refresh_dim_customer_insert.sql",
        "upsert_dim_date": base / "upsert_dim_date.sql",
        "refresh_fact_sales_line_delete": base / "refresh_fact_sales_line_delete.sql",
        "refresh_fact_sales_line_insert": base / "refresh_fact_sales_line_insert.sql",
        "merge_fact_customer_article_stats_batch_delta": base / "merge_fact_customer_article_stats_batch_delta.sql",
        "delete_impacted_stats_prefix": base / "delete_impacted_stats_prefix.sql",
        "insert_impacted_stats_prefix": base / "insert_impacted_stats_prefix.sql",
    }


def execute_step(trino: TrinoClient, sql: str, title: str) -> None:
    logger.info("Start: %s", title)
    trino.execute(sql)
    logger.info("Done: %s", title)


def log_row_count(trino: TrinoClient, table_name: str) -> None:
    row = trino.fetchone(f"SELECT COUNT(*) FROM {table_name}")
    count = row[0] if row else 0
    logger.info("Rows in %s: %s", table_name, count)


def parse_fqn(table_name: str) -> tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected fully qualified table name catalog.schema.table, got: {table_name}")
    return parts[0], parts[1], parts[2]


def log_iceberg_files_summary(trino: TrinoClient, table_name: str) -> None:
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


def log_batch_row_count(trino: TrinoClient, table_name: str, batch_id: str) -> None:
    batch = q(batch_id)
    row = trino.fetchone(
        f"SELECT COUNT(*) FROM {table_name} WHERE batch_id = '{batch}'"
    )
    count = row[0] if row else 0
    logger.info("Rows in %s for batch=%s: %s", table_name, batch_id, count)


def validate_bronze_batch(trino: TrinoClient, batch_id: str) -> Dict[str, int]:
    batch = q(batch_id)

    sources = {
        "hm_articles": "iceberg.bronze.hm_articles",
        "hm_customers": "iceberg.bronze.hm_customers",
        "hm_transactions": "iceberg.bronze.hm_transactions",
    }

    counts: Dict[str, int] = {}
    for name, table in sources.items():
        row = trino.fetchone(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id = '{batch}'"
        )
        count = int(row[0]) if row else 0
        counts[name] = count
        logger.info("Bronze source %s | batch=%s | rows=%s", table, batch_id, count)

    missing = [name for name, count in counts.items() if count == 0]
    if missing:
        raise RuntimeError(
            f"Bronze batch {batch_id!r} is incomplete or absent. "
            f"No rows found in: {', '.join(missing)}"
        )

    return counts


def ensure_silver_schema(trino: TrinoClient) -> None:
    scheme = settings.s3_table_scheme
    location = f"{scheme}://{LAKEHOUSE_BUCKET}/{SILVER_PREFIX.rstrip('/')}/"

    execute_step(
        trino,
        f"""
        CREATE SCHEMA IF NOT EXISTS iceberg.silver
        WITH (location = '{location}')
        """,
        "Create schema iceberg.silver",
    )


def ensure_silver_tables(trino: TrinoClient) -> None:
    for ddl_path in silver_ddl_paths():
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_path}")
        logger.info("Applying DDL: %s", ddl_path)
        trino.execute_file(ddl_path)


def refresh_dim_article(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)
    delete_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_dim_article_delete.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(trino, delete_sql, f"Delete impacted article_ids for batch={batch_id}")
    insert_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_dim_article_insert.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(trino, insert_sql, f"Insert impacted article_ids for batch={batch_id}")


def refresh_dim_customer(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)
    delete_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_dim_customer_delete.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(trino, delete_sql, f"Delete impacted customer_ids for batch={batch_id}")
    insert_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_dim_customer_insert.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(trino, insert_sql, f"Insert impacted customer_ids for batch={batch_id}")


def upsert_dim_date(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)
    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "upsert_dim_date.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(trino, sql, f"Upsert dim_date for batch={batch_id}")


def get_batch_months(trino: TrinoClient, batch_id: str) -> List[str]:
    batch = q(batch_id)
    rows = trino.fetchall(
        f"""
        SELECT CAST(date_trunc('month', t_dat) AS DATE) AS month_start
        FROM iceberg.bronze.hm_transactions
        WHERE batch_id = '{batch}'
          AND t_dat IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """
    )

    months: List[str] = []
    for row in rows:
        value = row[0]
        if isinstance(value, date):
            months.append(value.isoformat())
        else:
            months.append(str(value))
    return months


def parse_months_arg(months_arg: str | None) -> List[str]:
    if not months_arg:
        return []
    months = [value.strip() for value in months_arg.split(",") if value.strip()]
    if not months:
        return []

    normalized: List[str] = []
    for month_start in months:
        try:
            parsed = date.fromisoformat(month_start)
        except ValueError as exc:
            raise ValueError(
                f"Invalid month value {month_start!r}. Expected YYYY-MM-DD, for example 2020-09-01."
            ) from exc
        normalized.append(parsed.isoformat())

    return normalized


def resolve_months_to_process(batch_months: List[str], requested_months: List[str]) -> List[str]:
    if not batch_months:
        return []
    if not requested_months:
        return batch_months

    batch_months_set = set(batch_months)
    missing = [month for month in requested_months if month not in batch_months_set]
    if missing:
        raise RuntimeError(
            "Requested months are absent in the bronze batch: " + ", ".join(missing)
        )

    ordered = [month for month in batch_months if month in set(requested_months)]
    return ordered


def month_filter(column_name: str, month_start: str) -> str:
    return (
        f"{column_name} >= DATE '{month_start}' "
        f"AND {column_name} < date_add('month', 1, DATE '{month_start}')"
    )


def refresh_fact_sales_line_month(
    trino: TrinoClient,
    batch_id: str,
    month_start: str,
) -> None:
    batch = q(batch_id)
    month_predicate = month_filter("sale_date", month_start)
    bronze_month_predicate = month_filter("t_dat", month_start)

    delete_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_fact_sales_line_delete.sql",
        replacements={
            "BATCH_ID": batch,
            "SALE_MONTH_PREDICATE": month_predicate,
        },
    )
    execute_step(
        trino,
        delete_sql,
        f"Delete fact_sales_line batch={batch_id} month={month_start}",
    )

    insert_sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "refresh_fact_sales_line_insert.sql",
        replacements={
            "BATCH_ID": batch,
            "BRONZE_MONTH_PREDICATE": bronze_month_predicate,
        },
    )
    execute_step(
        trino,
        insert_sql,
        f"Insert fact_sales_line batch={batch_id} month={month_start}",
    )


def refresh_fact_sales_line_by_month(
    trino: TrinoClient,
    batch_id: str,
    requested_months: List[str] | None = None,
) -> None:
    batch_months = get_batch_months(trino, batch_id)
    months = resolve_months_to_process(batch_months, requested_months or [])

    if not months:
        raise RuntimeError(f"No months found in bronze.hm_transactions for batch {batch_id!r}")

    logger.info("Months to process for batch=%s: %s", batch_id, ", ".join(months))

    for month_start in months:
        logger.info("Processing fact_sales_line month chunk: %s", month_start)
        refresh_fact_sales_line_month(trino, batch_id, month_start)
        logger.info("Finished fact_sales_line month chunk: %s", month_start)


def fact_sales_line_batch_exists(trino: TrinoClient, batch_id: str) -> bool:
    batch = q(batch_id)
    row = trino.fetchone(
        f"""
        SELECT 1
        FROM iceberg.silver.fact_sales_line
        WHERE batch_id = '{batch}'
        LIMIT 1
        """
    )
    exists = row is not None
    logger.info(
        "Existing rows in silver.fact_sales_line for batch=%s before refresh: %s",
        batch_id,
        exists,
    )
    return exists


def merge_fact_customer_article_stats_batch_delta(
    trino: TrinoClient,
    batch_id: str,
) -> None:
    batch = q(batch_id)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "merge_fact_customer_article_stats_batch_delta.sql",
        replacements={"BATCH_ID": batch},
    )
    execute_step(
        trino,
        sql,
        f"Merge fact_customer_article_stats for new batch={batch_id}",
    )


def get_impacted_prefixes(
    trino: TrinoClient,
    batch_id: str,
    prefix_len: int,
) -> List[str]:
    batch = q(batch_id)

    rows = trino.fetchall(
        f"""
        SELECT DISTINCT substr(customer_id, 1, {prefix_len}) AS prefix_value
        FROM iceberg.bronze.hm_transactions
        WHERE batch_id = '{batch}'
          AND customer_id IS NOT NULL
          AND article_id IS NOT NULL
        ORDER BY 1
        """
    )
    return [str(row[0]) for row in rows if row[0] is not None]


def delete_impacted_stats_prefix(
    trino: TrinoClient,
    batch_id: str,
    prefix_len: int,
    prefix_value: str,
) -> None:
    batch = q(batch_id)
    prefix = q(prefix_value)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "delete_impacted_stats_prefix.sql",
        replacements={
            "BATCH_ID": batch,
            "PREFIX_LEN": str(prefix_len),
            "PREFIX_VALUE": prefix,
        },
    )
    execute_step(
        trino,
        sql,
        f"Delete impacted fact_customer_article_stats prefix={prefix_value}",
    )


def insert_impacted_stats_prefix(
    trino: TrinoClient,
    batch_id: str,
    prefix_len: int,
    prefix_value: str,
) -> None:
    batch = q(batch_id)
    prefix = q(prefix_value)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "silver",
        "insert_impacted_stats_prefix.sql",
        replacements={
            "BATCH_ID": batch,
            "PREFIX_LEN": str(prefix_len),
            "PREFIX_VALUE": prefix,
        },
    )
    execute_step(
      trino,
      sql,
      f"Insert impacted fact_customer_article_stats prefix={prefix_value}",
    )


def refresh_fact_customer_article_stats_incremental(
    trino: TrinoClient,
    batch_id: str,
    prefix_len: int,
) -> None:
    prefixes = get_impacted_prefixes(trino, batch_id, prefix_len)

    if not prefixes:
        logger.info("No impacted prefixes found for fact_customer_article_stats")
        return

    logger.info(
        "Refreshing fact_customer_article_stats by impacted prefixes (prefix_len=%s): %s",
        prefix_len,
        ", ".join(prefixes),
    )

    for prefix_value in prefixes:
        logger.info("Processing impacted stats prefix: %s", prefix_value)
        delete_impacted_stats_prefix(trino, batch_id, prefix_len, prefix_value)
        insert_impacted_stats_prefix(trino, batch_id, prefix_len, prefix_value)
        logger.info("Finished impacted stats prefix: %s", prefix_value)


def refresh_fact_customer_article_stats(
    trino: TrinoClient,
    batch_id: str,
    prefix_len: int,
    *,
    batch_already_loaded: bool,
) -> None:
    if batch_already_loaded:
        logger.info(
            "Batch %s already exists in silver.fact_sales_line; using safe rebuild path for fact_customer_article_stats",
            batch_id,
        )
        refresh_fact_customer_article_stats_incremental(trino, batch_id, prefix_len)
        return

    logger.info(
        "Batch %s is new for silver.fact_sales_line; using merge delta path for fact_customer_article_stats",
        batch_id,
    )
    merge_fact_customer_article_stats_batch_delta(trino, batch_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load silver layer from bronze via Trino"
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Bronze batch identifier to load into silver",
    )
    parser.add_argument(
        "--stats-prefix-len",
        type=int,
        default=2,
        help="How many first characters of customer_id to use for rerun-safe stats rebuild; default=2",
    )
    parser.add_argument(
        "--months",
        help="Optional comma-separated list of month starts to process for fact_sales_line, for example 2020-08-01,2020-09-01",
    )
    parser.add_argument(
        "--skip-stats",
        action="store_true",
        help="Skip refresh of silver.fact_customer_article_stats for faster smoke tests",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id
    stats_prefix_len = args.stats_prefix_len
    requested_months = parse_months_arg(args.months)

    trino = TrinoClient()

    logger.info("Validating bronze batch: %s", batch_id)
    validate_bronze_batch(trino, batch_id)

    logger.info("Ensuring silver schema")
    ensure_silver_schema(trino)

    logger.info("Ensuring silver tables")
    ensure_silver_tables(trino)

    fact_batch_already_loaded = False
    if not args.skip_stats:
        fact_batch_already_loaded = fact_sales_line_batch_exists(trino, batch_id)

    logger.info("Refreshing dim_article")
    refresh_dim_article(trino, batch_id)
    log_row_count(trino, "iceberg.silver.dim_article")

    logger.info("Refreshing dim_customer")
    refresh_dim_customer(trino, batch_id)
    log_row_count(trino, "iceberg.silver.dim_customer")

    logger.info("Upserting dim_date")
    upsert_dim_date(trino, batch_id)
    log_row_count(trino, "iceberg.silver.dim_date")

    logger.info("Refreshing base fact by month")
    refresh_fact_sales_line_by_month(trino, batch_id, requested_months)
    log_batch_row_count(trino, "iceberg.silver.fact_sales_line", batch_id)
    log_iceberg_files_summary(trino, "iceberg.silver.fact_sales_line")

    if args.skip_stats:
        logger.info("Skipping refresh of silver.fact_customer_article_stats because --skip-stats was provided")
    else:
        logger.info("Refreshing derived aggregate fact_customer_article_stats")
        refresh_fact_customer_article_stats(
            trino,
            batch_id,
            stats_prefix_len,
            batch_already_loaded=fact_batch_already_loaded,
        )
        log_iceberg_files_summary(trino, "iceberg.silver.fact_customer_article_stats")

    logger.info("Silver load finished successfully")


if __name__ == "__main__":
    main()
