from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path
from typing import Dict, List

from config.settings import settings
from ingestion.utils.trino_client import TrinoClient

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

LAKEHOUSE_BUCKET = settings.lakehouse_bucket
SILVER_PREFIX = settings.silver_prefix


def q(value: str) -> str:
    return value.replace("'", "''")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def silver_ddl_paths() -> List[Path]:
    base = repo_root() / "sql" / "ddl" / "silver"
    return [
        base / "silver_dim_article.sql",
        base / "silver_dim_customer.sql",
        base / "silver_dim_date.sql",
        base / "silver_fact_sales_line.sql",
        base / "silver_fact_customer_article_stats.sql",
    ]


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

    delete_sql = f"""
    DELETE FROM iceberg.silver.dim_article
    WHERE article_id IN (
      SELECT DISTINCT article_id
      FROM iceberg.bronze.hm_articles
      WHERE batch_id = '{batch}'
        AND article_id IS NOT NULL
    )
    """
    execute_step(trino, delete_sql, f"Delete impacted article_ids for batch={batch_id}")

    insert_sql = f"""
    INSERT INTO iceberg.silver.dim_article (
      article_id,
      product_code,
      prod_name,
      product_type_no,
      product_type_name,
      product_group_name,
      graphical_appearance_no,
      graphical_appearance_name,
      colour_group_code,
      colour_group_name,
      perceived_colour_value_id,
      perceived_colour_value_name,
      perceived_colour_master_id,
      perceived_colour_master_name,
      department_no,
      department_name,
      index_code,
      index_name,
      index_group_no,
      index_group_name,
      section_no,
      section_name,
      garment_group_no,
      garment_group_name,
      detail_desc,
      is_ladieswear,
      is_menswear,
      is_kids,
      color_family
    )
    WITH impacted_keys AS (
      SELECT DISTINCT article_id
      FROM iceberg.bronze.hm_articles
      WHERE batch_id = '{batch}'
        AND article_id IS NOT NULL
    ),
    ranked AS (
      SELECT
        b.article_id,
        b.product_code,
        b.prod_name,
        b.product_type_no,
        b.product_type_name,
        b.product_group_name,
        b.graphical_appearance_no,
        b.graphical_appearance_name,
        b.colour_group_code,
        b.colour_group_name,
        b.perceived_colour_value_id,
        b.perceived_colour_value_name,
        b.perceived_colour_master_id,
        b.perceived_colour_master_name,
        b.department_no,
        b.department_name,
        b.index_code,
        b.index_name,
        b.index_group_no,
        b.index_group_name,
        b.section_no,
        b.section_name,
        b.garment_group_no,
        b.garment_group_name,
        b.detail_desc,
        b.ingest_ts,
        b.batch_id,
        b.source_file_name,
        row_number() OVER (
          PARTITION BY b.article_id
          ORDER BY b.ingest_ts DESC, b.batch_id DESC, b.source_file_name DESC
        ) AS rn
      FROM iceberg.bronze.hm_articles b
      JOIN impacted_keys k
        ON b.article_id = k.article_id
    )
    SELECT
      article_id,
      product_code,
      prod_name,
      product_type_no,
      product_type_name,
      product_group_name,
      graphical_appearance_no,
      graphical_appearance_name,
      colour_group_code,
      colour_group_name,
      perceived_colour_value_id,
      perceived_colour_value_name,
      perceived_colour_master_id,
      perceived_colour_master_name,
      department_no,
      department_name,
      index_code,
      index_name,
      index_group_no,
      index_group_name,
      section_no,
      section_name,
      garment_group_no,
      garment_group_name,
      detail_desc,
      CASE
        WHEN regexp_like(
          lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
          '(^|[^a-z])(ladies|ladieswear)([^a-z]|$)'
        )
        THEN true
        ELSE false
      END AS is_ladieswear,
      CASE
        WHEN regexp_like(
          lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
          '(^|[^a-z])men([^a-z]|$)'
        )
        THEN true
        ELSE false
      END AS is_menswear,
      CASE
        WHEN regexp_like(
          lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
          '(^|[^a-z])(kids|kid|baby|children)([^a-z]|$)'
        )
        THEN true
        ELSE false
      END AS is_kids,
      CAST(NULL AS VARCHAR) AS color_family
    FROM ranked
    WHERE rn = 1
    """
    execute_step(trino, insert_sql, f"Insert impacted article_ids for batch={batch_id}")


def refresh_dim_customer(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)

    delete_sql = f"""
    DELETE FROM iceberg.silver.dim_customer
    WHERE customer_id IN (
      SELECT DISTINCT customer_id
      FROM iceberg.bronze.hm_customers
      WHERE batch_id = '{batch}'
        AND customer_id IS NOT NULL
    )
    """
    execute_step(trino, delete_sql, f"Delete impacted customer_ids for batch={batch_id}")

    insert_sql = f"""
    INSERT INTO iceberg.silver.dim_customer (
      customer_id,
      fn,
      active,
      club_member_status,
      fashion_news_frequency,
      age,
      postal_code,
      age_band,
      is_active_customer,
      is_fn_flag_present
    )
    WITH impacted_keys AS (
      SELECT DISTINCT customer_id
      FROM iceberg.bronze.hm_customers
      WHERE batch_id = '{batch}'
        AND customer_id IS NOT NULL
    ),
    ranked AS (
      SELECT
        b.customer_id,
        b.fn,
        b.active,
        b.club_member_status,
        b.fashion_news_frequency,
        b.age,
        b.postal_code,
        b.ingest_ts,
        b.batch_id,
        b.source_file_name,
        row_number() OVER (
          PARTITION BY b.customer_id
          ORDER BY b.ingest_ts DESC, b.batch_id DESC, b.source_file_name DESC
        ) AS rn
      FROM iceberg.bronze.hm_customers b
      JOIN impacted_keys k
        ON b.customer_id = k.customer_id
    )
    SELECT
      customer_id,
      fn,
      active,
      club_member_status,
      fashion_news_frequency,
      age,
      postal_code,
      CASE
        WHEN age IS NULL THEN 'unknown'
        WHEN age < 18 THEN 'under_18'
        WHEN age BETWEEN 18 AND 24 THEN '18_24'
        WHEN age BETWEEN 25 AND 34 THEN '25_34'
        WHEN age BETWEEN 35 AND 44 THEN '35_44'
        WHEN age BETWEEN 45 AND 54 THEN '45_54'
        WHEN age BETWEEN 55 AND 64 THEN '55_64'
        ELSE '65_plus'
      END AS age_band,
      CASE
        WHEN active = 1 THEN true
        ELSE false
      END AS is_active_customer,
      CASE
        WHEN fn IS NOT NULL THEN true
        ELSE false
      END AS is_fn_flag_present
    FROM ranked
    WHERE rn = 1
    """
    execute_step(trino, insert_sql, f"Insert impacted customer_ids for batch={batch_id}")


def upsert_dim_date(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)

    sql = f"""
    INSERT INTO iceberg.silver.dim_date (
      date_day,
      date_year,
      date_month,
      date_day_of_month,
      date_day_of_week,
      week_of_year
    )
    WITH bounds AS (
      SELECT
        min(t_dat) AS min_date,
        max(t_dat) AS max_date
      FROM iceberg.bronze.hm_transactions
      WHERE batch_id = '{batch}'
    ),
    dates AS (
      SELECT d AS date_day
      FROM bounds
      CROSS JOIN UNNEST(sequence(min_date, max_date)) AS t(d)
    )
    SELECT
      d.date_day,
      year(d.date_day) AS date_year,
      month(d.date_day) AS date_month,
      day(d.date_day) AS date_day_of_month,
      day_of_week(d.date_day) AS date_day_of_week,
      week(d.date_day) AS week_of_year
    FROM dates d
    WHERE NOT EXISTS (
      SELECT 1
      FROM iceberg.silver.dim_date x
      WHERE x.date_day = d.date_day
    )
    """
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

    delete_sql = f"""
    DELETE FROM iceberg.silver.fact_sales_line
    WHERE batch_id = '{batch}'
      AND {month_predicate}
    """
    execute_step(
        trino,
        delete_sql,
        f"Delete fact_sales_line batch={batch_id} month={month_start}",
    )

    insert_sql = f"""
    INSERT INTO iceberg.silver.fact_sales_line (
      sale_date,
      customer_id,
      article_id,
      price,
      sales_channel_id,
      ingest_ts,
      source_file_name,
      batch_id
    )
    SELECT
      t_dat AS sale_date,
      customer_id,
      article_id,
      price,
      sales_channel_id,
      ingest_ts,
      source_file_name,
      batch_id
    FROM iceberg.bronze.hm_transactions
    WHERE batch_id = '{batch}'
      AND {bronze_month_predicate}
    """
    execute_step(
        trino,
        insert_sql,
        f"Insert fact_sales_line batch={batch_id} month={month_start}",
    )


def refresh_fact_sales_line_by_month(trino: TrinoClient, batch_id: str) -> None:
    months = get_batch_months(trino, batch_id)

    if not months:
        raise RuntimeError(f"No months found in bronze.hm_transactions for batch {batch_id!r}")

    logger.info("Months to process for batch=%s: %s", batch_id, ", ".join(months))

    for month_start in months:
        logger.info("Processing fact_sales_line month chunk: %s", month_start)
        refresh_fact_sales_line_month(trino, batch_id, month_start)
        logger.info("Finished fact_sales_line month chunk: %s", month_start)


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

    sql = f"""
    DELETE FROM iceberg.silver.fact_customer_article_stats
    WHERE (customer_id, article_id) IN (
      SELECT customer_id, article_id
      FROM (
        SELECT DISTINCT customer_id, article_id
        FROM iceberg.bronze.hm_transactions
        WHERE batch_id = '{batch}'
          AND customer_id IS NOT NULL
          AND article_id IS NOT NULL
          AND substr(customer_id, 1, {prefix_len}) = '{prefix}'
      ) impacted_pairs
    )
    """
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

    sql = f"""
    INSERT INTO iceberg.silver.fact_customer_article_stats (
      customer_id,
      article_id,
      first_purchase_date,
      last_purchase_date,
      purchase_cnt,
      total_revenue,
      avg_price
    )
    WITH impacted_pairs AS (
      SELECT DISTINCT customer_id, article_id
      FROM iceberg.bronze.hm_transactions
      WHERE batch_id = '{batch}'
        AND customer_id IS NOT NULL
        AND article_id IS NOT NULL
        AND substr(customer_id, 1, {prefix_len}) = '{prefix}'
    )
    SELECT
      f.customer_id,
      f.article_id,
      min(f.sale_date) AS first_purchase_date,
      max(f.sale_date) AS last_purchase_date,
      count(*) AS purchase_cnt,
      CAST(sum(f.price) AS DECIMAL(12,4)) AS total_revenue,
      CAST(avg(f.price) AS DECIMAL(12,4)) AS avg_price
    FROM iceberg.silver.fact_sales_line f
    JOIN impacted_pairs p
      ON f.customer_id = p.customer_id
     AND f.article_id = p.article_id
    GROUP BY f.customer_id, f.article_id
    """
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
        help="How many first characters of customer_id to use for chunked stats refresh; default=2",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id
    stats_prefix_len = args.stats_prefix_len

    trino = TrinoClient()

    logger.info("Validating bronze batch: %s", batch_id)
    validate_bronze_batch(trino, batch_id)

    logger.info("Ensuring silver schema")
    ensure_silver_schema(trino)

    logger.info("Ensuring silver tables")
    ensure_silver_tables(trino)

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
    refresh_fact_sales_line_by_month(trino, batch_id)
    log_batch_row_count(trino, "iceberg.silver.fact_sales_line", batch_id)
    log_iceberg_files_summary(trino, "iceberg.silver.fact_sales_line")

    logger.info("Refreshing derived aggregate only for impacted pairs")
    refresh_fact_customer_article_stats_incremental(trino, batch_id, stats_prefix_len)
    log_iceberg_files_summary(trino, "iceberg.silver.fact_customer_article_stats")

    logger.info("Silver load finished successfully")


if __name__ == "__main__":
    main()