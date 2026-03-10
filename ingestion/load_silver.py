from __future__ import annotations

import argparse
import logging
import os
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
        logger.info("Bronze batch source %s | batch=%s | rows=%s", table, batch_id, count)

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


def rebuild_dim_article(trino: TrinoClient) -> None:
    execute_step(
        trino,
        "DELETE FROM iceberg.silver.dim_article",
        "DELETE FROM silver.dim_article",
    )

    sql = """
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
    WITH ranked AS (
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
        ingest_ts,
        batch_id,
        source_file_name,
        row_number() OVER (
          PARTITION BY article_id
          ORDER BY ingest_ts DESC, batch_id DESC, source_file_name DESC
        ) AS rn
      FROM iceberg.bronze.hm_articles
      WHERE article_id IS NOT NULL
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
    execute_step(trino, sql, "Rebuild silver.dim_article")


def rebuild_dim_customer(trino: TrinoClient) -> None:
    execute_step(
        trino,
        "DELETE FROM iceberg.silver.dim_customer",
        "DELETE FROM silver.dim_customer",
    )

    sql = """
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
    WITH ranked AS (
      SELECT
        customer_id,
        fn,
        active,
        club_member_status,
        fashion_news_frequency,
        age,
        postal_code,
        ingest_ts,
        batch_id,
        source_file_name,
        row_number() OVER (
          PARTITION BY customer_id
          ORDER BY ingest_ts DESC, batch_id DESC, source_file_name DESC
        ) AS rn
      FROM iceberg.bronze.hm_customers
      WHERE customer_id IS NOT NULL
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
    execute_step(trino, sql, "Rebuild silver.dim_customer")


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
    execute_step(trino, sql, f"Upsert silver.dim_date for batch={batch_id}")


def refresh_fact_sales_line(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)

    delete_sql = f"""
    DELETE FROM iceberg.silver.fact_sales_line
    WHERE batch_id = '{batch}'
    """
    execute_step(trino, delete_sql, f"Delete silver.fact_sales_line batch={batch_id}")

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
    """
    execute_step(trino, insert_sql, f"Insert silver.fact_sales_line batch={batch_id}")


def rebuild_fact_customer_article_stats(trino: TrinoClient) -> None:
    execute_step(
        trino,
        "DELETE FROM iceberg.silver.fact_customer_article_stats",
        "DELETE FROM silver.fact_customer_article_stats",
    )

    sql = """
    INSERT INTO iceberg.silver.fact_customer_article_stats (
      customer_id,
      article_id,
      first_purchase_date,
      last_purchase_date,
      purchase_cnt,
      total_revenue,
      avg_price
    )
    SELECT
      customer_id,
      article_id,
      min(sale_date) AS first_purchase_date,
      max(sale_date) AS last_purchase_date,
      count(*) AS purchase_cnt,
      CAST(sum(price) AS DECIMAL(12,4)) AS total_revenue,
      CAST(avg(price) AS DECIMAL(12,4)) AS avg_price
    FROM iceberg.silver.fact_sales_line
    GROUP BY customer_id, article_id
    """
    execute_step(trino, sql, "Rebuild silver.fact_customer_article_stats")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load silver layer from bronze via Trino"
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Bronze batch identifier to load into silver",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id

    trino = TrinoClient()

    logger.info("Validating bronze batch: %s", batch_id)
    validate_bronze_batch(trino, batch_id)

    logger.info("Ensuring silver schema")
    ensure_silver_schema(trino)

    logger.info("Ensuring silver tables")
    ensure_silver_tables(trino)

    logger.info("Rebuilding dimensions")
    rebuild_dim_article(trino)
    log_row_count(trino, "iceberg.silver.dim_article")

    rebuild_dim_customer(trino)
    log_row_count(trino, "iceberg.silver.dim_customer")

    logger.info("Upserting dim_date from batch")
    upsert_dim_date(trino, batch_id)
    log_row_count(trino, "iceberg.silver.dim_date")

    logger.info("Refreshing fact_sales_line for batch=%s", batch_id)
    refresh_fact_sales_line(trino, batch_id)
    log_batch_row_count(trino, "iceberg.silver.fact_sales_line", batch_id)
    log_row_count(trino, "iceberg.silver.fact_sales_line")

    logger.info("Rebuilding derived aggregate fact_customer_article_stats")
    rebuild_fact_customer_article_stats(trino)
    log_row_count(trino, "iceberg.silver.fact_customer_article_stats")

    logger.info("Silver load finished successfully")


if __name__ == "__main__":
    main()