from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Dict, List

from ingestion.utils.s3_client import S3Client
from ingestion.utils.sql_assets import SqlAssets
from ingestion.utils.trino_client import TrinoClient
from config.settings import settings

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


LAKEHOUSE_BUCKET = settings.lakehouse_bucket
RAW_PREFIX = settings.raw_prefix
BRONZE_PREFIX = settings.bronze_prefix
SQL_ASSETS = SqlAssets()


RAW_LAYOUT = {
    "articles": "hm/articles/load_date={load_date}/articles.csv",
    "customers": "hm/customers/load_date={load_date}/customers.csv",
    "transactions_train": "hm/transactions_train/load_date={load_date}/transactions_train.csv",
}

RAW_TABLE_COLUMNS = {
    "hm_articles_raw": [
        "article_id",
        "product_code",
        "prod_name",
        "product_type_no",
        "product_type_name",
        "product_group_name",
        "graphical_appearance_no",
        "graphical_appearance_name",
        "colour_group_code",
        "colour_group_name",
        "perceived_colour_value_id",
        "perceived_colour_value_name",
        "perceived_colour_master_id",
        "perceived_colour_master_name",
        "department_no",
        "department_name",
        "index_code",
        "index_name",
        "index_group_no",
        "index_group_name",
        "section_no",
        "section_name",
        "garment_group_no",
        "garment_group_name",
        "detail_desc",
    ],
    "hm_customers_raw": [
        "customer_id",
        "fn",
        "active",
        "club_member_status",
        "fashion_news_frequency",
        "age",
        "postal_code",
    ],
    "hm_transactions_raw": [
        "t_dat",
        "customer_id",
        "article_id",
        "price",
        "sales_channel_id",
    ],
}


def q(value: str) -> str:
    return value.replace("'", "''")

def bronze_ddl_paths() -> List[Path]:
    base = SQL_ASSETS.path("sql", "ddl", "bronze")
    return [
        base / "bronze_hm_articles.sql",
        base / "bronze_hm_customers.sql",
        base / "bronze_hm_transactions.sql",
    ]


def bronze_query_paths() -> Dict[str, Path]:
    base = SQL_ASSETS.path("sql", "queries", "bronze")
    return {
        "hm_articles": base / "load_hm_articles.sql",
        "hm_customers": base / "load_hm_customers.sql",
        "hm_transactions": base / "load_hm_transactions.sql",
    }


def validate_raw_files(s3: S3Client, bucket: str, raw_prefix: str, load_date: str) -> Dict[str, str]:
    resolved = {}

    for name, template in RAW_LAYOUT.items():
        key = f"{raw_prefix.rstrip('/')}/{template.format(load_date=load_date)}"
        if not s3.object_exists(bucket, key):
            raise FileNotFoundError(
                f"Raw file not found in MinIO: s3://{bucket}/{key}\n"
                f"Expected layout:\n"
                f"  raw/hm/articles/load_date=YYYY-MM-DD/articles.csv\n"
                f"  raw/hm/customers/load_date=YYYY-MM-DD/customers.csv\n"
                f"  raw/hm/transactions_train/load_date=YYYY-MM-DD/transactions_train.csv"
            )
        resolved[name] = key

    return resolved


def create_schemas(trino: TrinoClient, bucket: str) -> None:
    scheme = settings.s3_table_scheme
    trino.execute("CREATE SCHEMA IF NOT EXISTS hive.raw")
    trino.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS iceberg.bronze
        WITH (location = '{scheme}://{bucket}/{BRONZE_PREFIX.rstrip("/")}/')
        """
    )


def create_raw_external_table(
    trino: TrinoClient,
    table_name: str,
    columns: List[str],
    external_location: str,
) -> None:
    cols_sql = ",\n    ".join(f"{col} VARCHAR" for col in columns)

    trino.execute(f"DROP TABLE IF EXISTS hive.raw.{table_name}")

    sql = f"""
    CREATE TABLE hive.raw.{table_name} (
        {cols_sql}
    )
    WITH (
        external_location = '{external_location}',
        format = 'CSV',
        skip_header_line_count = 1
    )
    """
    trino.execute(sql)


def create_all_raw_tables(
    trino: TrinoClient,
    s3: S3Client,
    bucket: str,
    raw_keys: Dict[str, str],
) -> None:
    create_raw_external_table(
        trino=trino,
        table_name="hm_articles_raw",
        columns=RAW_TABLE_COLUMNS["hm_articles_raw"],
        external_location=s3.build_dir_uri(bucket, str(Path(raw_keys["articles"]).parent)),
    )

    create_raw_external_table(
        trino=trino,
        table_name="hm_customers_raw",
        columns=RAW_TABLE_COLUMNS["hm_customers_raw"],
        external_location=s3.build_dir_uri(bucket, str(Path(raw_keys["customers"]).parent)),
    )

    create_raw_external_table(
        trino=trino,
        table_name="hm_transactions_raw",
        columns=RAW_TABLE_COLUMNS["hm_transactions_raw"],
        external_location=s3.build_dir_uri(bucket, str(Path(raw_keys["transactions_train"]).parent)),
    )


def create_bronze_tables(trino: TrinoClient) -> None:
    for ddl_path in bronze_ddl_paths():
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_path}")
        logger.info("Applying DDL: %s", ddl_path)
        trino.execute_file(ddl_path)


def delete_batch(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)
    trino.execute(f"DELETE FROM iceberg.bronze.hm_articles WHERE batch_id = '{batch}'")
    trino.execute(f"DELETE FROM iceberg.bronze.hm_customers WHERE batch_id = '{batch}'")
    trino.execute(f"DELETE FROM iceberg.bronze.hm_transactions WHERE batch_id = '{batch}'")


def load_articles_to_bronze(trino: TrinoClient, batch_id: str, source_file_name: str) -> None:
    batch = q(batch_id)
    src = q(source_file_name)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "bronze",
        "load_hm_articles.sql",
        replacements={
            "SOURCE_FILE_NAME": src,
            "BATCH_ID": batch,
        },
    )
    trino.execute(sql)


def load_customers_to_bronze(trino: TrinoClient, batch_id: str, source_file_name: str) -> None:
    batch = q(batch_id)
    src = q(source_file_name)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "bronze",
        "load_hm_customers.sql",
        replacements={
            "SOURCE_FILE_NAME": src,
            "BATCH_ID": batch,
        },
    )
    trino.execute(sql)


def load_transactions_to_bronze(trino: TrinoClient, batch_id: str, source_file_name: str) -> None:
    batch = q(batch_id)
    src = q(source_file_name)

    sql = SQL_ASSETS.render(
        "sql",
        "queries",
        "bronze",
        "load_hm_transactions.sql",
        replacements={
            "SOURCE_FILE_NAME": src,
            "BATCH_ID": batch,
        },
    )
    trino.execute(sql)


def log_counts(trino: TrinoClient, batch_id: str) -> None:
    batch = q(batch_id)

    for table in [
        "iceberg.bronze.hm_articles",
        "iceberg.bronze.hm_customers",
        "iceberg.bronze.hm_transactions",
    ]:
        row = trino.fetchone(f"SELECT COUNT(*) FROM {table} WHERE batch_id = '{batch}'")
        count = row[0] if row else 0
        logger.info("%s | batch=%s | rows=%s", table, batch_id, count)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load H&M raw CSV from MinIO into bronze Iceberg tables")
    parser.add_argument("--load-date", required=True, help="Date partition in raw, format YYYY-MM-DD")
    parser.add_argument("--batch-id", required=True, help="Idempotent batch identifier")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="Root raw prefix in bucket, default=env RAW_PREFIX")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    s3 = S3Client()
    trino = TrinoClient()

    logger.info("Ensuring bucket exists: %s", LAKEHOUSE_BUCKET)
    s3.ensure_bucket(LAKEHOUSE_BUCKET)

    logger.info("Validating raw files for load_date=%s", args.load_date)
    raw_keys = validate_raw_files(
        s3=s3,
        bucket=LAKEHOUSE_BUCKET,
        raw_prefix=args.raw_prefix,
        load_date=args.load_date,
    )

    logger.info("Creating schemas")
    create_schemas(trino, LAKEHOUSE_BUCKET)

    logger.info("Creating raw external tables in hive.raw")
    create_all_raw_tables(trino, s3, LAKEHOUSE_BUCKET, raw_keys)

    logger.info("Creating bronze Iceberg tables")
    create_bronze_tables(trino)

    logger.info("Deleting previous rows for batch_id=%s", args.batch_id)
    delete_batch(trino, args.batch_id)

    logger.info("Loading articles -> bronze")
    load_articles_to_bronze(trino, args.batch_id, "articles.csv")

    logger.info("Loading customers -> bronze")
    load_customers_to_bronze(trino, args.batch_id, "customers.csv")

    logger.info("Loading transactions -> bronze")
    load_transactions_to_bronze(trino, args.batch_id, "transactions_train.csv")

    logger.info("Logging row counts")
    log_counts(trino, args.batch_id)

    logger.info("Bronze load finished successfully")


if __name__ == "__main__":
    main()
