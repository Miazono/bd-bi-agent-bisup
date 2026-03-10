import logging
import os

from trino.dbapi import connect

from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def get_trino_connection():
    """Создаёт подключение к Trino на основе переменных окружения."""
    host = settings.trino_host
    port = settings.trino_port
    user = settings.minio_root_user
    catalog = settings.trino_catalog
    schema = settings.trino_schema
    http_scheme = settings.trino_http_schema

    return connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
        schema=schema,
        http_scheme=http_scheme,
    )


def execute_sql(cursor, sql, title):
    """
    Выполняет SQL и логирует этап.
    """
    logging.info("Start: %s", title)
    cursor.execute(sql)
    logging.info("Done: %s", title)


def log_row_count(cursor, table_name):
    """
    Логирует количество строк после загрузки.
    """
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    logging.info("Rows in %s: %s", table_name, count)


# Grain: 1 строка = 1 article record
SQL_DIM_ARTICLE = """
INSERT INTO silver.dim_article (
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
  source_file_name,
  batch_id,
  is_ladieswear,
  is_menswear,
  is_kids,
  color_family
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
  ingest_ts,
  source_file_name,
  batch_id,
  CASE
    WHEN lower(coalesce(index_name, '')) LIKE '%ladies%'
      OR lower(coalesce(garment_group_name, '')) LIKE '%ladies%'
    THEN true
    ELSE false
  END AS is_ladieswear,
  CASE
    WHEN lower(coalesce(index_name, '')) LIKE '%men%'
      OR lower(coalesce(garment_group_name, '')) LIKE '%men%'
    THEN true
    ELSE false
  END AS is_menswear,
  CASE
    WHEN lower(coalesce(index_name, '')) LIKE '%kids%'
      OR lower(coalesce(index_name, '')) LIKE '%baby%'
      OR lower(coalesce(garment_group_name, '')) LIKE '%kids%'
      OR lower(coalesce(garment_group_name, '')) LIKE '%baby%'
    THEN true
    ELSE false
  END AS is_kids,
  CAST(NULL AS VARCHAR) AS color_family
FROM bronze.hm_articles
"""


# Grain: 1 строка = 1 customer record
SQL_DIM_CUSTOMER = """
INSERT INTO silver.dim_customer (
  customer_id,
  fn,
  active,
  club_member_status,
  fashion_news_frequency,
  age,
  postal_code,
  ingest_ts,
  source_file_name,
  batch_id,
  age_band,
  is_active_customer,
  is_fn_flag_present
)
SELECT
  customer_id,
  fn,
  active,
  club_member_status,
  fashion_news_frequency,
  age,
  postal_code,
  ingest_ts,
  source_file_name,
  batch_id,
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
    WHEN CAST(active AS VARCHAR) = '1' THEN true
    ELSE false
  END AS is_active_customer,
  CASE
    WHEN fn IS NOT NULL THEN true
    ELSE false
  END AS is_fn_flag_present
FROM bronze.hm_customers
"""


# Grain: 1 строка = 1 календарная дата
SQL_DIM_DATE = """
INSERT INTO silver.dim_date (
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
  FROM bronze.hm_transactions
),
dates AS (
  SELECT d AS date_day
  FROM bounds
  CROSS JOIN UNNEST(sequence(min_date, max_date)) AS t(d)
)
SELECT
  date_day,
  year(date_day) AS date_year,
  month(date_day) AS date_month,
  day(date_day) AS date_day_of_month,
  day_of_week(date_day) AS date_day_of_week,
  week(date_day) AS week_of_year
FROM dates
"""


# Grain: 1 строка = 1 transaction line
SQL_FACT_SALES_LINE = """
INSERT INTO silver.fact_sales_line (
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
FROM bronze.hm_transactions
"""


# Grain: 1 строка = customer_id + article_id
SQL_FCT_CUSTOMER_ARTICLE_STATS = """
INSERT INTO silver.fct_customer_article_stats (
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
FROM silver.fact_sales_line
GROUP BY customer_id, article_id
"""


def main():
    """
    Загружает silver-таблицы из bronze через Trino SQL.
    """
    with get_trino_connection() as conn:
        cursor = conn.cursor()

        execute_sql(cursor, SQL_DIM_ARTICLE, "silver.dim_article insert")
        log_row_count(cursor, "silver.dim_article")

        execute_sql(cursor, SQL_DIM_CUSTOMER, "silver.dim_customer insert")
        log_row_count(cursor, "silver.dim_customer")

        execute_sql(cursor, SQL_DIM_DATE, "silver.dim_date insert")
        log_row_count(cursor, "silver.dim_date")

        execute_sql(cursor, SQL_FACT_SALES_LINE, "silver.fact_sales_line insert")
        log_row_count(cursor, "silver.fact_sales_line")

        execute_sql(
            cursor,
            SQL_FCT_CUSTOMER_ARTICLE_STATS,
            "silver.fct_customer_article_stats insert",
        )
        log_row_count(cursor, "silver.fct_customer_article_stats")


if __name__ == "__main__":
    main()
