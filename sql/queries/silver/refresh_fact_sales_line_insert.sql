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
WHERE batch_id = '__BATCH_ID__'
  AND __BRONZE_MONTH_PREDICATE__
