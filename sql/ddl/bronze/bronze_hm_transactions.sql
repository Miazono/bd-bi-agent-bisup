CREATE TABLE IF NOT EXISTS bronze.hm_transactions (
  t_dat DATE,
  customer_id VARCHAR,
  article_id BIGINT,
  price DECIMAL(12,4),
  sales_channel_id INTEGER,
  ingest_ts TIMESTAMP(6),
  source_file_name VARCHAR,
  batch_id VARCHAR
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/bronze/hm_transactions/',
  partitioning = ARRAY['month(t_dat)']
);
