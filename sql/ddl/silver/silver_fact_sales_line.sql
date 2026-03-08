CREATE TABLE IF NOT EXISTS silver.fact_sales_line (
  sale_date DATE,
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
  location = 's3a://lakehouse/silver/fact_sales_line/',
  partitioning = ARRAY['month(sale_date)']
);
