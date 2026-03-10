CREATE TABLE IF NOT EXISTS bronze.hm_customers (
  customer_id VARCHAR,
  fn INTEGER,
  active INTEGER,
  club_member_status VARCHAR,
  fashion_news_frequency VARCHAR,
  age INTEGER,
  postal_code VARCHAR,
  ingest_ts TIMESTAMP(6),
  source_file_name VARCHAR,
  batch_id VARCHAR
)
WITH (
  format = 'PARQUET',
  location = 's3://lakehouse/bronze/hm_customers/'
);
