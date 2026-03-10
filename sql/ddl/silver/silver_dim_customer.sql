CREATE TABLE IF NOT EXISTS silver.dim_customer (
  customer_id VARCHAR,
  fn INTEGER,
  active INTEGER,
  club_member_status VARCHAR,
  fashion_news_frequency VARCHAR,
  age INTEGER,
  postal_code VARCHAR,
  age_band VARCHAR,
  is_active_customer BOOLEAN,
  is_fn_flag_present BOOLEAN
)
WITH (
  format = 'PARQUET',
  location = 's3://lakehouse/silver/dim_customer/'
);
