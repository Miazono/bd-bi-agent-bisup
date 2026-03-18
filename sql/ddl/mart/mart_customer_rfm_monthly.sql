CREATE TABLE IF NOT EXISTS mart.customer_rfm_monthly (
  customer_id VARCHAR,
  snapshot_month DATE,
  recency_days INTEGER,
  frequency_365d BIGINT,
  monetary_365d DECIMAL(12,4),
  rfm_segment VARCHAR
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/marts/customer_rfm_monthly/',
  partitioning = ARRAY['month(snapshot_month)']
);
