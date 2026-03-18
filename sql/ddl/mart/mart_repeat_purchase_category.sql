CREATE TABLE IF NOT EXISTS mart.repeat_purchase_category (
  category VARCHAR,
  repeat_pairs_cnt BIGINT,
  repeat_customers_cnt BIGINT,
  avg_purchase_cnt DECIMAL(12,4),
  repeat_revenue DECIMAL(12,4)
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/marts/repeat_purchase_category/'
);
