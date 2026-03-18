CREATE TABLE IF NOT EXISTS mart.customer_segment_monthly (
  sale_month DATE,
  customer_segment VARCHAR,
  revenue DECIMAL(12,4),
  buyers_cnt BIGINT,
  purchase_lines_cnt BIGINT,
  revenue_per_buyer DECIMAL(12,4),
  avg_item_price DECIMAL(12,4)
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/marts/customer_segment_monthly/',
  partitioning = ARRAY['month(sale_month)']
);
