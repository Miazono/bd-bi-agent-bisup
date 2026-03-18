CREATE TABLE IF NOT EXISTS mart.sales_monthly_category (
  sale_month DATE,
  category VARCHAR,
  revenue DECIMAL(12,4),
  items_sold BIGINT,
  buyers_cnt BIGINT,
  active_sku_cnt BIGINT
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/marts/sales_monthly_category/',
  partitioning = ARRAY['month(sale_month)']
);
