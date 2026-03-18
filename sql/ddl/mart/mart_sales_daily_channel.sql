CREATE TABLE IF NOT EXISTS mart.sales_daily_channel (
  sale_date DATE,
  sales_channel_id INTEGER,
  revenue DECIMAL(12,4),
  items_sold BIGINT,
  buyers_cnt BIGINT,
  avg_item_price DECIMAL(12,4)
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/marts/sales_daily_channel/',
  partitioning = ARRAY['month(sale_date)']
);
