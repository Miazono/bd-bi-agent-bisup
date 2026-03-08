CREATE TABLE IF NOT EXISTS silver.fct_customer_article_stats (
  customer_id VARCHAR,
  article_id BIGINT,
  first_purchase_date DATE,
  last_purchase_date DATE,
  purchase_cnt BIGINT,
  total_revenue DECIMAL(12,4),
  avg_price DECIMAL(12,4)
)
WITH (
  format = 'PARQUET',
  location = 's3a://lakehouse/silver/fct_customer_article_stats/'
);
