INSERT INTO iceberg.silver.fact_customer_article_stats (
  customer_id,
  article_id,
  first_purchase_date,
  last_purchase_date,
  purchase_cnt,
  total_revenue,
  avg_price
)
WITH impacted_pairs AS (
  SELECT DISTINCT customer_id, article_id
  FROM iceberg.bronze.hm_transactions
  WHERE batch_id = '__BATCH_ID__'
    AND customer_id IS NOT NULL
    AND article_id IS NOT NULL
    AND substr(customer_id, 1, __PREFIX_LEN__) = '__PREFIX_VALUE__'
)
SELECT
  f.customer_id,
  f.article_id,
  min(f.sale_date) AS first_purchase_date,
  max(f.sale_date) AS last_purchase_date,
  count(*) AS purchase_cnt,
  CAST(sum(f.price) AS DECIMAL(12,4)) AS total_revenue,
  CAST(avg(f.price) AS DECIMAL(12,4)) AS avg_price
FROM iceberg.silver.fact_sales_line f
JOIN impacted_pairs p
  ON f.customer_id = p.customer_id
 AND f.article_id = p.article_id
GROUP BY f.customer_id, f.article_id
