MERGE INTO iceberg.silver.fact_customer_article_stats AS target
USING (
  SELECT
    customer_id,
    article_id,
    min(sale_date) AS first_purchase_date,
    max(sale_date) AS last_purchase_date,
    count(*) AS purchase_cnt,
    CAST(sum(price) AS DECIMAL(12,4)) AS total_revenue
  FROM iceberg.silver.fact_sales_line
  WHERE batch_id = '__BATCH_ID__'
    AND customer_id IS NOT NULL
    AND article_id IS NOT NULL
  GROUP BY customer_id, article_id
) AS src
ON target.customer_id = src.customer_id
   AND target.article_id = src.article_id
WHEN MATCHED THEN UPDATE SET
  first_purchase_date = least(target.first_purchase_date, src.first_purchase_date),
  last_purchase_date = greatest(target.last_purchase_date, src.last_purchase_date),
  purchase_cnt = target.purchase_cnt + src.purchase_cnt,
  total_revenue = CAST(target.total_revenue + src.total_revenue AS DECIMAL(12,4)),
  avg_price = CAST(
    (target.total_revenue + src.total_revenue)
    / CAST(target.purchase_cnt + src.purchase_cnt AS DECIMAL(18,4))
    AS DECIMAL(12,4)
  )
WHEN NOT MATCHED THEN INSERT (
  customer_id,
  article_id,
  first_purchase_date,
  last_purchase_date,
  purchase_cnt,
  total_revenue,
  avg_price
)
VALUES (
  src.customer_id,
  src.article_id,
  src.first_purchase_date,
  src.last_purchase_date,
  src.purchase_cnt,
  src.total_revenue,
  CAST(
    src.total_revenue / CAST(src.purchase_cnt AS DECIMAL(18,4))
    AS DECIMAL(12,4)
  )
)
