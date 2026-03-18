/*
Grain: 1 строка = category
Sources: silver.fact_customer_article_stats, silver.dim_article
Metrics: repeat_pairs_cnt, repeat_customers_cnt, avg_purchase_cnt, repeat_revenue
*/
INSERT INTO iceberg.mart.repeat_purchase_category
SELECT
  a.product_group_name AS category,
  count(*) AS repeat_pairs_cnt,
  count(DISTINCT s.customer_id) AS repeat_customers_cnt,
  CAST(avg(s.purchase_cnt) AS DECIMAL(12,4)) AS avg_purchase_cnt,
  sum(s.total_revenue) AS repeat_revenue
FROM silver.fact_customer_article_stats AS s
JOIN silver.dim_article AS a
  ON s.article_id = a.article_id
WHERE s.purchase_cnt > 1
GROUP BY a.product_group_name;
