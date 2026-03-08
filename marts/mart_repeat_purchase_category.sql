/*
Grain: 1 строка = category
Sources: silver.fct_customer_article_stats, silver.dim_article
Metrics: repeat_pairs_cnt, repeat_customers_cnt, avg_purchase_cnt, repeat_revenue
*/
CREATE OR REPLACE VIEW mart.repeat_purchase_category AS
SELECT
  a.product_group_name AS category,
  count(*) AS repeat_pairs_cnt,
  count(DISTINCT s.customer_id) AS repeat_customers_cnt,
  avg(s.purchase_cnt) AS avg_purchase_cnt,
  sum(s.total_revenue) AS repeat_revenue
FROM silver.fct_customer_article_stats AS s
JOIN silver.dim_article AS a
  ON s.article_id = a.article_id
WHERE s.purchase_cnt > 1
GROUP BY a.product_group_name;
