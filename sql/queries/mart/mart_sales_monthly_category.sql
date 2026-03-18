/*
Grain: 1 строка = sale_month + category
Sources: silver.fact_sales_line, silver.dim_article
Metrics: revenue, items_sold, buyers_cnt, active_sku_cnt
*/
SELECT
  CAST(date_trunc('month', f.sale_date) AS DATE) AS sale_month,
  a.product_group_name AS category,
  sum(f.price) AS revenue,
  count(*) AS items_sold,
  count(DISTINCT f.customer_id) AS buyers_cnt,
  count(DISTINCT f.article_id) AS active_sku_cnt
FROM silver.fact_sales_line AS f
JOIN silver.dim_article AS a
  ON f.article_id = a.article_id
GROUP BY CAST(date_trunc('month', f.sale_date) AS DATE), a.product_group_name;
