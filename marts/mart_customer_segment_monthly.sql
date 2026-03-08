/*
Grain: 1 строка = sale_month + customer_segment
Sources: silver.fact_sales_line, silver.dim_customer
Metrics: revenue, buyers_cnt, purchase_lines_cnt, revenue_per_buyer, avg_item_price
*/
CREATE OR REPLACE VIEW mart.customer_segment_monthly AS
SELECT
  CAST(date_trunc('month', f.sale_date) AS DATE) AS sale_month,
  COALESCE(c.club_member_status, 'unknown') AS customer_segment,
  sum(f.price) AS revenue,
  count(DISTINCT f.customer_id) AS buyers_cnt,
  count(*) AS purchase_lines_cnt,
  sum(f.price) / NULLIF(count(DISTINCT f.customer_id), 0) AS revenue_per_buyer,
  avg(f.price) AS avg_item_price
FROM silver.fact_sales_line AS f
JOIN silver.dim_customer AS c
  ON f.customer_id = c.customer_id
GROUP BY
  CAST(date_trunc('month', f.sale_date) AS DATE),
  COALESCE(c.club_member_status, 'unknown');
