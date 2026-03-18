/*
Grain: 1 строка = sale_date + sales_channel_id
Sources: silver.fact_sales_line
Metrics: revenue, items_sold, buyers_cnt, avg_item_price
*/
SELECT
  sale_date,
  sales_channel_id,
  sum(price) AS revenue,
  count(*) AS items_sold,
  count(DISTINCT customer_id) AS buyers_cnt,
  CAST(avg(price) AS DECIMAL(12,4)) AS avg_item_price
FROM silver.fact_sales_line
GROUP BY sale_date, sales_channel_id;
