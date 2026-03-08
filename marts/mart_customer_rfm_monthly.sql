/*
Grain: 1 строка = customer_id + snapshot_month
Sources: silver.fact_sales_line, silver.dim_customer
Metrics: recency_days, frequency_365d, monetary_365d, rfm_segment
*/
CREATE OR REPLACE VIEW mart.customer_rfm_monthly AS
WITH months AS (
  SELECT DISTINCT
    CAST(date_trunc('month', sale_date) AS DATE) AS month_start
  FROM silver.fact_sales_line
),
snapshots AS (
  SELECT
    month_start,
    date_add('day', -1, date_add('month', 1, month_start)) AS snapshot_month
  FROM months
),
fact_window AS (
  SELECT
    s.snapshot_month,
    f.customer_id,
    f.sale_date,
    f.price
  FROM snapshots AS s
  JOIN silver.fact_sales_line AS f
    ON f.sale_date BETWEEN date_add('day', -364, s.snapshot_month) AND s.snapshot_month
),
agg AS (
  SELECT
    snapshot_month,
    customer_id,
    max(sale_date) AS last_purchase_date,
    count(*) AS frequency_365d,
    sum(price) AS monetary_365d
  FROM fact_window
  GROUP BY snapshot_month, customer_id
)
SELECT
  a.customer_id,
  a.snapshot_month,
  date_diff('day', a.last_purchase_date, a.snapshot_month) AS recency_days,
  a.frequency_365d,
  CAST(a.monetary_365d AS DECIMAL(12,4)) AS monetary_365d,
  CASE
    WHEN date_diff('day', a.last_purchase_date, a.snapshot_month) <= 30
      AND a.frequency_365d >= 5
      AND a.monetary_365d >= 200
    THEN 'Champions'
    WHEN date_diff('day', a.last_purchase_date, a.snapshot_month) <= 60
      AND a.frequency_365d >= 3
    THEN 'Loyal'
    WHEN date_diff('day', a.last_purchase_date, a.snapshot_month) BETWEEN 61 AND 180
    THEN 'At Risk'
    ELSE 'Lost'
  END AS rfm_segment
FROM agg AS a;
