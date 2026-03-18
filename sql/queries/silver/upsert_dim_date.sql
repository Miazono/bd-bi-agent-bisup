INSERT INTO iceberg.silver.dim_date (
  date_day,
  date_year,
  date_month,
  date_day_of_month,
  date_day_of_week,
  week_of_year
)
WITH bounds AS (
  SELECT
    min(t_dat) AS min_date,
    max(t_dat) AS max_date
  FROM iceberg.bronze.hm_transactions
  WHERE batch_id = '__BATCH_ID__'
),
dates AS (
  SELECT d AS date_day
  FROM bounds
  CROSS JOIN UNNEST(sequence(min_date, max_date)) AS t(d)
)
SELECT
  d.date_day,
  year(d.date_day) AS date_year,
  month(d.date_day) AS date_month,
  day(d.date_day) AS date_day_of_month,
  day_of_week(d.date_day) AS date_day_of_week,
  week(d.date_day) AS week_of_year
FROM dates d
WHERE NOT EXISTS (
  SELECT 1
  FROM iceberg.silver.dim_date x
  WHERE x.date_day = d.date_day
)
