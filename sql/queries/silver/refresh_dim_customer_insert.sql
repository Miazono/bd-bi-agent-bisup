INSERT INTO iceberg.silver.dim_customer (
  customer_id,
  fn,
  active,
  club_member_status,
  fashion_news_frequency,
  age,
  postal_code,
  age_band,
  is_active_customer,
  is_fn_flag_present
)
WITH impacted_keys AS (
  SELECT DISTINCT customer_id
  FROM iceberg.bronze.hm_customers
  WHERE batch_id = '__BATCH_ID__'
    AND customer_id IS NOT NULL
),
ranked AS (
  SELECT
    b.customer_id,
    b.fn,
    b.active,
    b.club_member_status,
    b.fashion_news_frequency,
    b.age,
    b.postal_code,
    b.ingest_ts,
    b.batch_id,
    b.source_file_name,
    row_number() OVER (
      PARTITION BY b.customer_id
      ORDER BY b.ingest_ts DESC, b.batch_id DESC, b.source_file_name DESC
    ) AS rn
  FROM iceberg.bronze.hm_customers b
  JOIN impacted_keys k
    ON b.customer_id = k.customer_id
)
SELECT
  customer_id,
  fn,
  active,
  club_member_status,
  fashion_news_frequency,
  age,
  postal_code,
  CASE
    WHEN age IS NULL THEN 'unknown'
    WHEN age < 18 THEN 'under_18'
    WHEN age BETWEEN 18 AND 24 THEN '18_24'
    WHEN age BETWEEN 25 AND 34 THEN '25_34'
    WHEN age BETWEEN 35 AND 44 THEN '35_44'
    WHEN age BETWEEN 45 AND 54 THEN '45_54'
    WHEN age BETWEEN 55 AND 64 THEN '55_64'
    ELSE '65_plus'
  END AS age_band,
  CASE
    WHEN active = 1 THEN true
    ELSE false
  END AS is_active_customer,
  CASE
    WHEN fn IS NOT NULL THEN true
    ELSE false
  END AS is_fn_flag_present
FROM ranked
WHERE rn = 1
