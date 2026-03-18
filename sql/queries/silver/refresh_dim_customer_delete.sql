DELETE FROM iceberg.silver.dim_customer
WHERE customer_id IN (
  SELECT DISTINCT customer_id
  FROM iceberg.bronze.hm_customers
  WHERE batch_id = '__BATCH_ID__'
    AND customer_id IS NOT NULL
)
