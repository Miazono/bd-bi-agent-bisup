DELETE FROM iceberg.silver.fact_sales_line
WHERE batch_id = '__BATCH_ID__'
  AND __SALE_MONTH_PREDICATE__
