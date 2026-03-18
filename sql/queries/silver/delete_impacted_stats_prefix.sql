DELETE FROM iceberg.silver.fact_customer_article_stats
WHERE (customer_id, article_id) IN (
  SELECT customer_id, article_id
  FROM (
    SELECT DISTINCT customer_id, article_id
    FROM iceberg.bronze.hm_transactions
    WHERE batch_id = '__BATCH_ID__'
      AND customer_id IS NOT NULL
      AND article_id IS NOT NULL
      AND substr(customer_id, 1, __PREFIX_LEN__) = '__PREFIX_VALUE__'
  ) impacted_pairs
)
