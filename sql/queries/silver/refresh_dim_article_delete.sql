DELETE FROM iceberg.silver.dim_article
WHERE article_id IN (
  SELECT DISTINCT article_id
  FROM iceberg.bronze.hm_articles
  WHERE batch_id = '__BATCH_ID__'
    AND article_id IS NOT NULL
)
