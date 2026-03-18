INSERT INTO iceberg.bronze.hm_transactions
SELECT
    CAST(NULLIF(t_dat, '') AS DATE) AS t_dat,
    NULLIF(customer_id, '') AS customer_id,
    CAST(NULLIF(article_id, '') AS BIGINT) AS article_id,
    CAST(NULLIF(price, '') AS DECIMAL(12,4)) AS price,
    CAST(NULLIF(sales_channel_id, '') AS INTEGER) AS sales_channel_id,
    CURRENT_TIMESTAMP AS ingest_ts,
    '__SOURCE_FILE_NAME__' AS source_file_name,
    '__BATCH_ID__' AS batch_id
FROM hive.raw.hm_transactions_raw
