INSERT INTO iceberg.bronze.hm_customers
SELECT
    NULLIF(customer_id, '') AS customer_id,
    TRY_CAST(regexp_replace(NULLIF(trim(fn), ''), '\\\\.0+$', '') AS INTEGER) AS fn,
    TRY_CAST(regexp_replace(NULLIF(trim(active), ''), '\\\\.0+$', '') AS INTEGER) AS active,
    NULLIF(club_member_status, '') AS club_member_status,
    NULLIF(fashion_news_frequency, '') AS fashion_news_frequency,
    TRY_CAST(regexp_replace(NULLIF(trim(age), ''), '\\\\.0+$', '') AS INTEGER) AS age,
    NULLIF(postal_code, '') AS postal_code,
    CURRENT_TIMESTAMP AS ingest_ts,
    '__SOURCE_FILE_NAME__' AS source_file_name,
    '__BATCH_ID__' AS batch_id
FROM hive.raw.hm_customers_raw
