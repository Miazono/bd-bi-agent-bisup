INSERT INTO iceberg.silver.dim_article (
  article_id,
  product_code,
  prod_name,
  product_type_no,
  product_type_name,
  product_group_name,
  graphical_appearance_no,
  graphical_appearance_name,
  colour_group_code,
  colour_group_name,
  perceived_colour_value_id,
  perceived_colour_value_name,
  perceived_colour_master_id,
  perceived_colour_master_name,
  department_no,
  department_name,
  index_code,
  index_name,
  index_group_no,
  index_group_name,
  section_no,
  section_name,
  garment_group_no,
  garment_group_name,
  detail_desc,
  is_ladieswear,
  is_menswear,
  is_kids,
  color_family
)
WITH impacted_keys AS (
  SELECT DISTINCT article_id
  FROM iceberg.bronze.hm_articles
  WHERE batch_id = '__BATCH_ID__'
    AND article_id IS NOT NULL
),
ranked AS (
  SELECT
    b.article_id,
    b.product_code,
    b.prod_name,
    b.product_type_no,
    b.product_type_name,
    b.product_group_name,
    b.graphical_appearance_no,
    b.graphical_appearance_name,
    b.colour_group_code,
    b.colour_group_name,
    b.perceived_colour_value_id,
    b.perceived_colour_value_name,
    b.perceived_colour_master_id,
    b.perceived_colour_master_name,
    b.department_no,
    b.department_name,
    b.index_code,
    b.index_name,
    b.index_group_no,
    b.index_group_name,
    b.section_no,
    b.section_name,
    b.garment_group_no,
    b.garment_group_name,
    b.detail_desc,
    b.ingest_ts,
    b.batch_id,
    b.source_file_name,
    row_number() OVER (
      PARTITION BY b.article_id
      ORDER BY b.ingest_ts DESC, b.batch_id DESC, b.source_file_name DESC
    ) AS rn
  FROM iceberg.bronze.hm_articles b
  JOIN impacted_keys k
    ON b.article_id = k.article_id
)
SELECT
  article_id,
  product_code,
  prod_name,
  product_type_no,
  product_type_name,
  product_group_name,
  graphical_appearance_no,
  graphical_appearance_name,
  colour_group_code,
  colour_group_name,
  perceived_colour_value_id,
  perceived_colour_value_name,
  perceived_colour_master_id,
  perceived_colour_master_name,
  department_no,
  department_name,
  index_code,
  index_name,
  index_group_no,
  index_group_name,
  section_no,
  section_name,
  garment_group_no,
  garment_group_name,
  detail_desc,
  CASE
    WHEN regexp_like(
      lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
      '(^|[^a-z])(ladies|ladieswear)([^a-z]|$)'
    )
    THEN true
    ELSE false
  END AS is_ladieswear,
  CASE
    WHEN regexp_like(
      lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
      '(^|[^a-z])men([^a-z]|$)'
    )
    THEN true
    ELSE false
  END AS is_menswear,
  CASE
    WHEN regexp_like(
      lower(coalesce(index_name, '') || ' ' || coalesce(garment_group_name, '')),
      '(^|[^a-z])(kids|kid|baby|children)([^a-z]|$)'
    )
    THEN true
    ELSE false
  END AS is_kids,
  CAST(NULL AS VARCHAR) AS color_family
FROM ranked
WHERE rn = 1
