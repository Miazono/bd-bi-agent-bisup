from scripts import gen_schema


def test_render_catalog_contains_all_physical_tables():
    catalog = gen_schema.render_catalog()

    expected_tables = {
        "bronze.hm_articles",
        "bronze.hm_customers",
        "bronze.hm_transactions",
        "silver.dim_article",
        "silver.dim_customer",
        "silver.dim_date",
        "silver.fact_sales_line",
        "silver.fact_customer_article_stats",
        "mart.sales_daily_channel",
        "mart.sales_monthly_category",
        "mart.customer_segment_monthly",
        "mart.repeat_purchase_category",
        "mart.customer_rfm_monthly",
    }

    for table_name in expected_tables:
        assert f"`{table_name}`" in catalog


def test_render_catalog_excludes_temporary_raw_tables():
    catalog = gen_schema.render_catalog()

    assert "hive.raw.hm_articles_raw" not in catalog
    assert "hive.raw.hm_customers_raw" not in catalog
    assert "hive.raw.hm_transactions_raw" not in catalog
    assert "не включает временные внешние таблицы `hive.raw.*`" in catalog
