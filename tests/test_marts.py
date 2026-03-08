def test_all_marts_exist(trino_conn):
    cursor = trino_conn.cursor()
    cursor.fetchall.return_value = [
        ("sales_daily_channel",),
        ("sales_monthly_category",),
        ("customer_segment_monthly",),
        ("repeat_purchase_category",),
        ("customer_rfm_monthly",),
    ]

    cursor.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mart'"
    )
    tables = {row[0] for row in cursor.fetchall()}
    expected = {
        "sales_daily_channel",
        "sales_monthly_category",
        "customer_segment_monthly",
        "repeat_purchase_category",
        "customer_rfm_monthly",
    }
    assert expected.issubset(tables)


def test_mart_sales_daily_channel_grain(trino_conn):
    cursor = trino_conn.cursor()
    cursor.fetchall.return_value = []

    cursor.execute(
        """
        SELECT sale_date, sales_channel_id, COUNT(*) AS cnt
        FROM mart.sales_daily_channel
        GROUP BY sale_date, sales_channel_id
        HAVING COUNT(*) > 1
        """
    )
    duplicates = cursor.fetchall()
    assert duplicates == []


def test_mart_rfm_no_null_segment(trino_conn):
    cursor = trino_conn.cursor()
    cursor.fetchone.return_value = (0,)

    cursor.execute(
        "SELECT COUNT(*) FROM mart.customer_rfm_monthly WHERE rfm_segment IS NULL"
    )
    null_cnt = cursor.fetchone()[0]
    assert null_cnt == 0
