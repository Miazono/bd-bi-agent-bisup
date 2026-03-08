from unittest.mock import call

from ingestion import load_raw


def test_raw_upload_creates_objects(tmp_path, minio_client, monkeypatch):
    load_date = "2020-01-15"
    for filename in load_raw.SOURCE_FILES:
        (tmp_path / filename).write_text("data", encoding="utf-8")

    monkeypatch.setattr(load_raw, "get_object_md5", lambda *args, **kwargs: None)
    monkeypatch.setattr(load_raw, "compute_md5", lambda *args, **kwargs: "md5")

    for filename in load_raw.SOURCE_FILES:
        load_raw.upload_file(
            client=minio_client,
            bucket="lakehouse",
            raw_prefix="raw",
            load_date=load_date,
            source_dir=str(tmp_path),
            filename=filename,
        )

    object_names = [call_item.args[1] for call_item in minio_client.fput_object.call_args_list]
    expected = {
        f"raw/hm/load_date={load_date}/{filename}"
        for filename in load_raw.SOURCE_FILES
    }
    assert set(object_names) == expected


def test_bronze_row_counts(trino_conn):
    cursor = trino_conn.cursor()
    cursor.fetchone.return_value = (1,)

    tables = [
        "bronze.hm_articles",
        "bronze.hm_customers",
        "bronze.hm_transactions",
    ]

    for table_name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count > 0

    executed_sql = [call_item.args[0] for call_item in cursor.execute.call_args_list]
    for table_name in tables:
        assert f"SELECT COUNT(*) FROM {table_name}" in executed_sql
