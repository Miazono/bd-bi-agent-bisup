from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    lakehouse_bucket: str = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
    raw_prefix: str = os.getenv("RAW_PREFIX", "raw")
    bronze_prefix: str = os.getenv("BRONZE_PREFIX", "bronze")
    silver_prefix: str = os.getenv("SILVER_PREFIX", "silver")
    mart_prefix: str = os.getenv("MART_PREFIX", "marts")
    s3_table_scheme: str = os.getenv("S3_TABLE_SCHEME", "s3a")

    trino_catalog_hive: str = "hive"
    trino_catalog_iceberg: str = "iceberg"

    @property
    def bronze_base_uri(self) -> str:
        return f"{self.s3_table_scheme}://{self.lakehouse_bucket}/{self.bronze_prefix}"

settings = Settings()