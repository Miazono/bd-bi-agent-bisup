from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
SUPPORTED_ENVS = {"local", "docker"}


def _load_environment() -> str:
    app_env = os.getenv("APP_ENV", "local").strip().lower()

    if app_env not in SUPPORTED_ENVS:
        raise RuntimeError(
            f"Unsupported APP_ENV={app_env!r}. "
            f"Use one of: {', '.join(sorted(SUPPORTED_ENVS))}"
        )

    env_file = BASE_DIR / f".env.{app_env}"
    load_dotenv(env_file, override=False)

    return app_env


APP_ENV = _load_environment()


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)

    if required and (value is None or value == ""):
        raise RuntimeError(f"Environment variable {name} is required")

    if value is None:
        raise RuntimeError(
            f"Environment variable {name} is not set and has no default value"
        )

    return value


@dataclass(frozen=True)
class Settings:
    app_env: str

    lakehouse_bucket: str
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    mart_prefix: str
    s3_table_scheme: str

    minio_root_user: str
    minio_root_password: str
    minio_endpoint: str
    minio_region: str

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_addressing_style: str

    hive_metastore_uri: str

    trino_host: str
    trino_port: int
    trino_catalog: str
    trino_schema: str
    trino_coordinator: str

    openai_api_key: str
    wren_ai_endpoint: str

    trino_catalog_hive: str = "hive"
    trino_catalog_iceberg: str = "iceberg"

    @property
    def raw_base_uri(self) -> str:
        return f"{self.s3_table_scheme}://{self.lakehouse_bucket}/{self.raw_prefix}"

    @property
    def bronze_base_uri(self) -> str:
        return f"{self.s3_table_scheme}://{self.lakehouse_bucket}/{self.bronze_prefix}"

    @property
    def silver_base_uri(self) -> str:
        return f"{self.s3_table_scheme}://{self.lakehouse_bucket}/{self.silver_prefix}"

    @property
    def mart_base_uri(self) -> str:
        return f"{self.s3_table_scheme}://{self.lakehouse_bucket}/{self.mart_prefix}"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_env=APP_ENV,
            lakehouse_bucket=_env("LAKEHOUSE_BUCKET", "lakehouse"),
            raw_prefix=_env("RAW_PREFIX", "raw"),
            bronze_prefix=_env("BRONZE_PREFIX", "bronze"),
            silver_prefix=_env("SILVER_PREFIX", "silver"),
            mart_prefix=_env("MART_PREFIX", "marts"),
            s3_table_scheme=_env("S3_TABLE_SCHEME", "s3a"),
            minio_root_user=_env("MINIO_ROOT_USER", "example"),
            minio_root_password=_env("MINIO_ROOT_PASSWORD", "exampleexample"),
            minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_region=_env("MINIO_REGION", "us-east-1"),
            s3_endpoint=_env("S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key=_env("S3_ACCESS_KEY", "example"),
            s3_secret_key=_env("S3_SECRET_KEY", "exampleexample"),
            s3_addressing_style=_env("S3_ADDRESSING_STYLE", "path"),
            hive_metastore_uri=_env("HIVE_METASTORE_URI", "thrift://localhost:9083"),
            trino_host=_env("TRINO_HOST", "localhost"),
            trino_port=int(_env("TRINO_PORT", "8080")),
            trino_catalog=_env("TRINO_CATALOG", "iceberg"),
            trino_schema=_env("TRINO_SCHEMA", "default"),
            trino_coordinator=_env("TRINO_COORDINATOR", "http://localhost:8080"),
            openai_api_key=_env("OPENAI_API_KEY", "sk-placeholder"),
            wren_ai_endpoint=_env("WREN_AI_ENDPOINT", "http://localhost:3000"),
        )


settings = Settings.from_env()