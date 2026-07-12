"""
Central configuration. Every pipeline module imports settings from here
instead of reading environment variables directly.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load pipelines/.env into the process environment. override=False means
# if a variable is already set (e.g. by Docker at runtime), that wins
# instead of being clobbered by the .env file.
load_dotenv(override=False)


def _require(key: str) -> str:
    """Fetch an environment variable or fail loudly with a clear message,
    instead of silently continuing with None and failing later somewhere
    confusing like inside a SQL connection string."""
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


@dataclass(frozen=True)
class SourcePostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


@dataclass(frozen=True)
class WarehousePostgresConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.db}"
        )


@dataclass(frozen=True)
class MongoConfig:
    uri: str
    database: str


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    bucket: str


@dataclass(frozen=True)
class ApiConfig:
    base_url: str


def get_source_postgres_config() -> SourcePostgresConfig:
    return SourcePostgresConfig(
        host=_require("SOURCE_POSTGRES_HOST"),
        port=int(_require("SOURCE_POSTGRES_PORT")),
        db=_require("SOURCE_POSTGRES_DB"),
        user=_require("SOURCE_POSTGRES_USER"),
        password=_require("SOURCE_POSTGRES_PASSWORD"),
    )


def get_warehouse_postgres_config() -> WarehousePostgresConfig:
    return WarehousePostgresConfig(
        host=_require("WAREHOUSE_POSTGRES_HOST"),
        port=int(_require("WAREHOUSE_POSTGRES_PORT")),
        db=_require("WAREHOUSE_POSTGRES_DB"),
        user=_require("WAREHOUSE_POSTGRES_USER"),
        password=_require("WAREHOUSE_POSTGRES_PASSWORD"),
    )


def get_mongo_config() -> MongoConfig:
    return MongoConfig(
        uri=_require("MONGODB_URI"),
        database=_require("MONGODB_DATABASE"),
    )


def get_minio_config() -> MinioConfig:
    return MinioConfig(
        endpoint=_require("MINIO_ENDPOINT"),
        access_key=_require("MINIO_ACCESS_KEY"),
        secret_key=_require("MINIO_SECRET_KEY"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        bucket=_require("MINIO_BUCKET"),
    )


def get_api_config() -> ApiConfig:
    return ApiConfig(base_url=_require("MOCK_API_BASE_URL"))
