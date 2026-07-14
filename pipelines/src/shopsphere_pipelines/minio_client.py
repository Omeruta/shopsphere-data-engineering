"""
MinIO helpers: a cached client, plus read/write functions for Parquet and
JSON Lines objects using our bronze/silver, dt=YYYY-MM-DD path convention.
Parquet is used for naturally tabular data (Postgres extracts, all
transformed/silver output). JSON Lines is used for raw MongoDB and
SwiftDrop extracts, whose nested/irregular structure (arrays, optional
nested objects) does not map cleanly onto a fixed tabular schema.
"""
import io
import json
import logging
from datetime import date
from functools import lru_cache
from typing import Any

import pandas as pd
from minio import Minio
from minio.error import S3Error

from shopsphere_pipelines.config import get_minio_config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_minio_client() -> Minio:
    cfg = get_minio_config()
    client = Minio(
        cfg.endpoint,
        access_key=cfg.access_key,
        secret_key=cfg.secret_key,
        secure=cfg.secure,
    )
    if not client.bucket_exists(cfg.bucket):
        logger.info("Bucket %s does not exist, creating it", cfg.bucket)
        client.make_bucket(cfg.bucket)
    return client


def build_object_path(layer: str, system: str, dataset: str, run_date: date, filename: str) -> str:
    """
    Builds a path like:
    bronze/postgres/customers/dt=2026-07-11/customers.parquet
    """
    dt_str = run_date.isoformat()
    return f"{layer}/{system}/{dataset}/dt={dt_str}/{filename}"


def object_exists(object_path: str) -> bool:
    """
    Checks whether an object exists without downloading it. Used by
    run_pipeline.py to skip a load step cleanly when an incremental
    extract found no new/changed rows and therefore never wrote a
    bronze object for today's run_date.
    """
    cfg = get_minio_config()
    client = get_minio_client()
    try:
        client.stat_object(cfg.bucket, object_path)
        return True
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return False
        raise


def write_parquet(df: pd.DataFrame, object_path: str) -> None:
    cfg = get_minio_config()
    client = get_minio_client()

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    client.put_object(
        bucket_name=cfg.bucket,
        object_name=object_path,
        data=buffer,
        length=size,
        content_type="application/octet-stream",
    )
    logger.info("Wrote %s rows to %s/%s (%d bytes)", len(df), cfg.bucket, object_path, size)


def read_parquet(object_path: str) -> pd.DataFrame:
    cfg = get_minio_config()
    client = get_minio_client()

    response = client.get_object(cfg.bucket, object_path)
    try:
        buffer = io.BytesIO(response.read())
    finally:
        response.close()
        response.release_conn()

    df = pd.read_parquet(buffer, engine="pyarrow")
    logger.info("Read %s rows from %s/%s", len(df), cfg.bucket, object_path)
    return df


def write_jsonl(records: list[dict[str, Any]], object_path: str) -> None:
    cfg = get_minio_config()
    client = get_minio_client()

    buffer = io.BytesIO()
    for record in records:
        line = json.dumps(record, default=str) + "\n"
        buffer.write(line.encode("utf-8"))
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    client.put_object(
        bucket_name=cfg.bucket,
        object_name=object_path,
        data=buffer,
        length=size,
        content_type="application/x-ndjson",
    )
    logger.info("Wrote %s records to %s/%s (%d bytes)", len(records), cfg.bucket, object_path, size)


def read_jsonl(object_path: str) -> list[dict[str, Any]]:
    cfg = get_minio_config()
    client = get_minio_client()

    response = client.get_object(cfg.bucket, object_path)
    try:
        raw = response.read()
    finally:
        response.close()
        response.release_conn()

    records = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
    logger.info("Read %s records from %s/%s", len(records), cfg.bucket, object_path)
    return records
