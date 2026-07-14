"""
Extracts customers, products, orders, and payments incrementally using
their updated_at column as a watermark. Extracts order_items in full every
run, since that table has no independent updated_at column upstream.
Writes raw rows to MinIO bronze as Parquet (flat, tabular source data).
"""
import logging
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import text

from shopsphere_pipelines.control import get_watermark, set_watermark, tracked_run
from shopsphere_pipelines.db import get_source_engine
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import build_object_path, write_parquet

logger = logging.getLogger(__name__)

PIPELINE_NAME = "extract_postgres"

# table_name -> watermark column, for tables we can extract incrementally
INCREMENTAL_TABLES = {
    "customers": "updated_at",
    "products": "updated_at",
    "orders": "updated_at",
    "payments": "updated_at",
}

# extracted in full every run: no independent updated_at column upstream
FULL_TABLES = ["order_items"]


def _extract_incremental(table_name: str, watermark_column: str) -> pd.DataFrame:
    engine = get_source_engine()
    watermark_value = get_watermark(PIPELINE_NAME, table_name)

    if watermark_value:
        # ::timestamptz cast is required: the watermark is stored as TEXT
        # in control.pipeline_watermarks, and Postgres will not implicitly
        # compare a plain text parameter against a timestamptz column.
        query = text(
            f"SELECT * FROM {table_name} "
            f"WHERE {watermark_column} > :watermark::timestamptz "
            f"ORDER BY {watermark_column}"
        )
        params = {"watermark": watermark_value}
        logger.info("Extracting %s incrementally since %s", table_name, watermark_value)
    else:
        query = text(f"SELECT * FROM {table_name} ORDER BY {watermark_column}")
        params = {}
        logger.info("Extracting %s in full (no prior watermark)", table_name)

    return pd.read_sql(query, engine, params=params)


def _extract_full(table_name: str) -> pd.DataFrame:
    engine = get_source_engine()
    logger.info("Extracting %s in full", table_name)
    return pd.read_sql(text(f"SELECT * FROM {table_name}"), engine)


def extract_table(table_name: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, table_name) as run:
        if table_name in INCREMENTAL_TABLES:
            df = _extract_incremental(table_name, INCREMENTAL_TABLES[table_name])
        else:
            df = _extract_full(table_name)

        run.records_extracted = len(df)

        if df.empty:
            logger.info("No new/changed rows for %s, skipping MinIO write", table_name)
            return

        object_path = build_object_path(
            layer="bronze",
            system="postgres",
            dataset=table_name,
            run_date=run_date,
            filename=f"{table_name}.parquet",
        )
        write_parquet(df, object_path)

        if table_name in INCREMENTAL_TABLES:
            watermark_column = INCREMENTAL_TABLES[table_name]
            new_watermark = pd.Timestamp(df[watermark_column].max()).isoformat()
            set_watermark(PIPELINE_NAME, table_name, watermark_column, new_watermark)
            run.watermark_value = new_watermark


def run_all(run_date: Optional[date] = None) -> None:
    run_date = run_date or date.today()
    for table_name in list(INCREMENTAL_TABLES.keys()) + FULL_TABLES:
        extract_table(table_name, run_date)


if __name__ == "__main__":
    setup_logging()
    run_all()
