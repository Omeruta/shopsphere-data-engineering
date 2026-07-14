"""
Loads silver (transformed) dimension data into analytics.dim_customer,
analytics.dim_product, and analytics.dim_carrier. dim_date is static and
populated once at DDL time (see sql/warehouse_ddl/01_dim_tables.sql), so
it is not loaded here.
"""
import logging

from shopsphere_pipelines.control import tracked_run
from shopsphere_pipelines.db import get_warehouse_engine
from shopsphere_pipelines.load.upsert import upsert_dataframe
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import read_parquet, read_jsonl, write_parquet, build_object_path
from shopsphere_pipelines.transform.transform_customers import transform_customers
from shopsphere_pipelines.transform.transform_products import transform_products
from shopsphere_pipelines.transform.transform_shipments import transform_carriers
from datetime import date

logger = logging.getLogger(__name__)

PIPELINE_NAME = "load_dimensions"


def load_dim_customer(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "dim_customer") as run:
        raw = read_parquet(bronze_object_path)
        clean = transform_customers(raw)
        run.records_extracted = len(clean)

        silver_path = build_object_path("silver", "postgres", "dim_customer", run_date, "dim_customer.parquet")
        write_parquet(clean, silver_path)

        engine = get_warehouse_engine()
        loaded = upsert_dataframe(engine, "analytics", "dim_customer", clean, conflict_columns=["customer_id"])
        run.records_loaded = loaded


def load_dim_product(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "dim_product") as run:
        raw = read_parquet(bronze_object_path)
        clean = transform_products(raw)
        run.records_extracted = len(clean)

        silver_path = build_object_path("silver", "postgres", "dim_product", run_date, "dim_product.parquet")
        write_parquet(clean, silver_path)

        engine = get_warehouse_engine()
        loaded = upsert_dataframe(engine, "analytics", "dim_product", clean, conflict_columns=["product_id"])
        run.records_loaded = loaded


def load_dim_carrier(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "dim_carrier") as run:
        raw = read_jsonl(bronze_object_path)
        clean = transform_carriers(raw)
        run.records_extracted = len(clean)

        silver_path = build_object_path("silver", "swiftdrop", "dim_carrier", run_date, "dim_carrier.parquet")
        write_parquet(clean, silver_path)

        engine = get_warehouse_engine()
        loaded = upsert_dataframe(engine, "analytics", "dim_carrier", clean, conflict_columns=["carrier_id"])
        run.records_loaded = loaded
