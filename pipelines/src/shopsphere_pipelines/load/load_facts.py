"""
Loads silver (transformed) fact data into every analytics.fact_* table.
Each function: reads bronze, transforms, resolves natural keys to
surrogate keys via already-loaded dimensions, writes silver, upserts.
"""
import logging
from datetime import date

from shopsphere_pipelines.control import tracked_run
from shopsphere_pipelines.db import get_warehouse_engine
from shopsphere_pipelines.load.key_lookup import get_key_map, resolve_key
from shopsphere_pipelines.load.upsert import upsert_dataframe
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import build_object_path, read_jsonl, read_parquet, write_parquet
from shopsphere_pipelines.transform.transform_events import transform_events
from shopsphere_pipelines.transform.transform_order_items import transform_order_items
from shopsphere_pipelines.transform.transform_orders import transform_orders
from shopsphere_pipelines.transform.transform_payments import transform_payments
from shopsphere_pipelines.transform.transform_reviews import transform_reviews
from shopsphere_pipelines.transform.transform_shipments import transform_shipment_events, transform_shipments

logger = logging.getLogger(__name__)

PIPELINE_NAME = "load_facts"


def load_fact_orders(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_orders") as run:
        raw = read_parquet(bronze_object_path)
        clean = transform_orders(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        customer_keys = get_key_map(engine, "analytics", "dim_customer", "customer_id", "customer_key")
        clean = resolve_key(clean, "customer_id", "customer_key", customer_keys)

        silver_path = build_object_path("silver", "postgres", "fact_orders", run_date, "fact_orders.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(engine, "analytics", "fact_orders", clean, conflict_columns=["order_id"])
        run.records_loaded = loaded


def load_fact_order_items(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_order_items") as run:
        raw = read_parquet(bronze_object_path)
        clean = transform_order_items(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        order_keys = get_key_map(engine, "analytics", "fact_orders", "order_id", "order_key")
        product_keys = get_key_map(engine, "analytics", "dim_product", "product_id", "product_key")
        clean = resolve_key(clean, "order_id", "order_key", order_keys)
        clean = resolve_key(clean, "product_id", "product_key", product_keys)

        silver_path = build_object_path("silver", "postgres", "fact_order_items", run_date, "fact_order_items.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(engine, "analytics", "fact_order_items", clean, conflict_columns=["order_item_id"])
        run.records_loaded = loaded


def load_fact_payments(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_payments") as run:
        raw = read_parquet(bronze_object_path)
        clean = transform_payments(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        order_keys = get_key_map(engine, "analytics", "fact_orders", "order_id", "order_key")
        clean = resolve_key(clean, "order_id", "order_key", order_keys)

        silver_path = build_object_path("silver", "postgres", "fact_payments", run_date, "fact_payments.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(engine, "analytics", "fact_payments", clean, conflict_columns=["payment_id"])
        run.records_loaded = loaded


def load_fact_customer_events(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_customer_events") as run:
        raw = read_jsonl(bronze_object_path)
        clean = transform_events(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        customer_keys = get_key_map(engine, "analytics", "dim_customer", "customer_id", "customer_key")
        product_keys = get_key_map(engine, "analytics", "dim_product", "product_id", "product_key")
        clean = resolve_key(clean, "customer_id", "customer_key", customer_keys)
        clean = resolve_key(clean, "product_id", "product_key", product_keys)

        silver_path = build_object_path("silver", "mongodb", "fact_customer_events", run_date, "fact_customer_events.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(
            engine, "analytics", "fact_customer_events", clean,
            conflict_columns=["session_id", "event_index"],
        )
        run.records_loaded = loaded


def load_fact_product_reviews(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_product_reviews") as run:
        raw = read_jsonl(bronze_object_path)
        clean = transform_reviews(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        product_keys = get_key_map(engine, "analytics", "dim_product", "product_id", "product_key")
        customer_keys = get_key_map(engine, "analytics", "dim_customer", "customer_id", "customer_key")
        clean = resolve_key(clean, "product_id", "product_key", product_keys)
        clean = resolve_key(clean, "customer_id", "customer_key", customer_keys)

        silver_path = build_object_path("silver", "mongodb", "fact_product_reviews", run_date, "fact_product_reviews.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(engine, "analytics", "fact_product_reviews", clean, conflict_columns=["review_id"])
        run.records_loaded = loaded


def load_fact_shipments(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_shipments") as run:
        raw = read_jsonl(bronze_object_path)
        clean = transform_shipments(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        order_keys = get_key_map(engine, "analytics", "fact_orders", "order_id", "order_key")
        carrier_keys = get_key_map(engine, "analytics", "dim_carrier", "carrier_id", "carrier_key")
        clean = resolve_key(clean, "order_id", "order_key", order_keys)
        clean = resolve_key(clean, "carrier_id", "carrier_key", carrier_keys)

        silver_path = build_object_path("silver", "swiftdrop", "fact_shipments", run_date, "fact_shipments.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(engine, "analytics", "fact_shipments", clean, conflict_columns=["shipment_id"])
        run.records_loaded = loaded


def load_fact_shipment_events(bronze_object_path: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "fact_shipment_events") as run:
        raw = read_jsonl(bronze_object_path)
        clean = transform_shipment_events(raw)
        run.records_extracted = len(clean)

        engine = get_warehouse_engine()
        silver_path = build_object_path("silver", "swiftdrop", "fact_shipment_events", run_date, "fact_shipment_events.parquet")
        write_parquet(clean, silver_path)

        loaded = upsert_dataframe(
            engine, "analytics", "fact_shipment_events", clean,
            conflict_columns=["shipment_id", "event_index"],
        )
        run.records_loaded = loaded
