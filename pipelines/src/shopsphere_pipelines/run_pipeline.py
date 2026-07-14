"""
Single entrypoint: runs extraction from all three sources, then loads
dimensions before facts, in dependency order. This ordering is the one
thing this file exists to guarantee -- see the comments below for why
each step must come before the next.

Usage:
    python -m shopsphere_pipelines.run_pipeline
"""
import logging
from datetime import date

from shopsphere_pipelines.extract import extract_mongodb, extract_postgres, extract_swiftdrop
from shopsphere_pipelines.load.load_dimensions import load_dim_carrier, load_dim_customer, load_dim_product
from shopsphere_pipelines.load.load_facts import (
    load_fact_customer_events,
    load_fact_order_items,
    load_fact_orders,
    load_fact_payments,
    load_fact_product_reviews,
    load_fact_shipment_events,
    load_fact_shipments,
)
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import build_object_path, object_exists

logger = logging.getLogger(__name__)


def _bronze_path(system: str, dataset: str, run_date: date, extension: str) -> str:
    return build_object_path("bronze", system, dataset, run_date, f"{dataset}.{extension}")


def _load_if_present(load_fn, system: str, dataset: str, run_date: date, extension: str) -> None:
    """
    Calls load_fn(bronze_path, run_date) only if today's bronze object
    actually exists. An incremental extract with no new/changed rows
    never writes a bronze object for run_date, and that is expected
    behavior, not an error -- so we log and skip rather than crash.
    """
    path = _bronze_path(system, dataset, run_date, extension)
    if not object_exists(path):
        logger.info("No bronze object at %s, skipping load for %s", path, dataset)
        return
    load_fn(path, run_date)


def run(run_date: date | None = None) -> None:
    run_date = run_date or date.today()
    logger.info("=== ShopSphere pipeline run starting for %s ===", run_date)

    # --- Extraction: order does not matter between sources, they are
    # independent of each other. ---
    logger.info("--- Extracting from PostgreSQL ---")
    extract_postgres.run_all(run_date)

    logger.info("--- Extracting from MongoDB ---")
    extract_mongodb.run_all(run_date)

    logger.info("--- Extracting from SwiftDrop API ---")
    extract_swiftdrop.run_all(run_date)

    # --- Loading: dimensions MUST be loaded before any fact that
    # references them, because load_facts.py resolves natural keys to
    # surrogate keys by reading the dimension table as it currently
    # stands in the warehouse (see load/key_lookup.py). ---
    logger.info("--- Loading dimensions ---")
    _load_if_present(load_dim_customer, "postgres", "customers", run_date, "parquet")
    _load_if_present(load_dim_product, "postgres", "products", run_date, "parquet")
    _load_if_present(load_dim_carrier, "swiftdrop", "carriers", run_date, "jsonl")

    # --- Loading: fact_orders MUST be loaded before fact_order_items,
    # fact_payments, and fact_shipments, because all three resolve their
    # order_id against the fact_orders table that must already contain
    # the matching rows. ---
    logger.info("--- Loading fact_orders ---")
    _load_if_present(load_fact_orders, "postgres", "orders", run_date, "parquet")

    logger.info("--- Loading remaining facts ---")
    _load_if_present(load_fact_order_items, "postgres", "order_items", run_date, "parquet")
    _load_if_present(load_fact_payments, "postgres", "payments", run_date, "parquet")
    _load_if_present(load_fact_customer_events, "mongodb", "customer_sessions", run_date, "jsonl")
    _load_if_present(load_fact_product_reviews, "mongodb", "product_reviews", run_date, "jsonl")
    _load_if_present(load_fact_shipments, "swiftdrop", "shipments", run_date, "jsonl")
    _load_if_present(load_fact_shipment_events, "swiftdrop", "shipments", run_date, "jsonl")

    logger.info("=== ShopSphere pipeline run complete for %s ===", run_date)


if __name__ == "__main__":
    setup_logging()
    run()
