"""
Extracts carriers (full, every run) and shipments (paginated, incremental
via the API's updated_since filter) from the SwiftDrop Logistics API.
Raw responses are written to MinIO bronze as JSON Lines: shipments contain
a nested delivery_address object and a nested events array, so a flat
Parquet schema is not a natural fit for the untouched raw layer.
"""
import logging
import time
from datetime import date
from typing import Optional

import requests

from shopsphere_pipelines.config import get_api_config
from shopsphere_pipelines.control import get_watermark, set_watermark, tracked_run
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import build_object_path, write_jsonl

logger = logging.getLogger(__name__)

PIPELINE_NAME = "extract_swiftdrop"

REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
PAGE_LIMIT = 100


def _get_with_retry(url: str, params: Optional[dict] = None) -> dict:
    """The API has no built-in rate limiting or random failures today, but
    real HTTP calls can still fail transiently (a brief network blip, a
    container not quite ready). This small retry loop is cheap insurance,
    and matches the 'retry transient failures' idea from api_documentation.md."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning("Request to %s failed (attempt %s/%s): %s", url, attempt, MAX_RETRIES, exc)
            time.sleep(attempt)  # simple linear backoff: 1s, 2s, 3s
    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} attempts") from last_exc


def extract_carriers(run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "carriers") as run:
        cfg = get_api_config()
        data = _get_with_retry(f"{cfg.base_url}/api/v1/carriers")
        carriers = data["value"]
        run.records_extracted = len(carriers)

        if not carriers:
            logger.info("No carriers returned, skipping MinIO write")
            return

        object_path = build_object_path(
            layer="bronze",
            system="swiftdrop",
            dataset="carriers",
            run_date=run_date,
            filename="carriers.jsonl",
        )
        write_jsonl(carriers, object_path)


def extract_shipments(run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, "shipments") as run:
        cfg = get_api_config()
        watermark_value = get_watermark(PIPELINE_NAME, "shipments")

        all_shipments: list[dict] = []
        max_updated_at: Optional[str] = watermark_value
        page = 1

        while True:
            params = {"page": page, "limit": PAGE_LIMIT}
            if watermark_value:
                params["updated_since"] = watermark_value

            data = _get_with_retry(f"{cfg.base_url}/api/v1/shipments", params=params)
            shipments = data["shipments"]
            all_shipments.extend(shipments)

            for shipment in shipments:
                updated_at = shipment.get("updated_at")
                if updated_at and (max_updated_at is None or updated_at > max_updated_at):
                    max_updated_at = updated_at

            logger.info(
                "Fetched page %s/%s (%s shipments so far)",
                data["page"], data["total_pages"], len(all_shipments),
            )

            next_page = data.get("next_page")
            if not next_page:
                break
            page = next_page

        run.records_extracted = len(all_shipments)

        if not all_shipments:
            logger.info("No new/changed shipments since last watermark, skipping MinIO write")
            return

        object_path = build_object_path(
            layer="bronze",
            system="swiftdrop",
            dataset="shipments",
            run_date=run_date,
            filename="shipments.jsonl",
        )
        write_jsonl(all_shipments, object_path)

        if max_updated_at:
            set_watermark(PIPELINE_NAME, "shipments", "updated_at", max_updated_at)
            run.watermark_value = max_updated_at


def run_all(run_date: Optional[date] = None) -> None:
    run_date = run_date or date.today()
    extract_carriers(run_date)
    extract_shipments(run_date)


if __name__ == "__main__":
    setup_logging()
    run_all()
