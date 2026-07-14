"""
Transforms raw SwiftDrop data into the clean shapes loaded into
analytics.dim_carrier, analytics.fact_shipments, and
analytics.fact_shipment_events.

- transform_carriers: simple passthrough/validation, source has no known
  data quality issues.
- transform_shipments: flattens the nested delivery_address object into
  flat columns; validates shipment_status.
- transform_shipment_events: flattens the nested events array into one
  row per event, same event_index dedup pattern as transform_events
  (event_time is not guaranteed unique, event_index is).
"""
import logging
from typing import Any

import pandas as pd

from shopsphere_pipelines.transform.timestamps import parse_flexible_datetime

logger = logging.getLogger(__name__)

VALID_SHIPMENT_STATUSES = {
    "pending", "shipped", "in_transit", "out_for_delivery",
    "delivered", "returned", "cancelled",
}


def transform_carriers(carriers: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(carriers)
    if df.empty:
        return df

    before = len(df)
    df = df.drop_duplicates(subset="carrier_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate carrier_id rows", duplicates_dropped)

    return df[["carrier_id", "carrier_name", "service_level", "support_phone"]]


def _flatten_delivery_address(shipments: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for shipment in shipments:
        address = shipment.get("delivery_address") or {}
        rows.append({
            "shipment_id": shipment.get("shipment_id"),
            "order_id": shipment.get("order_id"),
            "carrier_id": shipment.get("carrier_id"),
            "tracking_number": shipment.get("tracking_number"),
            "shipment_status": shipment.get("shipment_status"),
            "shipped_at": shipment.get("shipped_at"),
            "estimated_delivery_at": shipment.get("estimated_delivery_at"),
            "delivered_at": shipment.get("delivered_at"),
            "updated_at": shipment.get("updated_at"),
            "delivery_street": address.get("street"),
            "delivery_city": address.get("city"),
            "delivery_state": address.get("state"),
            "delivery_country": address.get("country"),
            "delivery_postal_code": address.get("postal_code"),
        })
    return pd.DataFrame(rows)


def transform_shipments(shipments: list[dict[str, Any]]) -> pd.DataFrame:
    df = _flatten_delivery_address(shipments)
    if df.empty:
        return df

    before = len(df)
    df = df.drop_duplicates(subset="shipment_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate shipment_id rows", duplicates_dropped)

    invalid_status_mask = ~df["shipment_status"].isin(VALID_SHIPMENT_STATUSES)
    invalid_count = int(invalid_status_mask.sum())
    if invalid_count:
        logger.warning(
            "Rejecting %s shipment(s) with unrecognized shipment_status values: %s",
            invalid_count,
            df.loc[invalid_status_mask, "shipment_status"].unique().tolist(),
        )
        df = df.loc[~invalid_status_mask]

    for col in ("shipped_at", "estimated_delivery_at", "delivered_at", "updated_at"):
        df[col] = parse_flexible_datetime(df[col], col)

    before = len(df)
    df = df[df["updated_at"].notna()]
    rejected = before - len(df)
    if rejected:
        logger.warning("Rejected %s shipment(s) with unparseable updated_at", rejected)

    return df.rename(columns={"updated_at": "source_updated_at"})


def transform_shipment_events(shipments: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for shipment in shipments:
        for event_index, event in enumerate(shipment.get("events", [])):
            rows.append({
                "shipment_id": shipment.get("shipment_id"),
                "event_index": event_index,
                "event_type": event.get("event_type"),
                "event_time": event.get("event_time"),
                "location": event.get("location"),
                "notes": event.get("notes"),
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["event_time"] = parse_flexible_datetime(df["event_time"], "event_time")

    before = len(df)
    df = df[df["event_time"].notna()]
    rejected = before - len(df)
    if rejected:
        logger.warning("Rejected %s shipment event(s) with unparseable event_time", rejected)

    before = len(df)
    df = df.drop_duplicates(subset=["shipment_id", "event_index"], keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate (shipment_id, event_index) rows", duplicates_dropped)

    return df
