"""
Flattens raw customer_sessions documents (list of dicts from MongoDB
JSONL) into one row per event, for analytics.fact_customer_events.

Handles three planted data quality conditions in the seed data:
1. customer_id can be null (anonymous session) -> passed through as null,
   resolved to a null customer_key at load time.
2. device can be null (missing device info) -> device_type/device_os
   become null instead of raising a KeyError on a missing nested object.
3. event_time / started_at can be malformed strings -> parsed via
   parse_flexible_datetime; rows where event_time still fails to parse
   are rejected, since event_time is NOT NULL in the target schema.

Two events within the same session can share an identical event_time in
the source data. event_index (the event's position within its session's
events array) is used as the dedup key instead of event_time, since
event_index is guaranteed unique per session and event_time is not.
"""
import logging
from typing import Any

import pandas as pd

from shopsphere_pipelines.transform.timestamps import parse_flexible_datetime

logger = logging.getLogger(__name__)


def _flatten_sessions(sessions: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for session in sessions:
        device = session.get("device") or {}
        location = session.get("location") or {}

        for event_index, event in enumerate(session.get("events", [])):
            rows.append({
                "session_id": session["session_id"],
                "event_index": event_index,
                "customer_id": session.get("customer_id"),
                "event_type": event.get("event_type"),
                "event_time": event.get("event_time"),
                "product_id": event.get("product_id"),
                "search_term": event.get("search_term"),
                "quantity": event.get("quantity"),
                "page_url": event.get("page_url"),
                "browser": session.get("browser"),
                "device_type": device.get("type"),
                "device_os": device.get("os"),
                "location_city": location.get("city"),
                "location_state": location.get("state"),
                "location_country": location.get("country"),
                "session_started_at": session.get("started_at"),
                "session_ended_at": session.get("ended_at"),
            })
    return pd.DataFrame(rows)


def transform_events(sessions: list[dict[str, Any]]) -> pd.DataFrame:
    df = _flatten_sessions(sessions)

    if df.empty:
        return df

    df["event_time"] = parse_flexible_datetime(df["event_time"], "event_time")
    df["session_started_at"] = parse_flexible_datetime(df["session_started_at"], "session_started_at")
    df["session_ended_at"] = parse_flexible_datetime(df["session_ended_at"], "session_ended_at")

    before = len(df)
    df = df[df["event_time"].notna()]
    rejected = before - len(df)
    if rejected:
        logger.warning("Rejected %s event(s) with unparseable event_time", rejected)

    # Defensive dedup on the real natural key. Guards against a source
    # re-extract accidentally duplicating a session's events, without
    # relying on event_time (which is not reliably unique, as noted above).
    before = len(df)
    df = df.drop_duplicates(subset=["session_id", "event_index"], keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate (session_id, event_index) rows", duplicates_dropped)

    return df
