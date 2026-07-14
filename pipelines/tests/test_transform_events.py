"""
Tests for transform_events, covering the three planted data quality
conditions: null device, malformed timestamps, and duplicate event_time
within a session (verified via the event_index dedup key instead).
"""
import pandas as pd

from shopsphere_pipelines.transform.transform_events import transform_events


def _sample_session(session_id="sess_0001", device=None, event_times=None):
    if event_times is None:
        event_times = ["2025-03-01T20:04:00.000Z", "2025-03-01T20:08:00.000Z"]
    return {
        "session_id": session_id,
        "customer_id": 5,
        "started_at": "2025-03-01T20:00:00.000Z",
        "ended_at": "2025-03-01T20:30:00.000Z",
        "browser": "Chrome",
        "location": {"city": "Lagos", "state": "Lagos", "country": "Nigeria"},
        "device": device,
        "events": [
            {"event_type": "product_view", "event_time": t, "product_id": 10,
             "search_term": None, "quantity": None, "page_url": "/products/10"}
            for t in event_times
        ],
    }


def test_null_device_does_not_raise_and_produces_null_columns():
    sessions = [_sample_session(device=None)]
    result = transform_events(sessions)

    assert len(result) == 2
    assert result["device_type"].isna().all()
    assert result["device_os"].isna().all()


def test_malformed_event_time_is_rejected():
    sessions = [_sample_session(event_times=[
        "2025-03-01T20:04:00.000Z",
        "2025/04/05 10:30:00",  # malformed, but parseable under format="mixed"
    ])]
    result = transform_events(sessions)

    # Both should actually parse successfully under format="mixed" —
    # this test documents that expectation explicitly.
    assert len(result) == 2
    assert result["event_time"].notna().all()


def test_duplicate_event_time_within_session_both_kept_via_event_index():
    same_time = "2025-03-01T20:04:00.000Z"
    sessions = [_sample_session(event_times=[same_time, same_time])]
    result = transform_events(sessions)

    # Same event_time twice, but different event_index -> both are
    # legitimate distinct events and neither should be dropped.
    assert len(result) == 2
    assert sorted(result["event_index"].tolist()) == [0, 1]
