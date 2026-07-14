"""
Tests for transform_shipments and transform_shipment_events, covering
delivery_address flattening, status validation, and the event_index
dedup pattern applied to the shipment events array.
"""
import pandas as pd

from shopsphere_pipelines.transform.transform_shipments import (
    transform_shipment_events,
    transform_shipments,
)


def _sample_shipment(shipment_id="ship_0001", shipment_status="delivered", event_times=None):
    if event_times is None:
        event_times = ["2025-03-02T14:00:00Z", "2025-03-02T22:00:00Z"]
    return {
        "shipment_id": shipment_id,
        "order_id": 1,
        "carrier_id": "swift",
        "tracking_number": "SWD00000001NG",
        "shipment_status": shipment_status,
        "shipped_at": "2025-03-02T14:00:00Z",
        "estimated_delivery_at": "2025-03-05T14:00:00Z",
        "delivered_at": "2025-03-04T15:00:00Z",
        "updated_at": "2025-03-04T15:00:00Z",
        "delivery_address": {
            "street": "11 Market Road", "city": "Lagos", "state": "Lagos",
            "country": "Nigeria", "postal_code": "100001",
        },
        "events": [
            {"event_type": "shipment_created", "event_time": t, "location": "Lagos", "notes": "n"}
            for t in event_times
        ],
    }


def test_delivery_address_is_flattened_to_columns():
    result = transform_shipments([_sample_shipment()])

    assert result.iloc[0]["delivery_city"] == "Lagos"
    assert result.iloc[0]["delivery_postal_code"] == "100001"
    assert "delivery_address" not in result.columns


def test_invalid_shipment_status_is_rejected():
    shipments = [
        _sample_shipment(shipment_id="ship_0001", shipment_status="delivered"),
        _sample_shipment(shipment_id="ship_0002", shipment_status="not_a_real_status"),
    ]
    result = transform_shipments(shipments)

    assert len(result) == 1
    assert result.iloc[0]["shipment_id"] == "ship_0001"


def test_shipment_events_dedup_key_is_event_index_not_event_time():
    same_time = "2025-03-02T14:00:00Z"
    shipments = [_sample_shipment(event_times=[same_time, same_time])]
    result = transform_shipment_events(shipments)

    assert len(result) == 2
    assert sorted(result["event_index"].tolist()) == [0, 1]
