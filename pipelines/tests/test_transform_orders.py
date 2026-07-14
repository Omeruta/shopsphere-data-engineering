"""
Tests for transform_orders, focused on the status validation and
date_key derivation logic — the two behaviors with real business impact
if they silently broke.
"""
import pandas as pd

from shopsphere_pipelines.transform.transform_orders import transform_orders


def _sample_row(order_id=1, order_status="delivered"):
    return {
        "order_id": order_id,
        "customer_id": 1,
        "order_status": order_status,
        "order_date": "2025-03-15T10:00:00Z",
        "currency": "NGN",
        "subtotal": 1000.00,
        "shipping_fee": 100.00,
        "discount_amount": 0.00,
        "total_amount": 1100.00,
        "updated_at": "2025-03-16T10:00:00Z",
    }


def test_valid_status_passes_through():
    raw = pd.DataFrame([_sample_row(order_status="delivered")])
    result = transform_orders(raw)

    assert len(result) == 1
    assert result.iloc[0]["order_status"] == "delivered"


def test_invalid_status_is_rejected():
    raw = pd.DataFrame([
        _sample_row(order_id=1, order_status="delivered"),
        _sample_row(order_id=2, order_status="not_a_real_status"),
    ])
    result = transform_orders(raw)

    assert len(result) == 1
    assert result.iloc[0]["order_id"] == 1


def test_order_date_key_matches_yyyymmdd():
    raw = pd.DataFrame([_sample_row()])
    result = transform_orders(raw)

    assert result.iloc[0]["order_date_key"] == 20250315


def test_drops_duplicate_order_id_keeping_last():
    raw = pd.DataFrame([
        {**_sample_row(order_id=1), "total_amount": 1100.00},
        {**_sample_row(order_id=1), "total_amount": 1200.00},
    ])
    result = transform_orders(raw)

    assert len(result) == 1
    assert result.iloc[0]["total_amount"] == 1200.00
