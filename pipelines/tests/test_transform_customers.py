"""
Tests for transform_customers. Uses small hand-built DataFrames rather
than live database data, so these run instantly with no Docker/network
dependency.
"""
import pandas as pd

from shopsphere_pipelines.transform.transform_customers import transform_customers


def _sample_row(customer_id=1, updated_at="2025-03-01T10:00:00Z"):
    return {
        "customer_id": customer_id,
        "first_name": "Ada",
        "last_name": "Obi",
        "email": "ada@example.com",
        "phone": "+2348000000000",
        "city": "Lagos",
        "state": "Lagos",
        "country": "Nigeria",
        "created_at": "2025-01-01T10:00:00Z",
        "updated_at": updated_at,
    }


def test_renames_timestamp_columns():
    raw = pd.DataFrame([_sample_row()])
    result = transform_customers(raw)

    assert "source_created_at" in result.columns
    assert "source_updated_at" in result.columns
    assert "created_at" not in result.columns
    assert "updated_at" not in result.columns


def test_drops_duplicate_customer_id_keeping_last():
    raw = pd.DataFrame([
        _sample_row(customer_id=1, updated_at="2025-03-01T10:00:00Z"),
        _sample_row(customer_id=1, updated_at="2025-03-02T10:00:00Z"),  # newer duplicate
    ])
    result = transform_customers(raw)

    assert len(result) == 1
    assert result.iloc[0]["source_updated_at"] == pd.Timestamp("2025-03-02T10:00:00Z")


def test_timestamps_are_parsed_as_datetime():
    raw = pd.DataFrame([_sample_row()])
    result = transform_customers(raw)

    assert pd.api.types.is_datetime64_any_dtype(result["source_created_at"])
    assert pd.api.types.is_datetime64_any_dtype(result["source_updated_at"])
