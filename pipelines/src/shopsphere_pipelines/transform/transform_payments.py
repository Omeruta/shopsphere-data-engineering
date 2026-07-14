"""
Transforms raw payments extract into the clean shape loaded into
analytics.fact_payments. Adds paid_date_key for the dim_date join —
nullable, since paid_at itself is nullable (a payment can be pending
or failed with no paid_at yet).
"""
import logging

import pandas as pd

from shopsphere_pipelines.transform.date_keys import to_date_key

logger = logging.getLogger(__name__)

VALID_PAYMENT_STATUSES = {"pending", "successful", "failed", "refunded"}


def transform_payments(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    before = len(df)
    df = df.drop_duplicates(subset="payment_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate payment_id rows", duplicates_dropped)

    invalid_status_mask = ~df["payment_status"].isin(VALID_PAYMENT_STATUSES)
    invalid_count = int(invalid_status_mask.sum())
    if invalid_count:
        logger.warning(
            "Rejecting %s payments with unrecognized payment_status values: %s",
            invalid_count,
            df.loc[invalid_status_mask, "payment_status"].unique().tolist(),
        )
        df = df.loc[~invalid_status_mask]

    df["paid_at"] = pd.to_datetime(df["paid_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["paid_date_key"] = to_date_key(df["paid_at"])

    return df[[
        "payment_id", "order_id", "payment_method", "payment_status",
        "amount", "transaction_reference", "paid_date_key", "paid_at",
        "updated_at",
    ]].rename(columns={"updated_at": "source_updated_at"})
