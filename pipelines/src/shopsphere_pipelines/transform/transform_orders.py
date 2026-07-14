"""
Transforms raw orders extract into the clean shape loaded into
analytics.fact_orders. Adds order_date_key for the dim_date join, and
validates order_status against the known set of valid values (defensive
check — the source schema already constrains this, but the pipeline
should not blindly trust that every upstream write went through that
constraint correctly).
"""
import logging

import pandas as pd

from shopsphere_pipelines.transform.date_keys import to_date_key

logger = logging.getLogger(__name__)

VALID_ORDER_STATUSES = {
    "pending", "confirmed", "processing", "shipped",
    "delivered", "cancelled", "returned",
}


def transform_orders(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    before = len(df)
    df = df.drop_duplicates(subset="order_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate order_id rows", duplicates_dropped)

    invalid_status_mask = ~df["order_status"].isin(VALID_ORDER_STATUSES)
    invalid_count = int(invalid_status_mask.sum())
    if invalid_count:
        logger.warning(
            "Rejecting %s orders with unrecognized order_status values: %s",
            invalid_count,
            df.loc[invalid_status_mask, "order_status"].unique().tolist(),
        )
        df = df.loc[~invalid_status_mask]

    df["order_date"] = pd.to_datetime(df["order_date"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    df["order_date_key"] = to_date_key(df["order_date"])

    return df[[
        "order_id", "customer_id", "order_status", "currency",
        "subtotal", "shipping_fee", "discount_amount", "total_amount",
        "order_date_key", "updated_at",
    ]].rename(columns={"updated_at": "source_updated_at"})
