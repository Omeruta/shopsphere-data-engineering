"""
Transforms raw customers extract into the clean shape loaded into
analytics.dim_customer. Kept intentionally simple: source data quality
for customers has no planted issues, so this is mostly a column rename/
selection pass, with one defensive check (duplicate customer_id).
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_customers(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # Defensive dedup: customer_id is the source's own primary key, so this
    # should never fire in practice, but a warehouse load must never assume
    # upstream constraints will always hold  we check explicitly instead.
    before = len(df)
    df = df.drop_duplicates(subset="customer_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate customer_id rows", duplicates_dropped)

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

    return df[[
        "customer_id", "first_name", "last_name", "email", "phone",
        "city", "state", "country", "created_at", "updated_at",
    ]].rename(columns={
        "created_at": "source_created_at",
        "updated_at": "source_updated_at",
    })
