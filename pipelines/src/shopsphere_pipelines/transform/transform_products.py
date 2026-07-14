"""
Transforms raw products extract into the clean shape loaded into
analytics.dim_product.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_products(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    before = len(df)
    df = df.drop_duplicates(subset="product_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate product_id rows", duplicates_dropped)

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

    return df[[
        "product_id", "product_name", "category", "brand",
        "unit_price", "cost_price", "stock_quantity",
        "created_at", "updated_at",
    ]].rename(columns={
        "created_at": "source_created_at",
        "updated_at": "source_updated_at",
    })
