"""
Transforms raw order_items extract into the clean shape loaded into
analytics.fact_order_items. No known data quality issues in this table,
so this is a dedup pass plus a numeric sanity check on quantity.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_order_items(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    before = len(df)
    df = df.drop_duplicates(subset="order_item_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate order_item_id rows", duplicates_dropped)

    invalid_qty_mask = df["quantity"] <= 0
    invalid_count = int(invalid_qty_mask.sum())
    if invalid_count:
        logger.warning("Rejecting %s order_item(s) with non-positive quantity", invalid_count)
        df = df.loc[~invalid_qty_mask]

    return df[[
        "order_item_id", "order_id", "product_id", "quantity",
        "unit_price", "discount_amount", "line_total",
    ]]
