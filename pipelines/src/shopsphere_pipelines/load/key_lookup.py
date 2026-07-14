"""
Loads natural-key -> surrogate-key mappings from already-loaded dimension
and fact tables, so fact loaders can resolve foreign keys (e.g. a raw
customer_id into the customer_key that fact tables actually reference).
"""
import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def get_key_map(engine: Engine, schema: str, table: str, natural_col: str, surrogate_col: str) -> dict:
    """
    Returns {natural_key_value: surrogate_key_value} for every row
    currently in schema.table. Called fresh at the start of each fact
    load so it reflects dimension rows loaded earlier in the same run.
    """
    query = text(f"SELECT {natural_col}, {surrogate_col} FROM {schema}.{table}")
    with engine.connect() as conn:
        rows = conn.execute(query).all()
    mapping = {row[0]: row[1] for row in rows}
    logger.info("Loaded %s key(s) from %s.%s", len(mapping), schema, table)
    return mapping


def resolve_key(df: pd.DataFrame, natural_col: str, surrogate_col: str, key_map: dict, drop_natural: bool = True) -> pd.DataFrame:
    """
    Adds surrogate_col to df by mapping natural_col through key_map.
    Unmatched values (e.g. an orphan product_id with no matching
    dim_product row) become null in surrogate_col rather than raising --
    this is what allows a review referencing an unknown product to still
    load, with product_key = NULL, instead of crashing the whole batch.
    Logs how many rows failed to match, so the gap is visible, not silent.
    """
    df = df.copy()
    df[surrogate_col] = df[natural_col].map(key_map)

    unmatched = df[natural_col].notna() & df[surrogate_col].isna()
    unmatched_count = int(unmatched.sum())
    if unmatched_count:
        logger.warning(
            "%s row(s) have a %s with no matching %s: %s",
            unmatched_count, natural_col, surrogate_col,
            df.loc[unmatched, natural_col].unique().tolist(),
        )

    if drop_natural:
        df = df.drop(columns=[natural_col])

    return df
