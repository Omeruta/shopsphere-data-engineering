"""
Shared helper for converting timestamps to dim_date.date_key integers
(YYYYMMDD format). Used by any transform that needs to link a fact row
to analytics.dim_date.
"""
import pandas as pd


def to_date_key(series: pd.Series) -> pd.Series:
    """
    Converts a datetime series to YYYYMMDD integers matching
    analytics.dim_date.date_key. Nulls are preserved as pandas NA
    (nullable Int64), since a missing timestamp should produce a
    missing date_key, not a fabricated one like 0 or 19700101.
    """
    dt = pd.to_datetime(series, utc=True)
    return dt.dt.strftime("%Y%m%d").astype("Int64")
