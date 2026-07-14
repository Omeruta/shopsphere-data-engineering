"""
Shared helper for parsing timestamps that may arrive in mixed/malformed
formats. The MongoDB seed data intentionally includes non-ISO strings
(e.g. "2025/04/05 10:30:00", "04-15-2025 12:00:00") mixed in among
normal ISO-8601 values, to simulate a real messy upstream source.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def parse_flexible_datetime(series: pd.Series, field_name: str) -> pd.Series:
    """
    Parses a Series of timestamp strings in mixed formats. format="mixed"
    tells pandas to infer the format per-value instead of requiring one
    consistent format for the whole column. Values that still cannot be
    parsed become NaT (errors="coerce") rather than raising and crashing
    the whole batch over one bad row.
    """
    parsed = pd.to_datetime(series, utc=True, format="mixed", errors="coerce")

    originally_present = series.notna()
    failed_to_parse = parsed.isna() & originally_present
    failed_count = int(failed_to_parse.sum())
    if failed_count:
        logger.warning(
            "%s: %s value(s) could not be parsed as a timestamp and became null: %s",
            field_name, failed_count, series[failed_to_parse].unique().tolist(),
        )

    return parsed
