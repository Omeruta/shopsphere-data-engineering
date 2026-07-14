"""
Generic upsert helper: writes a DataFrame into a warehouse table using
INSERT ... ON CONFLICT (...) DO UPDATE, so re-running a load never
creates duplicate rows -- it just refreshes existing ones.
"""
import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def upsert_dataframe(
    engine: Engine,
    schema: str,
    table: str,
    df: pd.DataFrame,
    conflict_columns: list[str],
) -> int:
    """
    Upserts every row in df into schema.table. conflict_columns must match
    a UNIQUE or PRIMARY KEY constraint on the table (e.g. ["customer_id"]
    for dim_customer, ["session_id", "event_index"] for
    fact_customer_events). Every non-conflict column is updated on
    conflict. Returns the number of rows attempted.
    """
    if df.empty:
        logger.info("No rows to upsert into %s.%s", schema, table)
        return 0

    columns = list(df.columns)
    update_columns = [c for c in columns if c not in conflict_columns]

    insert_cols = ", ".join(columns)
    value_placeholders = ", ".join(f":{c}" for c in columns)
    conflict_cols = ", ".join(conflict_columns)
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    # If every column is part of the conflict key, there is nothing left
    # to update on conflict -- fall back to DO NOTHING instead of emitting
    # invalid SQL with an empty SET clause.
    if update_clause:
        conflict_action = f"DO UPDATE SET {update_clause}"
    else:
        conflict_action = "DO NOTHING"

    stmt = text(f"""
        INSERT INTO {schema}.{table} ({insert_cols})
        VALUES ({value_placeholders})
        ON CONFLICT ({conflict_cols}) {conflict_action}
    """)

    # Convert to plain Python objects: SQLAlchemy's executemany-style call
    # (passing a list of dicts) does not accept pandas/NumPy scalar types
    # (e.g. numpy.int64, pandas.Timestamp, pandas.NA) as cleanly as native
    # Python types. where(pd.notna) turns pandas NA/NaT into real None,
    # which the database driver maps correctly to SQL NULL.
    records = df.where(pd.notna(df), None).to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(stmt, records)

    logger.info("Upserted %s row(s) into %s.%s", len(records), schema, table)
    return len(records)
