"""
Reads and writes control.pipeline_runs and control.pipeline_watermarks.
Every extract/load step should wrap its work in tracked_run() so runs
are logged even when something fails partway through.
"""
import logging
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import text

from shopsphere_pipelines.db import get_warehouse_engine

logger = logging.getLogger(__name__)


def start_run(pipeline_name: str, source_name: str) -> int:
    engine = get_warehouse_engine()
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                INSERT INTO control.pipeline_runs (pipeline_name, source_name, status)
                VALUES (:pipeline_name, :source_name, 'started')
                RETURNING run_id
            """),
            {"pipeline_name": pipeline_name, "source_name": source_name},
        )
        run_id = result.scalar_one()
    logger.info("Started run %s for %s/%s", run_id, pipeline_name, source_name)
    return run_id


def complete_run(
    run_id: int,
    status: str,
    records_extracted: int = 0,
    records_loaded: int = 0,
    error_message: Optional[str] = None,
    watermark_value: Optional[str] = None,
) -> None:
    if status not in ("success", "failed", "skipped"):
        raise ValueError(f"Invalid status: {status}")

    engine = get_warehouse_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE control.pipeline_runs
                SET completed_at = now(),
                    status = :status,
                    records_extracted = :records_extracted,
                    records_loaded = :records_loaded,
                    error_message = :error_message,
                    watermark_value = :watermark_value
                WHERE run_id = :run_id
            """),
            {
                "run_id": run_id,
                "status": status,
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "error_message": error_message,
                "watermark_value": watermark_value,
            },
        )
    logger.info(
        "Completed run %s status=%s extracted=%s loaded=%s",
        run_id, status, records_extracted, records_loaded,
    )


def get_watermark(pipeline_name: str, source_name: str) -> Optional[str]:
    engine = get_warehouse_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT watermark_value
                FROM control.pipeline_watermarks
                WHERE pipeline_name = :pipeline_name AND source_name = :source_name
            """),
            {"pipeline_name": pipeline_name, "source_name": source_name},
        ).first()
    return row[0] if row else None


def set_watermark(
    pipeline_name: str, source_name: str, watermark_column: str, watermark_value: str
) -> None:
    engine = get_warehouse_engine()
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO control.pipeline_watermarks
                    (pipeline_name, source_name, watermark_column, watermark_value, updated_at)
                VALUES (:pipeline_name, :source_name, :watermark_column, :watermark_value, now())
                ON CONFLICT (pipeline_name, source_name)
                DO UPDATE SET
                    watermark_column = EXCLUDED.watermark_column,
                    watermark_value = EXCLUDED.watermark_value,
                    updated_at = now()
            """),
            {
                "pipeline_name": pipeline_name,
                "source_name": source_name,
                "watermark_column": watermark_column,
                "watermark_value": watermark_value,
            },
        )
    logger.info(
        "Set watermark %s/%s: %s=%s", pipeline_name, source_name, watermark_column, watermark_value
    )


class RunTracker:
    """Mutable counters an extract/load step fills in as it works,
    read back by tracked_run() once the block finishes."""
    def __init__(self) -> None:
        self.records_extracted = 0
        self.records_loaded = 0
        self.watermark_value: Optional[str] = None


@contextmanager
def tracked_run(pipeline_name: str, source_name: str):
    """
    Usage:
        with tracked_run("extract_postgres", "customers") as run:
            df = extract_customers()
            run.records_extracted = len(df)
    Automatically records a 'started' row, then 'success' on clean exit
    or 'failed' (with the error message) if anything inside raises.
    The exception is always re-raised after logging it.
    """
    run_id = start_run(pipeline_name, source_name)
    tracker = RunTracker()
    try:
        yield tracker
    except Exception as exc:
        complete_run(run_id, status="failed", error_message=str(exc))
        logger.exception("Run %s (%s/%s) failed", run_id, pipeline_name, source_name)
        raise
    else:
        complete_run(
            run_id,
            status="success",
            records_extracted=tracker.records_extracted,
            records_loaded=tracker.records_loaded,
            watermark_value=tracker.watermark_value,
        )
