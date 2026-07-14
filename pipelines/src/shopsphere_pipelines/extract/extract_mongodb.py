"""
Extracts MongoDB collections (customer_sessions, product_reviews) in full
each run. Both collections are small enough that a full re-read every run
is simple and cheap  no watermark tracking needed here.
Raw documents are written to MinIO bronze as JSON Lines, since their
nested/irregular structure (events arrays, an optional device object)
does not map cleanly onto a fixed tabular schema.
"""
import logging
from datetime import date
from typing import Optional

from shopsphere_pipelines.control import tracked_run
from shopsphere_pipelines.logging_config import setup_logging
from shopsphere_pipelines.minio_client import build_object_path, write_jsonl
from shopsphere_pipelines.mongo_client import get_mongo_database

logger = logging.getLogger(__name__)

PIPELINE_NAME = "extract_mongodb"

COLLECTIONS = ["customer_sessions", "product_reviews"]


def _strip_object_id(doc: dict) -> dict:
    """Mongo auto-adds an _id ObjectId to every document. We do not need it
    downstream (session_id / review_id are the real natural keys), so drop
    it here once rather than handling it at every later read site."""
    doc = dict(doc)
    doc.pop("_id", None)
    return doc


def extract_collection(collection_name: str, run_date: date) -> None:
    with tracked_run(PIPELINE_NAME, collection_name) as run:
        db = get_mongo_database()
        documents = [_strip_object_id(doc) for doc in db[collection_name].find({})]
        run.records_extracted = len(documents)

        if not documents:
            logger.info("No documents found in %s, skipping MinIO write", collection_name)
            return

        object_path = build_object_path(
            layer="bronze",
            system="mongodb",
            dataset=collection_name,
            run_date=run_date,
            filename=f"{collection_name}.jsonl",
        )
        write_jsonl(documents, object_path)


def run_all(run_date: Optional[date] = None) -> None:
    run_date = run_date or date.today()
    for collection_name in COLLECTIONS:
        extract_collection(collection_name, run_date)


if __name__ == "__main__":
    setup_logging()
    run_all()
