"""
Transforms raw product_reviews documents (list of dicts from MongoDB
JSONL) into the clean shape loaded into analytics.fact_product_reviews.

Handles two planted data quality conditions in the seed data:
1. rating outside the valid 1-5 range -> rejected and logged, not
   silently clamped, since a fabricated "corrected" rating would be
   worse than an honest gap in the data.
2. created_at can be a malformed string -> parsed via
   parse_flexible_datetime; rows where it still fails are rejected,
   since created_at is NOT NULL in the target schema.

product_id values that do not exist in dim_product (orphan references,
e.g. review 88 -> product_id 8888) are intentionally NOT filtered out
here. They pass through as-is and are resolved to a null product_key at
load time, where the unmatched count is logged as its own data quality
check — a review referencing an unknown product is still a real review
and worth keeping, just without a product link.
"""
import logging
from typing import Any

import pandas as pd

from shopsphere_pipelines.transform.date_keys import to_date_key
from shopsphere_pipelines.transform.timestamps import parse_flexible_datetime

logger = logging.getLogger(__name__)

MIN_VALID_RATING = 1
MAX_VALID_RATING = 5


def transform_reviews(reviews: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(reviews)

    if df.empty:
        return df

    before = len(df)
    df = df.drop_duplicates(subset="review_id", keep="last")
    duplicates_dropped = before - len(df)
    if duplicates_dropped:
        logger.warning("Dropped %s duplicate review_id rows", duplicates_dropped)

    invalid_rating_mask = ~df["rating"].between(MIN_VALID_RATING, MAX_VALID_RATING)
    invalid_count = int(invalid_rating_mask.sum())
    if invalid_count:
        logger.warning(
            "Rejecting %s review(s) with rating outside %s-%s: %s",
            invalid_count, MIN_VALID_RATING, MAX_VALID_RATING,
            df.loc[invalid_rating_mask, "rating"].tolist(),
        )
        df = df.loc[~invalid_rating_mask]

    df["created_at"] = parse_flexible_datetime(df["created_at"], "created_at")

    before = len(df)
    df = df[df["created_at"].notna()]
    rejected = before - len(df)
    if rejected:
        logger.warning("Rejected %s review(s) with unparseable created_at", rejected)

    df["review_date_key"] = to_date_key(df["created_at"])

    return df[[
        "review_id", "product_id", "customer_id", "rating", "title",
        "review_text", "verified_purchase", "review_date_key",
        "created_at", "helpful_votes",
    ]]
