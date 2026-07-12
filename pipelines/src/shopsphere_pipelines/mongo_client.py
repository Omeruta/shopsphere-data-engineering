"""
MongoDB client factory for the shopsphere_events database.
"""
import logging
from functools import lru_cache

from pymongo import MongoClient
from pymongo.database import Database

from shopsphere_pipelines.config import get_mongo_config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_mongo_database() -> Database:
    cfg = get_mongo_config()
    logger.info("Connecting to MongoDB database: %s", cfg.database)
    client = MongoClient(cfg.uri, serverSelectionTimeoutMS=5000)
    return client[cfg.database]
