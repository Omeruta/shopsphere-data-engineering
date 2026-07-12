"""
Postgres engine factories for the source and warehouse databases.
Engines are created once per process and reused (SQLAlchemy pools
connections internally, so we do not want a new engine per call).
"""
import logging
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from shopsphere_pipelines.config import (
    get_source_postgres_config,
    get_warehouse_postgres_config,
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_source_engine() -> Engine:
    cfg = get_source_postgres_config()
    logger.info("Creating source Postgres engine for %s:%s/%s", cfg.host, cfg.port, cfg.db)
    return create_engine(cfg.sqlalchemy_url, pool_pre_ping=True)


@lru_cache(maxsize=1)
def get_warehouse_engine() -> Engine:
    cfg = get_warehouse_postgres_config()
    logger.info("Creating warehouse Postgres engine for %s:%s/%s", cfg.host, cfg.port, cfg.db)
    return create_engine(cfg.sqlalchemy_url, pool_pre_ping=True)
