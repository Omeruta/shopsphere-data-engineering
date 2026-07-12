"""
Shared logging setup. Call setup_logging() once at the start of any
entrypoint script (run_pipeline.py, individual extract scripts, etc.).
"""
import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
