"""Centralized logging configuration."""
import logging
import sys
from datetime import datetime
from shared.fdk_constants import IST


class ISTFormatter(logging.Formatter):
    """Formatter that always uses IST regardless of server timezone."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%H:%M:%S")


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(f"fdk.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(ISTFormatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
