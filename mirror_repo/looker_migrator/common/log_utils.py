"""
Logging utilities for Looker Migrator.

Provides standardized logging functions.
"""

import logging
from typing import Optional

# Configure default logger
logger = logging.getLogger("looker_migrator")

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def log_info(message: str, extra: Optional[dict] = None) -> None:
    """Log info message."""
    logger.info(message, extra=extra)


def log_debug(message: str, extra: Optional[dict] = None) -> None:
    """Log debug message."""
    logger.debug(message, extra=extra)


def log_warning(message: str, extra: Optional[dict] = None) -> None:
    """Log warning message."""
    logger.warning(message, extra=extra)


def log_error(message: str, extra: Optional[dict] = None) -> None:
    """Log error message."""
    logger.error(message, extra=extra)


def set_log_level(level: str) -> None:
    """Set logging level."""
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    logger.setLevel(level_map.get(level.upper(), logging.INFO))
