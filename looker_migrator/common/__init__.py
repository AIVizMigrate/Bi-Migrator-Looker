"""Common utilities for Looker Migrator."""

from .log_utils import log_info, log_debug, log_warning, log_error, logger
from .logging_service import (
    LoggingService,
    LookerLoggingService,
    looker_logging_service,
    initialize_looker_logging_function,
    set_looker_task_info,
    looker_logging_helper,
    logging_helper,
    set_task_info,
    ProgressMessage,
)
from .deduplication import (
    TableDeduplicator,
    DeduplicationResult,
    deduplicate_measures_for_table,
)
from .websocket_client import (
    set_websocket_post_function,
    set_task_info as ws_set_task_info,
    increment_progress,
    get_progress,
    post_websocket_data,
    logging_helper as ws_logging_helper,
    send_looker_progress,
    send_looker_error,
    send_looker_completion,
    send_conversion_progress,
)

__all__ = [
    # Log utilities
    "log_info",
    "log_debug",
    "log_warning",
    "log_error",
    "logger",
    # Logging service
    "LoggingService",
    "LookerLoggingService",
    "looker_logging_service",
    "initialize_looker_logging_function",
    "set_looker_task_info",
    "looker_logging_helper",
    "logging_helper",
    "set_task_info",
    "ProgressMessage",
    # Deduplication
    "TableDeduplicator",
    "DeduplicationResult",
    "deduplicate_measures_for_table",
    # WebSocket client
    "set_websocket_post_function",
    "ws_set_task_info",
    "increment_progress",
    "get_progress",
    "post_websocket_data",
    "ws_logging_helper",
    "send_looker_progress",
    "send_looker_error",
    "send_looker_completion",
    "send_conversion_progress",
]
