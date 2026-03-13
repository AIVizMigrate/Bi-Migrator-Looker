"""
Enhanced WebSocket client for Looker migrator with Django integration.

This module provides functionality to send log messages to a Django WebSocket consumer.
It matches the Tableau migrator pattern for consistent frontend integration.
"""

import json
import logging
import datetime
from typing import Dict, Any, Optional, Callable, Literal

# Type hint for the WebSocket post function
WebSocketPostFunc = Optional[Callable[[Dict[str, Any]], None]]

# Global variables
_websocket_post_function: WebSocketPostFunc = None
_task_id: Optional[str] = None
_total_steps: Optional[int] = None
_current_step: int = 0
_db_save_function: Optional[Callable[[Dict[str, Any]], None]] = None

# Configure logger
logger = logging.getLogger('looker_migrator')


def set_websocket_post_function(func: WebSocketPostFunc) -> None:
    """
    Set the function that will be used to post data to WebSockets.

    Args:
        func: A function that takes a dictionary and sends it to WebSockets
    """
    global _websocket_post_function
    _websocket_post_function = func


def set_task_info(task_id: str, total_steps: int = 12) -> None:
    """
    Set information about the current task for progress tracking.

    Args:
        task_id: The ID of the current task
        total_steps: The total number of steps in the task
    """
    global _task_id, _total_steps, _current_step
    if _task_id == task_id and _total_steps == total_steps and _current_step == 0:
        return

    _task_id = task_id
    _total_steps = total_steps
    _current_step = 0
    logger.info(f"Task initialized: {task_id} with {total_steps} steps")


def increment_progress(value: float = 1) -> Optional[int]:
    """
    Increment the progress counter.

    Args:
        value: Amount to increment (default 1)

    Returns:
        The current progress percentage (0-100)
    """
    global _current_step, _total_steps
    if _total_steps is not None and _total_steps > 0:
        _current_step = min(_current_step + value, _total_steps)
        return int((_current_step / _total_steps) * 100)
    return None


def get_progress() -> Optional[int]:
    """
    Get the current progress percentage.

    Returns:
        The current progress percentage (0-100) or None if not set
    """
    global _current_step, _total_steps
    if _total_steps is not None and _total_steps > 0:
        return int((_current_step / _total_steps) * 100)
    return None


def set_db_save_function(func: Callable[[Dict[str, Any]], None]) -> None:
    """
    Set the function that will be used to save log data to the database.

    Args:
        func: A function that takes a dictionary and saves it to the database
    """
    global _db_save_function
    _db_save_function = func


def post_websocket_data(data: Dict[str, Any]) -> None:
    """
    Post data to WebSockets if a posting function has been set.

    Args:
        data: The data to post to WebSockets
    """
    global _websocket_post_function, _task_id, _db_save_function

    # Add task_id if available
    if _task_id is not None and "task_id" not in data:
        data["task_id"] = _task_id

    # Add timestamp if not present
    if "timestamp" not in data:
        data["timestamp"] = datetime.datetime.now().isoformat()

    # Send to WebSocket if function is set
    if _websocket_post_function is not None:
        try:
            _websocket_post_function(data)
        except Exception as e:
            logger.error(f"Error sending to WebSocket: {str(e)}")
    else:
        # Log that we would have sent data if the function was set
        logger.debug(f"WebSocket post function not set, would have posted: {data}")

    # Save to database if function is set
    if _db_save_function is not None:
        try:
            _db_save_function(data)
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")


def logging_helper(
    message: str,
    progress: Optional[int] = None,
    message_type: Literal['info', 'warning', 'error'] = 'info',
    options: Optional[Dict[str, Any]] = None,
    model_name: Optional[str] = None,
    step_name: Optional[str] = None,
    phase: Optional[str] = None
) -> None:
    """
    Enhanced logging helper for Looker migration with additional metadata.

    Args:
        message: Message of output or processing.
        progress: Progress of task in percentage integer. Must be within 0 and 100.
        message_type: Type of message, must be of type info, warning, error.
        options: Additional options to include in the log message.
        model_name: Name of the Looker model being processed.
        step_name: Name of the current migration step.
        phase: Current migration phase (parsing, extraction, conversion, generation).
    """
    # Map message_type to log level
    level_map = {
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR
    }
    level = level_map.get(message_type, logging.INFO)

    # Log using the standard logging system
    logger.log(level, message)

    # Use provided progress or calculate from the task info
    current_progress = progress if progress is not None else get_progress()

    # Determine status based on progress and message type
    if message_type == 'error':
        status = 'failed'
    elif current_progress is not None and current_progress >= 100:
        status = 'success'
    else:
        status = 'running'

    # Map message_type to level format that frontend expects
    level_str_map = {
        'info': 'INFO',
        'warning': 'WARNING',
        'error': 'ERROR'
    }

    data = {
        "message": message,
        "progress": current_progress,
        "level": level_str_map.get(message_type, 'INFO'),
        "message_type": message_type,
        "status": status,
        "timestamp": datetime.datetime.now().isoformat(),
        "migration_type": "looker"
    }

    # Add Looker-specific metadata
    if model_name:
        data["model_name"] = model_name

    if step_name:
        data["step_name"] = step_name

    if phase:
        data["phase"] = phase

    # Add any additional options
    if options:
        data.update(options)

    # Post the data to WebSockets and/or database
    post_websocket_data(data)


def send_looker_progress(
    task_id: str,
    progress: int,
    message: str,
    model_name: str = None,
    step_name: str = None,
    phase: str = None,
    **kwargs
) -> None:
    """
    Send Looker migration progress update via WebSocket.

    Args:
        task_id: Unique identifier for the migration task
        progress: Progress percentage (0-100)
        message: Progress message
        model_name: Name of the Looker model
        step_name: Current migration step
        phase: Current migration phase
        **kwargs: Additional metadata
    """
    # Determine status based on progress
    if progress >= 100:
        status = 'success'
    else:
        status = 'running'

    data = {
        "task_id": task_id,
        "progress": progress,
        "message": message,
        "migration_type": "looker",
        "status": status,
        "level": "INFO",
        "timestamp": datetime.datetime.now().isoformat(),
        **kwargs
    }

    if model_name:
        data["model_name"] = model_name

    if step_name:
        data["step_name"] = step_name

    if phase:
        data["phase"] = phase

    post_websocket_data(data)


def send_looker_error(
    task_id: str,
    error_message: str,
    error_type: str = None,
    model_name: str = None,
    **kwargs
) -> None:
    """
    Send Looker migration error via WebSocket.

    Args:
        task_id: Unique identifier for the migration task
        error_message: Error description
        error_type: Type of error
        model_name: Name of the Looker model
        **kwargs: Additional error metadata
    """
    data = {
        "task_id": task_id,
        "message": error_message,
        "message_type": "error",
        "level": "ERROR",
        "status": "failed",
        "migration_type": "looker",
        "timestamp": datetime.datetime.now().isoformat(),
        **kwargs
    }

    if error_type:
        data["error_type"] = error_type

    if model_name:
        data["model_name"] = model_name

    post_websocket_data(data)


def send_looker_completion(
    task_id: str,
    success: bool,
    model_name: str = None,
    output_files: list = None,
    tables_count: int = 0,
    measures_count: int = 0,
    relationships_count: int = 0,
    **kwargs
) -> None:
    """
    Send Looker migration completion status via WebSocket.

    Args:
        task_id: Unique identifier for the migration task
        success: Whether migration completed successfully
        model_name: Name of the Looker model
        output_files: List of generated output files
        tables_count: Number of tables generated
        measures_count: Number of measures generated
        relationships_count: Number of relationships generated
        **kwargs: Additional completion metadata
    """
    data = {
        "task_id": task_id,
        "message": f"Looker migration {'completed successfully' if success else 'failed'}",
        "message_type": "info" if success else "error",
        "level": "INFO" if success else "ERROR",
        "migration_type": "looker",
        "migration_status": "completed",
        "success": success,
        "status": "success" if success else "failed",
        "progress": 100 if success else None,
        "tables_count": tables_count,
        "measures_count": measures_count,
        "relationships_count": relationships_count,
        "timestamp": datetime.datetime.now().isoformat(),
        **kwargs
    }

    if model_name:
        data["model_name"] = model_name

    if output_files:
        data["output_files"] = output_files

    post_websocket_data(data)


def send_conversion_progress(
    task_id: str,
    calculation_name: str,
    calculation_index: int,
    total_calculations: int,
    conversion_method: str = "rule-based",
    table_name: str = None,
    **kwargs
) -> None:
    """
    Send calculation conversion progress for AI tracking.

    Args:
        task_id: Unique identifier for the migration task
        calculation_name: Name of the calculation being converted
        calculation_index: Current calculation index
        total_calculations: Total number of calculations
        conversion_method: "AI" or "rule-based"
        table_name: Name of the table containing the calculation
        **kwargs: Additional metadata
    """
    # Calculate progress within conversion phase (50-75%)
    conversion_progress = 50 + int((calculation_index / max(total_calculations, 1)) * 25)

    message = f"Calculation {calculation_index}/{total_calculations} - '{calculation_name}' being converted using {conversion_method}"

    data = {
        "task_id": task_id,
        "message": message,
        "progress": conversion_progress,
        "level": "INFO",
        "status": "running",
        "migration_type": "looker",
        "phase": "conversion",
        "calculation_name": calculation_name,
        "calculation_index": calculation_index,
        "total_calculations": total_calculations,
        "conversion_method": conversion_method,
        "timestamp": datetime.datetime.now().isoformat(),
        **kwargs
    }

    if table_name:
        data["table_name"] = table_name

    post_websocket_data(data)


# Create a class that can be used as a logging handler
class WebSocketLogHandler(logging.Handler):
    """
    A logging handler that sends log messages to WebSockets.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        """
        Emit a log record to WebSockets.

        Args:
            record: The log record to emit
        """
        # Determine message type based on log level
        message_type = 'info'
        if record.levelno >= logging.ERROR:
            message_type = 'error'
        elif record.levelno >= logging.WARNING:
            message_type = 'warning'

        # Format the message
        message = self.format(record)

        # Send the log message
        logging_helper(message, message_type=message_type)
