"""
Enhanced Logging Service for Looker Migrator.

Provides real-time progress tracking and logging capabilities,
compatible with WebSocket-based frontend updates.
Matches the Tableau migrator pattern for consistent frontend integration.
"""

import json
import logging
import datetime
from typing import Callable, Optional, Protocol, Dict, Any
from dataclasses import dataclass
from enum import Enum

from .log_utils import logger
from .websocket_client import post_websocket_data


class LogFunction(Protocol):
    """
    Default function signature for the custom logging function.
    This function should take data as extra kwargs.
    """

    def __call__(self, *, data: dict[str, str]) -> None: ...


class LogLevel(Enum):
    """Log level enumeration."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"


@dataclass
class ProgressMessage:
    """Structure for progress messages."""
    task_id: str
    message: str
    progress: Optional[float] = None
    log_type: str = "INFO"
    phase: Optional[str] = None
    details: Optional[dict] = None


class LookerLoggingService:
    """
    Enhanced service to manage logging for Looker migration with progress tracking and WebSocket integration.

    Matches the Tableau migrator pattern for consistent frontend integration.
    """

    def __init__(self):
        self._logger: LogFunction = None
        self._task_progress = {}
        self._websocket_enabled = False
        self._current_task_id = None
        self._total_steps = 12  # Default: 12 steps like Tableau
        self._phase_weights = {
            "parsing": (0, 25),
            "extraction": (25, 50),
            "conversion": (50, 75),
            "generation": (75, 100),
        }

    @staticmethod
    def _default_message(*, data: dict[str, str]) -> None:
        """
        Default messaging function.
        This function should take a data as a dictionary.
        """
        if not data:
            data = {}
        logger.info(f"Looker Migration: {json.dumps(data, indent=2, default=str)}")

    def initialize_logger(self, func: LogFunction) -> None:
        """
        Initialize a custom logging function.
        This function should take data as kwargs which should match the default function.
        """
        self._logger = func
        logger.info(f"Looker migrator logger initialized with custom function: {func.__name__}.")

    def enable_websocket_logging(self, enabled: bool = True) -> None:
        """Enable or disable WebSocket logging."""
        self._websocket_enabled = enabled
        logger.info(f"WebSocket logging {'enabled' if enabled else 'disabled'}")

    def set_task_info(self, task_id: str, total_steps: int = 12) -> None:
        """Set task information for progress tracking."""
        existing_progress = self._task_progress.get(task_id)
        if (
            self._current_task_id == task_id
            and self._total_steps == total_steps
            and existing_progress == 0
        ):
            return

        self._current_task_id = task_id
        self._total_steps = total_steps
        self._task_progress[task_id] = 0
        logger.info(f"Task initialized: {task_id} with {total_steps} steps")

    def _set_task_progress(self, task_id: str, progress: float = 0) -> Dict[str, float]:
        """Set progress for a specific task."""
        self._task_progress[task_id] = min(100, max(0, progress))
        return self._task_progress

    def increment_progress(self, task_id: str, value: float = 1) -> 'LookerLoggingService':
        """Increment progress for specific task."""
        if task_id not in self._task_progress:
            self._task_progress[task_id] = 0

        self._task_progress[task_id] += round(value, 1)
        self._task_progress[task_id] = min(100, max(self._task_progress[task_id], 0))
        return self

    def get_progress(self, task_id: str = None) -> float:
        """Get current progress for task."""
        if task_id is None:
            task_id = self._current_task_id
        return self._task_progress.get(task_id, 0)

    def handle_message(
            self, *,
            log_type: str = "INFO",
            task_id: str = None,
            message: str,
            progress: int = None,
            increment: float = None,
            model_name: str = None,
            step_name: str = None,
            phase: str = None,
            **kwargs
    ) -> None:
        """Handle log message with progress tracking and WebSocket integration."""

        # Use current task if none specified
        if task_id is None:
            task_id = self._current_task_id

        if not task_id:
            task_id = "default_task"

        # Handle progress updates
        if increment:
            self.increment_progress(task_id, increment)
            progress = self._task_progress.get(task_id, 0)

        if progress is not None:
            self._set_task_progress(task_id, progress)

        # Get current progress
        current_progress = progress or self.get_progress(task_id)

        # Determine status based on progress and log type
        if log_type.upper() == 'ERROR':
            status = 'failed'
        elif current_progress >= 100:
            status = 'success'
        else:
            status = 'running'

        # Create comprehensive message data
        message_data = {
            'task_id': task_id,
            'log_type': log_type,
            'level': log_type.upper(),
            'message': message,
            'progress': current_progress,
            'status': status,
            'timestamp': datetime.datetime.now().isoformat(),
            'migration_type': 'looker',
            **kwargs
        }

        # Add Looker-specific information
        if model_name:
            message_data['model_name'] = model_name

        if step_name:
            message_data['step_name'] = step_name

        if phase:
            message_data['phase'] = phase

        # Send through WebSocket (primary - matches Tableau pattern)
        post_websocket_data(message_data)

        # Also send through configured logger if set (backward compatibility)
        if self._logger:
            self._logger(data=message_data)

    def log_model_info(self, model_name: str, task_id: str = None, **kwargs) -> None:
        """Log model-specific information."""
        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=f"Processing Looker model: {model_name}",
            model_name=model_name,
            **kwargs
        )

    def log_step_start(self, step_name: str, step_number: int = None, task_id: str = None, progress: int = None, **kwargs) -> None:
        """Log the start of a migration step."""
        message = f"Starting step: {step_name}"
        if step_number:
            message = f"Step {step_number}: {step_name}"

        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=message,
            step_name=step_name,
            step_number=step_number,
            progress=progress,
            **kwargs
        )

    def log_step_complete(self, step_name: str, step_number: int = None, task_id: str = None, progress: int = None, **kwargs) -> None:
        """Log the completion of a migration step."""
        message = f"Completed step: {step_name}"
        if step_number:
            message = f"Step {step_number} completed: {step_name}"

        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=message,
            step_name=step_name,
            step_number=step_number,
            progress=progress,
            **kwargs
        )

    def log_phase_progress(
        self,
        phase: str,
        phase_progress: float,
        message: str,
        task_id: str = None,
        **kwargs
    ) -> None:
        """
        Log progress within a migration phase.

        Args:
            phase: Current migration phase (parsing, extraction, conversion, generation)
            phase_progress: Progress percentage within the phase (0-100)
            message: Log message
            task_id: Task identifier
            **kwargs: Additional metadata
        """
        # Calculate actual progress based on phase weights
        if phase in self._phase_weights:
            phase_start, phase_end = self._phase_weights[phase]
            phase_range = phase_end - phase_start
            actual_progress = phase_start + (phase_progress / 100 * phase_range)
        else:
            actual_progress = phase_progress

        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=message,
            progress=int(actual_progress),
            phase=phase,
            **kwargs
        )

    def log_file_generated(self, file_path: str, file_type: str = None, task_id: str = None, **kwargs) -> None:
        """Log file generation with enhanced metadata."""
        message = f"Generated file: {file_path}"
        if file_type:
            message = f"Generated {file_type}: {file_path}"

        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=message,
            file_path=file_path,
            file_type=file_type,
            operation="file_generated",
            **kwargs
        )

    def log_conversion_progress(
        self,
        calculation_name: str,
        calculation_index: int,
        total_calculations: int,
        conversion_method: str = "rule-based",
        table_name: str = None,
        task_id: str = None,
        **kwargs
    ) -> None:
        """
        Log calculation conversion progress with AI tracking.

        Args:
            calculation_name: Name of the calculation being converted
            calculation_index: Current calculation index
            total_calculations: Total number of calculations
            conversion_method: "AI" or "rule-based"
            table_name: Name of the table containing the calculation
            task_id: Task identifier
            **kwargs: Additional metadata
        """
        # Calculate progress within conversion phase (50-75%)
        conversion_progress = 50 + int((calculation_index / max(total_calculations, 1)) * 25)

        message = f"Calculation {calculation_index}/{total_calculations} - '{calculation_name}' being converted using {conversion_method}"

        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message=message,
            progress=conversion_progress,
            phase="conversion",
            calculation_name=calculation_name,
            calculation_index=calculation_index,
            total_calculations=total_calculations,
            conversion_method=conversion_method,
            table_name=table_name,
            **kwargs
        )

    def log_error(self, error_message: str, exception: Exception = None, task_id: str = None, **kwargs) -> None:
        """Log error with enhanced error tracking."""
        message_data = {
            'log_type': 'ERROR',
            'task_id': task_id or self._current_task_id,
            'message': error_message,
            'error_type': type(exception).__name__ if exception else 'Unknown',
            **kwargs
        }

        if exception:
            message_data['exception_details'] = str(exception)

        self.handle_message(**message_data)

    def log_settings_info(self, settings: Dict[str, Any], task_id: str = None, **kwargs) -> None:
        """Log settings information for debugging."""
        self.handle_message(
            log_type="INFO",
            task_id=task_id,
            message="Migration settings configured",
            settings_summary={
                'output_format': settings.get('output', {}).get('format', 'tmdl'),
            },
            **kwargs
        )

    def get_task_summary(self, task_id: str = None) -> Dict[str, Any]:
        """Get summary information for a task."""
        if task_id is None:
            task_id = self._current_task_id

        return {
            'task_id': task_id,
            'progress': self.get_progress(task_id),
            'total_steps': self._total_steps,
            'websocket_enabled': self._websocket_enabled
        }


# Module-level instance
looker_logging_service = LookerLoggingService()


# Convenience functions for initialization
def initialize_looker_logging_function(func: Callable[[str], None]) -> None:
    """Initialize custom logging function for Looker migration."""
    looker_logging_service.initialize_logger(func)


def set_looker_task_info(task_id: str, total_steps: int = 12) -> None:
    """Set task information for Looker migration progress tracking."""
    looker_logging_service.set_task_info(task_id, total_steps)


def looker_logging_helper(
    message: str,
    progress: int = None,
    message_type: str = 'info',
    task_id: str = None,
    phase: str = None,
    **kwargs
) -> None:
    """
    Enhanced logging helper for Looker migration.

    Args:
        message: Log message
        progress: Progress percentage (0-100)
        message_type: Type of message ('info', 'warning', 'error')
        task_id: Task identifier
        phase: Current migration phase
        **kwargs: Additional metadata
    """
    looker_logging_service.handle_message(
        log_type=message_type.upper(),
        task_id=task_id,
        message=message,
        progress=progress,
        phase=phase,
        **kwargs
    )


# Legacy LoggingService class for backward compatibility
class LoggingService:
    """
    Centralized logging service for migration progress tracking.

    Supports callback-based logging for WebSocket integration.
    """

    # Phase weights for progress calculation
    PHASE_WEIGHTS = {
        "parsing": (0, 25),
        "extraction": (25, 50),
        "conversion": (50, 75),
        "generation": (75, 100),
    }

    def __init__(
        self,
        job_id: Optional[str] = None,
        callback: Optional[Callable[[str, int, str], None]] = None,
    ):
        """
        Initialize the logging service.

        Args:
            job_id: Optional job identifier
            callback: Optional callback function (phase, percent, message)
        """
        self.job_id = job_id
        self.callback = callback
        self.current_phase = "parsing"
        self.messages: list[ProgressMessage] = []

        # Also set up enhanced service
        if job_id:
            looker_logging_service.set_task_info(job_id)

    def log_phase(
        self,
        phase: str,
        percent: int,
        message: str,
        details: Optional[dict] = None,
    ) -> None:
        """
        Log progress for a phase.

        Args:
            phase: Current phase name
            percent: Overall progress percentage (0-100)
            message: Progress message
            details: Optional additional details
        """
        self.current_phase = phase

        update = ProgressMessage(
            phase=phase,
            progress=float(percent),
            message=message,
            details=details,
            task_id=self.job_id or "default",
        )
        self.messages.append(update)

        # Use enhanced logging
        looker_logging_service.handle_message(
            log_type="INFO",
            task_id=self.job_id,
            message=message,
            progress=percent,
            phase=phase,
        )

        if self.callback:
            self.callback(phase, percent, message)

    def log_info(self, message: str) -> None:
        """Log an info message."""
        looker_logging_service.handle_message(
            log_type="INFO",
            task_id=self.job_id,
            message=message,
            phase=self.current_phase,
        )

        if self.callback:
            phase_range = self.PHASE_WEIGHTS.get(self.current_phase, (0, 100))
            current_percent = (phase_range[0] + phase_range[1]) // 2
            self.callback(self.current_phase, current_percent, message)

    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        looker_logging_service.handle_message(
            log_type="WARNING",
            task_id=self.job_id,
            message=message,
            phase=self.current_phase,
        )

    def log_error(self, message: str) -> None:
        """Log an error message."""
        looker_logging_service.handle_message(
            log_type="ERROR",
            task_id=self.job_id,
            message=message,
            phase=self.current_phase,
        )

    def get_messages(self) -> list[ProgressMessage]:
        """Get all logged messages."""
        return self.messages


# Re-export for consistent API
logging_helper = looker_logging_helper
set_task_info = set_looker_task_info
