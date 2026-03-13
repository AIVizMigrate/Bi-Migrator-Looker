"""
Looker to Power BI Migrator.

This package provides tools for migrating Looker LookML projects
to Power BI TMDL format.

Example usage:
    from looker_migrator import migrate_lookml_project

    result = migrate_lookml_project(
        input_path="path/to/lookml/project",
        output_dir="path/to/output",
    )

    if result.success:
        print(f"Migration complete: {result.output_path}")
    else:
        print(f"Migration failed: {result.errors}")
"""

__version__ = "0.1.0"
__author__ = "BI Migrator Team"

from .main import (
    migrate_lookml_project,
    migrate_lookml_view,
    migrate_lookml_project_arch,
    migrate_single_project,
    migrate_single_workbook,
    LookerMigrator,
)
from .config import (
    Settings,
    load_settings,
)
from .common.websocket_client import (
    set_websocket_post_function,
    set_task_info,
    logging_helper,
    send_conversion_progress,
)
from .common.calculation_tracker import (
    CalculationTracker,
    get_calculation_tracker,
    reset_calculation_tracker,
)
from .models import (
    MigrationResult,
    MigrationError,
    MigrationWarning,
    LookmlProject,
    LookmlModel,
    LookmlView,
    PbiModel,
)

__all__ = [
    # Main entry points
    "migrate_lookml_project",
    "migrate_lookml_view",
    "migrate_lookml_project_arch",
    "migrate_single_project",
    "migrate_single_workbook",
    "LookerMigrator",
    # Configuration
    "Settings",
    "load_settings",
    # Logging (Tableau-style WebSocket integration)
    "set_websocket_post_function",
    "set_task_info",
    "logging_helper",
    "send_conversion_progress",
    # Calculation tracking
    "CalculationTracker",
    "get_calculation_tracker",
    "reset_calculation_tracker",
    # Models
    "MigrationResult",
    "MigrationError",
    "MigrationWarning",
    "LookmlProject",
    "LookmlModel",
    "LookmlView",
    "PbiModel",
    # Version
    "__version__",
]
