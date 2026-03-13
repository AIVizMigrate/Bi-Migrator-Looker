"""
Model Generator for Looker to Power BI conversion.

Orchestrates conversion from Looker project to Power BI model
with per-calculation progress tracking and WebSocket logging.

Follows Tableau's 2-phase calculation tracking pattern:
  Phase 1: Extract all calculations (add_looker_calculation with FormulaDax='')
  Phase 2: Convert and update (update_powerbi_calculation with DAX)
"""

import re
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from ..models import (
    LookmlProject,
    LookmlModel,
    LookmlView,
    LookmlExplore,
    PbiModel,
    PbiTable,
    PbiRelationship,
    MigrationWarning,
)
from ..converters import ExpressionConverter, JoinConverter, DaxApiConfig
from ..common.log_utils import log_info, log_debug, log_warning
from ..common.calculation_tracker import CalculationTracker, get_calculation_tracker, reset_calculation_tracker
from .view_converter import ViewConverter


class ModelGenerator:
    """
    Generates a Power BI model from a Looker project with calculation tracking.
    """

    def __init__(
        self,
        config: Optional[dict] = None,
        dax_api_config: Optional[DaxApiConfig] = None,
        task_id: Optional[str] = None,
        output_dir: Optional[Path] = None,
    ):
        """
        Initialize the model generator.

        Args:
            config: General configuration dictionary
            dax_api_config: Configuration for DAX API client
            task_id: Task ID for progress tracking and WebSocket logging
            output_dir: Output directory for calculations.json persistence
        """
        self.config = config or {}
        self.dax_api_config = dax_api_config
        self.task_id = task_id
        self.output_dir = output_dir

        # Initialize calculation tracker (matches Tableau pattern)
        reset_calculation_tracker()  # Start fresh for each migration
        self.calculation_tracker = get_calculation_tracker(
            task_id=task_id,
            output_dir=output_dir,
        )

        # Initialize components — AI conversion by default, falls back to rule-based
        self.expression_converter = ExpressionConverter(
            config=config,
            dax_api_config=dax_api_config,
        )
        self.view_converter = ViewConverter(
            expression_converter=self.expression_converter,
            config=config,
            calculation_tracker=self.calculation_tracker,
            task_id=task_id,
        )
        self.join_converter = JoinConverter()

        self.warnings: list[MigrationWarning] = []
        self.conversion_stats: dict[str, int] = {}

    def generate_from_project(
        self,
        project: LookmlProject,
        model_name: Optional[str] = None,
    ) -> PbiModel:
        """
        Generate a Power BI model from a Looker project.

        Follows Tableau's 2-phase pattern:
          Phase 1: Extract ALL calculations (register in tracker with FormulaDax='')
          Phase 2: Convert each view (update tracker with actual DAX)

        Args:
            project: Parsed Looker project
            model_name: Optional name for PBI model

        Returns:
            Power BI model definition
        """
        self.warnings = []
        self.expression_converter.reset_conversion_stats()

        name = model_name or project.name
        log_info(f"Generating Power BI model: {name}")

        # Determine connection type
        connection_type = self._infer_connection_type(project.connection)
        primary_keys_by_view = self._build_primary_keys_by_view(project.views)

        # Phase 1: Pre-count and register ALL calculations BEFORE conversion starts
        # (Tableau pattern: total is known upfront so progress shows "X/TOTAL" correctly)
        total_calculations = self._count_all_calculations(project.views)
        self.calculation_tracker.set_total_calculations(total_calculations)
        self._register_all_calculations(project.views, connection_type)
        log_info(f"Total calculations to convert: {total_calculations}")

        # Phase 2: Convert views to tables (update_powerbi_calculation called per calc)
        tables = []
        view_to_table = {}
        total_views = len(project.views)

        for i, view in enumerate(project.views, 1):
            dim_count = len(view.dimensions) if view.dimensions else 0
            measure_count = len(view.measures) if view.measures else 0
            log_info(
                f"Converting view {i}/{total_views}: {view.name} "
                f"({dim_count} dimensions, {measure_count} measures)"
            )
            try:
                table = self.view_converter.convert(view, connection_type)
                tables.append(table)
                view_to_table[view.name] = table.name
            except Exception as e:
                log_warning(f"Failed to convert view {view.name}: {e}")
                self.warnings.append(
                    MigrationWarning(
                        source_element=view.name,
                        message=f"View conversion failed: {str(e)}",
                    )
                )

        # Power BI requires globally unique measure names across the model.
        self._ensure_unique_measure_names(tables)

        # Convert relationships from explores
        relationships = []
        for model in project.models:
            for explore in model.explores:
                rels = self._convert_explore_joins(
                    explore,
                    view_to_table,
                    primary_keys_by_view,
                )
                relationships.extend(rels)

        log_info(
            f"Generated model with {len(tables)} tables, "
            f"{len(relationships)} relationships"
        )
        self.conversion_stats = self.expression_converter.get_conversion_stats()

        return PbiModel(
            name=name,
            tables=tables,
            relationships=relationships,
            description=f"Migrated from Looker project: {project.name}",
        )

    def generate_from_view(
        self,
        view: LookmlView,
        model_name: Optional[str] = None,
        connection_type: str = "sql_server",
    ) -> PbiModel:
        """
        Generate a Power BI model from a single view.

        Args:
            view: Looker view
            model_name: Optional model name
            connection_type: Database connection type

        Returns:
            Power BI model with single table
        """
        name = model_name or view.name
        log_info(f"Generating model from view: {view.name}")
        self.expression_converter.reset_conversion_stats()

        table = self.view_converter.convert(view, connection_type)
        self._ensure_unique_measure_names([table])
        self.conversion_stats = self.expression_converter.get_conversion_stats()

        return PbiModel(
            name=name,
            tables=[table],
            relationships=[],
            description=view.description or f"Migrated from Looker view: {view.name}",
        )

    def generate_from_explore(
        self,
        explore: LookmlExplore,
        views: dict[str, LookmlView],
        model_name: Optional[str] = None,
        connection_type: str = "sql_server",
    ) -> PbiModel:
        """
        Generate a Power BI model from an explore and its views.

        Args:
            explore: Looker explore
            views: Dictionary of available views
            model_name: Optional model name
            connection_type: Database connection type

        Returns:
            Power BI model
        """
        name = model_name or explore.name
        log_info(f"Generating model from explore: {explore.name}")
        self.expression_converter.reset_conversion_stats()

        tables = []
        view_to_table = {}

        # Get base view
        base_view_name = explore.view_name or explore.name
        if base_view_name in views:
            base_view = views[base_view_name]
            table = self.view_converter.convert(base_view, connection_type)
            tables.append(table)
            view_to_table[base_view_name] = table.name

        # Get joined views
        for join in explore.joins:
            view_name = join.from_view or join.name
            if view_name in views:
                view = views[view_name]
                table = self.view_converter.convert(view, connection_type)
                tables.append(table)
                view_to_table[view_name] = table.name

        self._ensure_unique_measure_names(tables)
        self.conversion_stats = self.expression_converter.get_conversion_stats()

        # Convert joins to relationships
        relationships = self._convert_explore_joins(explore, view_to_table)

        return PbiModel(
            name=name,
            tables=tables,
            relationships=relationships,
            description=explore.description or f"Migrated from Looker explore: {explore.name}",
        )

    def _convert_explore_joins(
        self,
        explore: LookmlExplore,
        view_to_table: dict[str, str],
        primary_keys_by_view: Optional[dict[str, set[str]]] = None,
    ) -> list[PbiRelationship]:
        """Convert explore joins to relationships."""
        if not explore.joins:
            return []

        base_view = explore.view_name or explore.name
        relationships = self.join_converter.convert_all(
            explore.joins,
            base_view,
            view_to_table,
            primary_keys_by_view=primary_keys_by_view,
        )

        return relationships

    def _count_all_calculations(self, views: list[LookmlView]) -> int:
        """
        Count all calculations (measures + calculated dimensions) across all views.

        Called BEFORE conversion so the total is known upfront for progress tracking.
        Matches Tableau's pre-count pattern in column_parser_calculated.py.

        Args:
            views: All Looker views in the project

        Returns:
            Total number of calculations to convert
        """
        total = 0
        for view in views:
            # Count measures
            total += len(view.measures) if view.measures else 0

            # Count calculated dimensions (those with non-simple SQL)
            if view.dimensions:
                for dim in view.dimensions:
                    if dim.sql and not self.view_converter._is_simple_table_reference(dim.sql):
                        total += 1

        return total

    def _register_all_calculations(
        self,
        views: list[LookmlView],
        connection_type: str,
    ) -> None:
        """
        Register ALL calculations in the tracker BEFORE conversion starts.

        Matches Tableau's pattern in join_structure_analyzer.py where all calculations
        are added via add_tableau_calculation() with FormulaDax='' before any conversion.

        Args:
            views: All Looker views in the project
            connection_type: Database connection type
        """
        for view in views:
            table_name = self.view_converter._sanitize_name(view.name)

            # Register measures
            for measure in (view.measures or []):
                measure_name = self.view_converter._sanitize_name(measure.name)
                raw_type = getattr(measure.type, "value", measure.type)
                measure_type = str(raw_type).lower() if raw_type else "count"

                self.calculation_tracker.add_looker_calculation(
                    table_name=table_name,
                    calculation_name=measure_name,
                    expression=measure.sql or "",
                    formula_type="measure",
                    looker_type=measure_type,
                    description=measure.description or measure.label,
                )

            # Register calculated dimensions
            for dim in (view.dimensions or []):
                if not dim.sql or self.view_converter._is_simple_table_reference(dim.sql):
                    continue

                column_name = self.view_converter._sanitize_name(dim.name)
                dim_type = getattr(dim.type, "value", dim.type)
                dim_type_text = str(dim_type).lower() if dim_type else ""

                self.calculation_tracker.add_looker_calculation(
                    table_name=table_name,
                    calculation_name=column_name,
                    expression=dim.sql,
                    formula_type="calculated_column",
                    looker_type=dim_type_text,
                    description=dim.description or dim.label,
                )

    @staticmethod
    def _build_primary_keys_by_view(views: list[LookmlView]) -> dict[str, set[str]]:
        """Build a map of view -> primary key fields for relationship safety checks."""
        pk_map: dict[str, set[str]] = {}
        table_ref_pattern = re.compile(r'^\$\{TABLE\}\.([A-Za-z0-9_]+)$', re.IGNORECASE)

        for view in views:
            keys: set[str] = set()
            for dim in view.dimensions:
                if not dim.primary_key:
                    continue
                if dim.name:
                    keys.add(dim.name.lower())
                sql = (dim.sql or "").strip()
                match = table_ref_pattern.match(sql)
                if match:
                    keys.add(match.group(1).lower())

            pk_map[view.name] = keys

        return pk_map

    def _infer_connection_type(self, connection: Optional[str]) -> str:
        """Infer database type from connection name."""
        if not connection:
            configured_default = None
            if isinstance(self.config, dict):
                converter_cfg = self.config.get("converter", {})
                if isinstance(converter_cfg, dict):
                    configured_default = converter_cfg.get("default_connection_type")
            return str(configured_default or "sql_server")

        conn_lower = connection.lower()

        if 'bigquery' in conn_lower or 'bq' in conn_lower:
            return "bigquery"
        elif 'lookerdata' in conn_lower:
            return "bigquery"
        elif 'local_csv' in conn_lower or 'csv' == conn_lower or 'file_csv' in conn_lower:
            return "local_csv"
        elif 'snowflake' in conn_lower or 'sf' in conn_lower:
            return "snowflake"
        elif 'redshift' in conn_lower or 'rs' in conn_lower:
            return "redshift"
        elif 'postgres' in conn_lower or 'pg' in conn_lower:
            return "postgresql"
        elif 'mysql' in conn_lower:
            return "mysql"

        return "sql_server"

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Case-insensitive normalization for uniqueness checks."""
        return (name or "").strip().lower()

    def _ensure_unique_measure_names(self, tables: list[PbiTable]) -> None:
        """Rename duplicate measure names so they are unique across the whole model."""
        used_names: set[str] = set()
        renamed_count = 0

        for table in tables:
            for measure in table.measures:
                if not measure.name or not measure.name.strip():
                    measure.name = "Measure"

                base_name = measure.name.strip()
                candidate = base_name
                candidate_key = self._normalize_name(candidate)

                if candidate_key not in used_names:
                    used_names.add(candidate_key)
                    continue

                prefix_base = f"{table.name}_{base_name}"
                candidate = prefix_base
                candidate_key = self._normalize_name(candidate)
                suffix = 2
                while candidate_key in used_names:
                    candidate = f"{prefix_base}_{suffix}"
                    candidate_key = self._normalize_name(candidate)
                    suffix += 1

                old_name = measure.name
                measure.name = candidate
                used_names.add(candidate_key)
                renamed_count += 1

                message = (
                    f"Renamed duplicate measure '{old_name}' to '{candidate}' "
                    f"in table '{table.name}'"
                )
                log_debug(message)
                self.warnings.append(
                    MigrationWarning(
                        code="DUPLICATE_MEASURE_RENAMED",
                        source_element=table.name,
                        message=message,
                    )
                )

        if renamed_count:
            log_info(
                "Normalized %d duplicate measure name(s) for model-wide uniqueness."
                % renamed_count
            )

    def get_warnings(self) -> list[MigrationWarning]:
        """Get warnings from the last generation."""
        return self.warnings

    def get_calculation_tracker(self) -> CalculationTracker:
        """Get the calculation tracker with all tracked calculations."""
        return self.calculation_tracker

    def export_calculations(self, output_path: Path) -> Path:
        """
        Export tracked calculations to JSON file.

        Args:
            output_path: Directory to write calculations.json

        Returns:
            Path to the generated file
        """
        return self.calculation_tracker.export_calculations_json(output_path)

    def get_calculation_summary(self) -> dict:
        """Get summary statistics for calculations."""
        return self.calculation_tracker.get_summary()

    def get_conversion_stats(self) -> dict[str, int]:
        """Get API usage counters from the last generation."""
        return dict(self.conversion_stats)
