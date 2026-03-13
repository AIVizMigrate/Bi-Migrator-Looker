"""
View Converter for Looker to Power BI conversion.

Converts Looker views to Power BI tables with per-calculation
progress tracking and WebSocket logging.
"""

import re
from typing import Optional

from ..models import (
    LookmlView,
    LookmlDimension,
    LookmlMeasure,
    PbiTable,
    PbiColumn,
    PbiMeasure,
    PbiPartition,
    DataType,
)
from ..converters import ExpressionConverter, DatatypeMapper, SqlToDaxConverter
from ..common.log_utils import log_debug, log_warning
from ..common.calculation_tracker import CalculationTracker


class ViewConverter:
    """
    Converts Looker views to Power BI tables with calculation tracking.
    """

    def __init__(
        self,
        expression_converter: Optional[ExpressionConverter] = None,
        config: Optional[dict] = None,
        calculation_tracker: Optional[CalculationTracker] = None,
        task_id: Optional[str] = None,
    ):
        """
        Initialize the view converter.

        Args:
            expression_converter: Expression converter instance
            config: Configuration dictionary
            calculation_tracker: Optional tracker for calculation progress
            task_id: Task ID for progress reporting
        """
        self.expression_converter = expression_converter or ExpressionConverter()
        self.config = config or {}
        self.sql_converter = SqlToDaxConverter(self.config)
        self.calculation_tracker = calculation_tracker
        self.task_id = task_id

    def convert(
        self,
        view: LookmlView,
        connection_type: str = "sql_server",
    ) -> PbiTable:
        """
        Convert a Looker view to a Power BI table.

        Args:
            view: Looker view definition
            connection_type: Database connection type

        Returns:
            Power BI table definition
        """
        log_debug(f"Converting view: {view.name}")

        # Convert dimensions to columns
        columns = []
        table_name = self._sanitize_name(view.name)
        column_mappings: dict[str, str] = {}
        seen_column_names: set[str] = set()
        for dim in view.dimensions:
            col = self._convert_dimension(dim, view.name)
            if not col:
                continue

            expanded_cols = self._expand_dimension_columns(dim, col)
            for expanded_col, looker_field_name in expanded_cols:
                normalized_col_name = expanded_col.name.lower()
                if normalized_col_name in seen_column_names:
                    continue

                seen_column_names.add(normalized_col_name)
                columns.append(expanded_col)

                dax_ref = f"{table_name}[{expanded_col.name}]"
                key = looker_field_name.lower()
                column_mappings[key] = dax_ref
                column_mappings[f"table.{looker_field_name}".lower()] = dax_ref
                column_mappings[f"{view.name}.{looker_field_name}".lower()] = dax_ref

        # Convert measures
        measures = []
        for measure in view.measures:
            m = self._convert_measure(measure, view.name, column_mappings)
            if m:
                measures.append(m)

        # Generate partition (data source)
        partition = self._generate_partition(view, connection_type)

        return PbiTable(
            name=table_name,
            columns=columns,
            measures=measures,
            partitions=[partition] if partition else [],
            description=view.description or view.label,
            is_hidden=False,
        )

    def _convert_dimension(
        self,
        dimension: LookmlDimension,
        view_name: str,
    ) -> Optional[PbiColumn]:
        """Convert a Looker dimension to Power BI column with tracking for calculated columns."""
        dim_type = getattr(dimension.type, "value", dimension.type)
        dim_type_text = str(dim_type).lower() if dim_type else ""
        table_name = self._sanitize_name(view_name)
        column_name = self._sanitize_name(dimension.name)

        # Handle dimension_group (time dimensions)
        if dim_type_text == 'time' and dimension.timeframes:
            # Create column for the base time field
            data_type = DataType.DATETIME
        else:
            data_type = DatatypeMapper.map_type(dim_type)

        # Determine if hidden
        is_hidden = dimension.hidden

        # Get format string
        format_string = ""
        if dimension.value_format:
            format_string = dimension.value_format
        elif dimension.value_format_name:
            format_string = self._convert_format_name(dimension.value_format_name)

        if dimension.sql and not self._is_simple_table_reference(dimension.sql):
            # This is a calculated dimension — already registered in tracker by
            # _register_all_calculations(). Now convert and update tracker.
            converted = self.expression_converter.convert_dimension(
                dimension=dimension,
                view_name=view_name,
                table_name=table_name,
            )

            if not converted.dax_expression:
                log_warning(
                    f"Could not convert calculated dimension '{dimension.name}' in view '{view_name}'"
                )
                if self.calculation_tracker:
                    self.calculation_tracker.fail_conversion(
                        column_name, table_name, "Could not generate DAX expression"
                    )
                return None

            # Determine conversion method
            conversion_method = "AI" if getattr(converted, 'used_api', False) else "rule-based"

            # Update tracker with DAX (matches Tableau's update_powerbi_calculation)
            if self.calculation_tracker:
                self.calculation_tracker.update_powerbi_calculation(
                    table_name=table_name,
                    calculation_name=column_name,
                    powerbi_name=column_name,
                    dax_expression=converted.dax_expression,
                    conversion_method=conversion_method,
                    confidence=converted.confidence,
                    used_api=getattr(converted, 'used_api', False),
                    format_string=format_string,
                    summarize_by="none",
                    warnings=converted.warnings,
                )

            return PbiColumn(
                name=column_name,
                data_type=data_type,
                source_column=None,
                expression=converted.dax_expression,
                is_calculated=True,
                is_hidden=is_hidden,
                format_string=format_string,
                summarize_by=DatatypeMapper.get_summarize_by(data_type),
                description=dimension.description or dimension.label,
                display_folder=dimension.group_label,
                looker_name=dimension.name,
                formula_looker=dimension.sql,
                original_expression=dimension.sql,
            )

        source_column = self._resolve_source_column(dimension)

        return PbiColumn(
            name=column_name,
            data_type=data_type,
            source_column=source_column,
            is_hidden=is_hidden,
            format_string=format_string,
            summarize_by=DatatypeMapper.get_summarize_by(data_type),
            description=dimension.description or dimension.label,
            display_folder=dimension.group_label,
            looker_name=dimension.name,
            formula_looker=dimension.sql if dimension.sql else None,
            original_expression=dimension.sql if dimension.sql else None,
        )

    def _convert_measure(
        self,
        measure: LookmlMeasure,
        view_name: str,
        column_mappings: Optional[dict[str, str]] = None,
    ) -> Optional[PbiMeasure]:
        """Convert a Looker measure to Power BI measure with tracking."""
        table_name = self._sanitize_name(view_name)
        measure_name = self._sanitize_name(measure.name)

        # Get measure type for tracking
        raw_measure_type = getattr(measure.type, "value", measure.type)
        measure_type = str(raw_measure_type).lower() if raw_measure_type else "count"

        # Measure already registered in tracker by _register_all_calculations().
        # Now convert and update tracker with DAX.
        result = self.expression_converter.convert_measure(
            measure,
            view_name,
            table_name,
            column_mappings=column_mappings,
        )

        if not result.dax_expression:
            log_warning(f"Could not convert measure: {measure.name}")
            if self.calculation_tracker:
                self.calculation_tracker.fail_conversion(
                    measure_name, table_name, "Could not generate DAX expression"
                )
            return None

        # Get format string
        format_string = self.expression_converter.get_format_string(measure)

        # Determine conversion method
        conversion_method = "AI" if getattr(result, 'used_api', False) else "rule-based"

        # Update tracker with DAX (matches Tableau's update_powerbi_calculation)
        if self.calculation_tracker:
            self.calculation_tracker.update_powerbi_calculation(
                table_name=table_name,
                calculation_name=measure_name,
                powerbi_name=measure_name,
                dax_expression=result.dax_expression,
                conversion_method=conversion_method,
                confidence=result.confidence,
                used_api=getattr(result, 'used_api', False),
                format_string=format_string,
                summarize_by="sum",
                warnings=result.warnings,
            )

        return PbiMeasure(
            name=measure_name,
            expression=result.dax_expression,
            format_string=format_string,
            description=measure.description or measure.label,
            display_folder=measure.group_label,
            is_hidden=measure.hidden,
            looker_name=measure.name,
            formula_looker=measure.sql,
            original_expression=measure.sql,
        )

    def _expand_dimension_columns(
        self,
        dimension: LookmlDimension,
        base_column: PbiColumn,
    ) -> list[tuple[PbiColumn, str]]:
        """
        Expand dimension_group timeframes to alias columns.

        Example:
        - Looker field `created` -> base column `Created`
        - Looker field `created_date` -> alias column `CreatedDate`
        """
        expanded: list[tuple[PbiColumn, str]] = [(base_column, dimension.name)]

        dim_type = getattr(dimension.type, "value", dimension.type)
        dim_type_text = str(dim_type).lower() if dim_type else ""
        if dim_type_text != "time" or not dimension.timeframes:
            return expanded

        base_col_ref = f"[{base_column.name}]"
        for timeframe in dimension.timeframes:
            timeframe_key = str(timeframe or "").strip().lower()
            if not timeframe_key:
                continue

            looker_field_name = f"{dimension.name}_{timeframe_key}"
            alias_column_name = self._sanitize_name(looker_field_name)
            if alias_column_name.lower() == base_column.name.lower():
                continue

            alias_expression, alias_data_type = self._timeframe_expression(
                timeframe_key=timeframe_key,
                base_column_ref=base_col_ref,
                base_data_type=base_column.data_type,
            )
            if not alias_expression:
                continue

            alias_column = PbiColumn(
                name=alias_column_name,
                data_type=alias_data_type,
                source_column=None,
                expression=alias_expression,
                is_calculated=True,
                is_hidden=base_column.is_hidden,
                format_string=base_column.format_string,
                summarize_by=base_column.summarize_by,
                description=base_column.description,
                display_folder=base_column.display_folder,
                looker_name=looker_field_name,
                formula_looker=base_column.formula_looker,
                original_expression=base_column.original_expression,
            )
            expanded.append((alias_column, looker_field_name))

        return expanded

    @staticmethod
    def _timeframe_expression(
        timeframe_key: str,
        base_column_ref: str,
        base_data_type: DataType,
    ) -> tuple[Optional[str], DataType]:
        """
        Build calculated-column expression for a Looker time timeframe.
        """
        if timeframe_key == "raw":
            return base_column_ref, base_data_type
        if timeframe_key == "time":
            return (
                f"TIME(HOUR({base_column_ref}), MINUTE({base_column_ref}), SECOND({base_column_ref}))",
                DataType.TIME,
            )
        if timeframe_key == "date":
            return (
                f"DATE(YEAR({base_column_ref}), MONTH({base_column_ref}), DAY({base_column_ref}))",
                DataType.DATE,
            )
        if timeframe_key == "week":
            return (f"({base_column_ref} - WEEKDAY({base_column_ref}, 2) + 1)", DataType.DATE)
        if timeframe_key == "month":
            return (f"DATE(YEAR({base_column_ref}), MONTH({base_column_ref}), 1)", DataType.DATE)
        if timeframe_key == "quarter":
            return (
                f"DATE(YEAR({base_column_ref}), ((INT((MONTH({base_column_ref}) - 1) / 3) * 3) + 1), 1)",
                DataType.DATE,
            )
        if timeframe_key == "year":
            return (f"YEAR({base_column_ref})", DataType.INT64)

        return None, base_data_type

    def _generate_partition(
        self,
        view: LookmlView,
        connection_type: str,
    ) -> Optional[PbiPartition]:
        """Generate partition for data loading."""
        # Handle derived tables
        if view.derived_table:
            sql = view.derived_table.get('sql')
            if sql:
                result = self.sql_converter.convert_derived_table(sql, connection_type)
                return PbiPartition(
                    name=f"{view.name}_Partition",
                    source_type="m",
                    expression=result.m_expression,
                )

        # Handle sql_table_name
        if view.sql_table_name:
            table_name = view.sql_table_name.strip('`"')

            m_expr = self.sql_converter.convert_simple_select(
                table_name=table_name,
                columns=['*'],
                connection_type=connection_type,
            )

            return PbiPartition(
                name=f"{view.name}_Partition",
                source_type="m",
                expression=m_expr,
            )

        # Placeholder partition
        return PbiPartition(
            name=f"{view.name}_Partition",
            source_type="m",
            expression=f'''let
    // TODO: Configure data source for {view.name}
    Source = #"Data Source",
    Table = Source{{[Name="{view.name}"]}}[Data]
in
    Table''',
        )

    @staticmethod
    def _resolve_source_column(dimension: LookmlDimension) -> str:
        """
        Determine the physical source column name for a dimension.

        Prefer `${TABLE}.column_name` references when available; otherwise
        fall back to the dimension name.
        """
        sql = (dimension.sql or "").strip()
        match = re.match(r'^\$\{TABLE\}\.([A-Za-z0-9_]+)$', sql)
        if match:
            return match.group(1)
        return dimension.name

    @staticmethod
    def _is_simple_table_reference(sql: str) -> bool:
        sql_text = (sql or "").strip()
        if not sql_text:
            return True
        return bool(re.match(r'^\$\{TABLE\}\.[A-Za-z0-9_]+$', sql_text))

    def _convert_format_name(self, format_name: str) -> str:
        """Convert Looker named format to Power BI format."""
        format_names = {
            'decimal_0': '0',
            'decimal_1': '0.0',
            'decimal_2': '0.00',
            'usd': '$#,##0.00',
            'usd_0': '$#,##0',
            'percent_0': '0%',
            'percent_1': '0.0%',
            'percent_2': '0.00%',
            'id': '0',
        }
        return format_names.get(format_name.lower(), '')

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Sanitize name for Power BI."""
        if not name:
            return "Unnamed"

        # Replace problematic characters
        result = name.strip()
        replacements = {
            '/': '_',
            '\\': '_',
            ':': '_',
            '*': '_',
            '?': '_',
            '"': '',
            '<': '',
            '>': '',
            '|': '_',
        }

        for old, new in replacements.items():
            result = result.replace(old, new)

        result = result.strip('_')

        # Convert snake_case/space-separated names to PascalCase.
        parts = [p for p in re.split(r'[_\s]+', result) if p]
        if len(parts) > 1:
            result = ''.join(p[:1].upper() + p[1:] for p in parts)
        elif result.islower():
            result = result[:1].upper() + result[1:]

        return result or "Unnamed"
