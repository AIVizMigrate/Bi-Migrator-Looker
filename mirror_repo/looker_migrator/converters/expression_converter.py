"""
Expression Converter for Looker to DAX conversion.

Converts Looker SQL expressions to DAX expressions.
Supports AI-powered conversion via the BI-Migrator-DAX-API when enabled.
"""

import re
from typing import Optional
from dataclasses import dataclass, field

from ..models import ConversionResult, LookmlMeasure, LookmlDimension
from ..common.log_utils import log_debug, log_warning, log_info
from .dax_api_client import DaxApiClient, DaxApiConfig, get_dax_api_client


@dataclass
class ConversionContext:
    """Context for expression conversion."""
    view_name: str
    table_name: str
    column_mappings: dict[str, str] = field(default_factory=dict)
    measure_mappings: dict[str, str] = field(default_factory=dict)


class ExpressionConverter:
    """
    Converts Looker expressions to DAX.

    Handles:
    - ${view.field} references
    - Looker templated filters
    - SQL to DAX function mapping
    - Measure type conversions
    """

    # Looker field reference pattern: ${view.field} or ${field}
    FIELD_REF_PATTERN = re.compile(
        r'\$\{(\w+)\.(\w+)\}|\$\{(\w+)\}'
    )

    # Looker liquid template pattern
    LIQUID_PATTERN = re.compile(
        r'\{%.*?%\}|\{\{.*?\}\}'
    )

    # SQL function mappings to DAX
    SQL_TO_DAX_FUNCTIONS = {
        # Aggregations
        'COUNT': 'COUNT',
        'COUNT_DISTINCT': 'DISTINCTCOUNT',
        'SUM': 'SUM',
        'AVG': 'AVERAGE',
        'MIN': 'MIN',
        'MAX': 'MAX',
        'MEDIAN': 'MEDIAN',

        # String functions
        'CONCAT': 'CONCATENATE',
        'LENGTH': 'LEN',
        'LOWER': 'LOWER',
        'UPPER': 'UPPER',
        'TRIM': 'TRIM',
        'SUBSTRING': 'MID',
        'REPLACE': 'SUBSTITUTE',
        'COALESCE': 'COALESCE',

        # Date functions
        'YEAR': 'YEAR',
        'MONTH': 'MONTH',
        'DAY': 'DAY',
        'QUARTER': 'QUARTER',
        'WEEK': 'WEEKNUM',
        'DATE_TRUNC': '_DATE_TRUNC',  # Needs special handling
        'DATE_ADD': 'DATEADD',
        'DATE_DIFF': 'DATEDIFF',
        'CURRENT_DATE': 'TODAY',
        'CURRENT_TIMESTAMP': 'NOW',

        # Logical
        'IF': 'IF',
        'CASE': 'SWITCH',
        'NULLIF': 'IF',
        'IFNULL': 'COALESCE',
        'NVL': 'COALESCE',

        # Math
        'ABS': 'ABS',
        'ROUND': 'ROUND',
        'FLOOR': 'FLOOR',
        'CEILING': 'CEILING',
        'POWER': 'POWER',
        'SQRT': 'SQRT',
        'MOD': 'MOD',
    }

    def __init__(
        self,
        config: Optional[dict] = None,
        dax_api_config: Optional[DaxApiConfig] = None,
    ):
        """
        Initialize the converter.

        Args:
            config: General configuration
            dax_api_config: Configuration for DAX API client
        """
        self.config = config or {}
        self._cache: dict[str, str] = {}
        self._api_client: Optional[DaxApiClient] = None
        self._conversion_stats = {
            "api_attempts": 0,
            "api_success": 0,
            "api_fallbacks": 0,
        }

        # Always try to initialize the DAX API client for AI-powered conversion
        if dax_api_config is not None:
            self._api_client = get_dax_api_client(dax_api_config)
            if self._api_client.is_available:
                log_info("DAX API client initialized and available")
            else:
                log_warning("DAX API not available, falling back to rule-based conversion")

    def reset_conversion_stats(self) -> None:
        """Reset API usage counters."""
        self._conversion_stats = {
            "api_attempts": 0,
            "api_success": 0,
            "api_fallbacks": 0,
        }

    def get_conversion_stats(self) -> dict[str, int]:
        """Get a snapshot of API usage counters."""
        return dict(self._conversion_stats)

    def convert_dimension(
        self,
        dimension: LookmlDimension,
        view_name: str,
        table_name: str,
    ) -> ConversionResult:
        """
        Convert a Looker dimension to DAX column expression.

        Args:
            dimension: Looker dimension
            view_name: Source view name
            table_name: Target Power BI table name

        Returns:
            ConversionResult with DAX expression
        """
        context = ConversionContext(
            view_name=view_name,
            table_name=table_name,
        )

        if not dimension.sql:
            # Simple column reference
            return ConversionResult(
                dax_expression=f"{table_name}[{dimension.name}]",
                confidence=1.0,
                original_expression=dimension.sql,
            )

        # Try API-based conversion first if enabled and available
        if self._api_client and self._api_client.is_available:
            try:
                self._conversion_stats["api_attempts"] += 1
                raw_dimension_type = getattr(dimension.type, "value", dimension.type)
                dimension_type = str(raw_dimension_type).lower() if raw_dimension_type else "string"
                api_response = self._api_client.convert_dimension(
                    dimension_name=dimension.name,
                    dimension_type=dimension_type,
                    sql_expression=dimension.sql,
                    view_name=view_name,
                    table_name=table_name,
                    column_mappings=context.column_mappings or None,
                )

                if api_response.success and api_response.dax_expression:
                    self._conversion_stats["api_success"] += 1
                    log_debug(f"API conversion succeeded for dimension: {dimension.name}")
                    return ConversionResult(
                        dax_expression=api_response.dax_expression,
                        confidence=api_response.confidence,
                        original_expression=dimension.sql,
                        warnings=api_response.warnings,
                        used_api=True,
                    )
                else:
                    self._conversion_stats["api_fallbacks"] += 1
                    log_debug(f"API conversion failed for dimension {dimension.name}, using fallback: {api_response.error}")
            except Exception as e:
                self._conversion_stats["api_fallbacks"] += 1
                log_warning(f"API error for dimension {dimension.name}: {e}, using fallback")

        return self.convert_expression(dimension.sql, context)

    def convert_measure(
        self,
        measure: LookmlMeasure,
        view_name: str,
        table_name: str,
        dependencies: Optional[list[dict]] = None,
        column_mappings: Optional[dict[str, str]] = None,
    ) -> ConversionResult:
        """
        Convert a Looker measure to DAX measure expression.

        Args:
            measure: Looker measure
            view_name: Source view name
            table_name: Target Power BI table name
            dependencies: Optional dependent measures already converted

        Returns:
            ConversionResult with DAX expression
        """
        context = ConversionContext(
            view_name=view_name,
            table_name=table_name,
            column_mappings=column_mappings or {},
        )

        raw_measure_type = getattr(measure.type, "value", measure.type)
        measure_type = str(raw_measure_type).lower() if raw_measure_type else "count"

        # Try API-based conversion first if enabled and available
        if self._api_client and self._api_client.is_available:
            try:
                self._conversion_stats["api_attempts"] += 1
                api_response = self._api_client.convert_measure(
                    measure_name=measure.name,
                    measure_type=measure_type,
                    sql_expression=measure.sql,
                    view_name=view_name,
                    table_name=table_name,
                    column_mappings=context.column_mappings or None,
                    dependencies=dependencies,
                    filters=measure.filters if hasattr(measure, 'filters') else None,
                )

                if api_response.success and api_response.dax_expression:
                    self._conversion_stats["api_success"] += 1
                    log_debug(f"API conversion succeeded for measure: {measure.name}")
                    return ConversionResult(
                        dax_expression=api_response.dax_expression,
                        confidence=api_response.confidence,
                        original_expression=measure.sql,
                        warnings=api_response.warnings,
                        used_api=True,
                    )
                else:
                    self._conversion_stats["api_fallbacks"] += 1
                    log_debug(f"API conversion failed for measure {measure.name}, using fallback: {api_response.error}")
            except Exception as e:
                self._conversion_stats["api_fallbacks"] += 1
                log_warning(f"API error for measure {measure.name}: {e}, using fallback")

        # Rule-based conversion (fallback or primary if API not enabled)
        # Handle different measure types
        if measure_type == "count":
            dax_expr = f"COUNTROWS({table_name})"
            dax_expr, filter_warnings = self._apply_measure_filters(
                dax_expr,
                measure,
                context,
            )
            return ConversionResult(
                dax_expression=dax_expr,
                confidence=1.0,
                original_expression=f"count(*)",
                warnings=filter_warnings,
            )

        elif measure_type == "count_distinct":
            if measure.sql:
                col_ref = self._convert_field_references(measure.sql, context)
                dax_expr = f"DISTINCTCOUNT({col_ref})"
                dax_expr, filter_warnings = self._apply_measure_filters(
                    dax_expr,
                    measure,
                    context,
                )
                return ConversionResult(
                    dax_expression=dax_expr,
                    confidence=0.9,
                    original_expression=measure.sql,
                    warnings=filter_warnings,
                )

        elif measure_type in ("sum", "average", "min", "max"):
            if measure.sql:
                col_ref = self._convert_field_references(measure.sql, context)
                dax_func = self.SQL_TO_DAX_FUNCTIONS.get(measure_type.upper(), measure_type.upper())
                dax_expr = f"{dax_func}({col_ref})"
                dax_expr, filter_warnings = self._apply_measure_filters(
                    dax_expr,
                    measure,
                    context,
                )
                return ConversionResult(
                    dax_expression=dax_expr,
                    confidence=0.9,
                    original_expression=measure.sql,
                    warnings=filter_warnings,
                )

        elif measure_type == "number":
            # Custom expression
            if measure.sql:
                converted = self.convert_expression(measure.sql, context)
                dax_expr, filter_warnings = self._apply_measure_filters(
                    converted.dax_expression,
                    measure,
                    context,
                )
                return ConversionResult(
                    dax_expression=dax_expr,
                    confidence=converted.confidence,
                    original_expression=measure.sql,
                    warnings=converted.warnings + filter_warnings,
                )

        elif measure_type == "yesno":
            if measure.sql:
                converted = self.convert_expression(measure.sql, context)
                # Wrap in IF for yes/no
                dax_expr = f'IF({converted.dax_expression}, "Yes", "No")'
                dax_expr, filter_warnings = self._apply_measure_filters(
                    dax_expr,
                    measure,
                    context,
                )
                return ConversionResult(
                    dax_expression=dax_expr,
                    confidence=converted.confidence * 0.9,
                    original_expression=measure.sql,
                    warnings=converted.warnings + filter_warnings,
                )

        # Fallback
        if measure.sql:
            converted = self.convert_expression(measure.sql, context)
            dax_expr, filter_warnings = self._apply_measure_filters(
                converted.dax_expression,
                measure,
                context,
            )
            return ConversionResult(
                dax_expression=dax_expr,
                confidence=converted.confidence,
                original_expression=measure.sql,
                warnings=converted.warnings + filter_warnings,
            )

        return ConversionResult(
            dax_expression=f"/* TODO: {measure.name} ({measure_type}) */",
            confidence=0.0,
            original_expression=measure.sql,
            warnings=[f"Could not convert measure type: {measure_type}"],
        )

    def convert_expression(
        self,
        expression: str,
        context: ConversionContext,
    ) -> ConversionResult:
        """
        Convert a Looker SQL expression to DAX.

        Args:
            expression: Looker SQL expression
            context: Conversion context

        Returns:
            ConversionResult with DAX expression
        """
        if not expression or not expression.strip():
            return ConversionResult(
                dax_expression="",
                confidence=1.0,
                original_expression=expression,
            )

        warnings = []
        confidence = 1.0

        # Check cache
        cache_key = f"{expression}::{context.view_name}::{context.table_name}"
        if cache_key in self._cache:
            return ConversionResult(
                dax_expression=self._cache[cache_key],
                confidence=1.0,
                original_expression=expression,
            )

        dax_expr = expression

        # Step 1: Remove Liquid templates (not supported in DAX)
        if self.LIQUID_PATTERN.search(dax_expr):
            dax_expr = self.LIQUID_PATTERN.sub('', dax_expr)
            warnings.append("Liquid templates removed - not supported in DAX")
            confidence *= 0.7

        # Step 2: Convert field references ${view.field} -> Table[Column]
        dax_expr = self._convert_field_references(dax_expr, context)

        # Step 3: Convert SQL functions to DAX
        dax_expr, func_warnings = self._convert_sql_functions(dax_expr)
        warnings.extend(func_warnings)

        # Step 4: Convert SQL syntax to DAX syntax
        dax_expr = self._convert_sql_syntax(dax_expr)

        # Step 5: Clean up
        dax_expr = self._cleanup_expression(dax_expr)

        # Adjust confidence
        if warnings:
            confidence = max(0.5, confidence - (len(warnings) * 0.1))

        # Cache result
        self._cache[cache_key] = dax_expr

        return ConversionResult(
            dax_expression=dax_expr,
            confidence=confidence,
            warnings=warnings,
            original_expression=expression,
        )

    def _apply_measure_filters(
        self,
        base_expression: str,
        measure: LookmlMeasure,
        context: ConversionContext,
    ) -> tuple[str, list[str]]:
        """
        Apply Looker measure filters to a base DAX expression using CALCULATE.
        """
        normalized_filters = self._normalize_measure_filters(measure.filters)
        if not normalized_filters:
            return base_expression, []

        predicates: list[str] = []
        warnings: list[str] = []

        for field_name, filter_value in normalized_filters:
            predicate = self._build_filter_predicate(
                field_name=field_name,
                filter_value=filter_value,
                context=context,
            )
            if predicate is None:
                warnings.append(
                    f"Skipped unsupported measure filter '{field_name}: {filter_value}' "
                    f"on measure '{measure.name}'"
                )
                continue
            predicates.append(predicate)

        if not predicates:
            return base_expression, warnings

        filtered_expression = f"CALCULATE({base_expression}, {', '.join(predicates)})"
        return filtered_expression, warnings

    def _normalize_measure_filters(
        self,
        filters: Optional[object],
    ) -> list[tuple[str, str]]:
        """Normalize filters input to a list of (field, value) pairs."""
        if not filters:
            return []

        normalized: list[tuple[str, str]] = []
        if isinstance(filters, dict):
            for key, value in filters.items():
                if key:
                    normalized.append((str(key).strip(), str(value).strip()))
            return normalized

        if isinstance(filters, list):
            for item in filters:
                if isinstance(item, dict):
                    for key, value in item.items():
                        if key:
                            normalized.append((str(key).strip(), str(value).strip()))
                else:
                    text = str(item or "").strip()
                    if ":" in text:
                        key, value = text.split(":", 1)
                        normalized.append((key.strip(), value.strip().strip('"').strip("'")))
            return normalized

        text = str(filters).strip()
        if ":" in text:
            key, value = text.split(":", 1)
            normalized.append((key.strip(), value.strip().strip('"').strip("'")))
        return normalized

    def _build_filter_predicate(
        self,
        field_name: str,
        filter_value: str,
        context: ConversionContext,
    ) -> Optional[str]:
        """Build a DAX predicate for one Looker measure filter."""
        normalized_field = str(field_name or "").strip()
        if not normalized_field:
            return None

        field_lookup_keys = (
            normalized_field.lower(),
            f"table.{normalized_field}".lower(),
            f"{context.view_name}.{normalized_field}".lower(),
        )

        column_ref = None
        for lookup_key in field_lookup_keys:
            if lookup_key in context.column_mappings:
                column_ref = context.column_mappings[lookup_key]
                break

        if not column_ref:
            return None

        normalized_value = str(filter_value or "").strip().strip('"').strip("'")
        upper_value = normalized_value.upper()
        lower_value = normalized_value.lower()

        if upper_value in {"NOT NULL", "NOT_NULL"}:
            return f"NOT(ISBLANK({column_ref}))"
        if upper_value in {"NULL", "IS NULL"}:
            return f"ISBLANK({column_ref})"
        if lower_value in {"yes", "true"}:
            return f"{column_ref} = TRUE()"
        if lower_value in {"no", "false"}:
            return f"{column_ref} = FALSE()"

        if "," in normalized_value:
            values = [v.strip() for v in normalized_value.split(",") if v.strip()]
            if not values:
                return None
            dax_values = ", ".join(self._to_dax_literal(v) for v in values)
            return f"{column_ref} IN {{{dax_values}}}"

        return f"{column_ref} = {self._to_dax_literal(normalized_value)}"

    @staticmethod
    def _to_dax_literal(value: str) -> str:
        """Convert a scalar text value to a DAX literal."""
        text = str(value or "").strip()
        if not text:
            return '""'

        if re.fullmatch(r"-?\d+(\.\d+)?", text):
            return text

        escaped = text.replace('"', '""')
        return f'"{escaped}"'

    def convert_field_reference(
        self,
        reference: str,
        view_mapping: Optional[dict[str, str]] = None,
        current_view: Optional[str] = None,
    ) -> str:
        """
        Convert a single Looker field reference to DAX table/column syntax.

        Example:
            ${orders.amount} -> 'Orders'[Amount]
        """
        if not reference:
            return ""

        view_mapping = view_mapping or {}
        ref = reference.strip()

        qualified_match = re.match(r'^\$\{(\w+)\.(\w+)\}$', ref)
        if qualified_match:
            view_name = qualified_match.group(1)
            field_name = qualified_match.group(2)
            table_name = view_mapping.get(view_name, view_name)
            return f"'{table_name}'[{self._to_pascal_case(field_name)}]"

        simple_match = re.match(r'^\$\{(\w+)\}$', ref)
        if simple_match:
            field_name = simple_match.group(1)
            if current_view and current_view in view_mapping:
                table_name = view_mapping[current_view]
            elif view_mapping:
                table_name = next(iter(view_mapping.values()))
            else:
                table_name = "Table"
            return f"'{table_name}'[{self._to_pascal_case(field_name)}]"

        # Fallback: return normalized expression conversion.
        context_view = current_view or (next(iter(view_mapping.keys())) if view_mapping else "table")
        context_table = view_mapping.get(context_view, context_view)
        converted = self._convert_field_references(
            ref,
            ConversionContext(view_name=context_view, table_name=context_table),
        )
        return converted

    def _convert_field_references(
        self,
        expression: str,
        context: ConversionContext,
    ) -> str:
        """Convert ${view.field} references to Table[Column] format."""
        # Handle ${TABLE}.field placeholder used heavily in LookML.
        table_field_pattern = re.compile(r'\$\{TABLE\}\.(\w+)', re.IGNORECASE)

        def replace_table_field(match: re.Match) -> str:
            field_name = match.group(1)
            mapping_key = f"table.{field_name}".lower()
            if mapping_key in context.column_mappings:
                return context.column_mappings[mapping_key]
            return f"{context.table_name}[{self._to_pascal_case(field_name)}]"

        expression = table_field_pattern.sub(replace_table_field, expression)

        def replace_ref(match: re.Match) -> str:
            if match.group(1) and match.group(2):
                # ${view.field} format
                view = match.group(1)
                field_name = match.group(2)

                # Check mappings
                key = f"{view}.{field_name}".lower()
                if key in context.column_mappings:
                    return context.column_mappings[key]

                # Use table name from context if view matches
                if view.lower() in {"table", context.view_name.lower()}:
                    return f"{context.table_name}[{self._to_pascal_case(field_name)}]"

                return f"{view}[{self._to_pascal_case(field_name)}]"

            elif match.group(3):
                # ${field} format - use current table
                field_name = match.group(3)
                key = field_name.lower()
                if key in context.column_mappings:
                    return context.column_mappings[key]
                return f"{context.table_name}[{self._to_pascal_case(field_name)}]"

            return match.group(0)

        return self.FIELD_REF_PATTERN.sub(replace_ref, expression)

    def _convert_sql_functions(
        self,
        expression: str,
    ) -> tuple[str, list[str]]:
        """Convert SQL functions to DAX functions."""
        warnings = []
        result = expression

        # Find function calls
        func_pattern = re.compile(r'\b([A-Z_]+)\s*\(', re.IGNORECASE)

        for match in func_pattern.finditer(expression):
            func_name = match.group(1).upper()

            if func_name in self.SQL_TO_DAX_FUNCTIONS:
                dax_func = self.SQL_TO_DAX_FUNCTIONS[func_name]

                if dax_func.startswith('_'):
                    # Needs special handling
                    warnings.append(f"Function {func_name} requires manual review")
                else:
                    result = re.sub(
                        rf'\b{func_name}\s*\(',
                        f'{dax_func}(',
                        result,
                        flags=re.IGNORECASE,
                    )

        return result, warnings

    def _convert_sql_syntax(self, expression: str) -> str:
        """Convert SQL syntax to DAX syntax."""
        result = expression

        # Convert string concatenation || to &
        result = re.sub(r'\|\|', ' & ', result)

        # Convert != to <>
        result = re.sub(r'!=', '<>', result)

        # Convert CASE WHEN to SWITCH/IF pattern
        result = self._convert_case_statement(result)

        # Convert IS NULL / IS NOT NULL
        result = re.sub(r'\bIS\s+NULL\b', '= BLANK()', result, flags=re.IGNORECASE)
        result = re.sub(r'\bIS\s+NOT\s+NULL\b', '<> BLANK()', result, flags=re.IGNORECASE)

        # Convert LIKE to SEARCH pattern
        result = self._convert_like_pattern(result)

        return result

    def _convert_case_statement(self, expression: str) -> str:
        """Convert SQL CASE statements to DAX IF/SWITCH."""
        # Simple CASE WHEN ... THEN ... ELSE ... END
        case_pattern = re.compile(
            r'\bCASE\s+WHEN\s+(.+?)\s+THEN\s+(.+?)\s+ELSE\s+(.+?)\s+END\b',
            re.IGNORECASE | re.DOTALL
        )

        def replace_case(match: re.Match) -> str:
            condition = match.group(1).strip()
            then_value = match.group(2).strip()
            else_value = match.group(3).strip()
            return f'IF({condition}, {then_value}, {else_value})'

        return case_pattern.sub(replace_case, expression)

    def _convert_like_pattern(self, expression: str) -> str:
        """Convert SQL LIKE to DAX SEARCH pattern."""
        like_pattern = re.compile(
            r"(.+?)\s+LIKE\s+'([^']+)'",
            re.IGNORECASE
        )

        def replace_like(match: re.Match) -> str:
            column = match.group(1).strip()
            pattern = match.group(2)

            # Convert % wildcards
            search_pattern = pattern.replace('%', '*')

            return f'SEARCH("{search_pattern}", {column}, 1, 0) > 0'

        return like_pattern.sub(replace_like, expression)

    def _cleanup_expression(self, expression: str) -> str:
        """Clean up the converted expression."""
        # Remove extra whitespace
        result = re.sub(r'\s+', ' ', expression)
        result = result.strip()

        return result

    @staticmethod
    def _to_pascal_case(value: str) -> str:
        """Convert snake_case field names to PascalCase for DAX-style identifiers."""
        if not value:
            return value
        parts = [p for p in re.split(r'[_\s]+', value) if p]
        if not parts:
            return value
        return ''.join(p[:1].upper() + p[1:] for p in parts)

    def get_format_string(self, measure: LookmlMeasure) -> str:
        """Get Power BI format string from Looker format."""
        if measure.value_format:
            return self._convert_format(measure.value_format)

        if measure.value_format_name:
            return self._convert_format_name(measure.value_format_name)

        # Default based on type
        raw_measure_type = getattr(measure.type, "value", measure.type)
        measure_type = str(raw_measure_type).lower() if raw_measure_type else "number"
        if measure_type in ("count", "count_distinct"):
            return "0"
        elif measure_type in ("sum", "average"):
            return "#,##0.00"
        elif measure_type == "percent_of_total":
            return "0.00%"

        return "#,##0.00"

    def _convert_format(self, looker_format: str) -> str:
        """Convert Looker format string to Power BI format."""
        # Common Looker formats
        format_map = {
            '"$"#,##0': '$#,##0',
            '"$"#,##0.00': '$#,##0.00',
            '#,##0': '#,##0',
            '#,##0.00': '#,##0.00',
            '0.00%': '0.00%',
            '0%': '0%',
            '#,##0.0': '#,##0.0',
        }

        return format_map.get(looker_format, looker_format)

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

        return format_names.get(format_name.lower(), '#,##0.00')
