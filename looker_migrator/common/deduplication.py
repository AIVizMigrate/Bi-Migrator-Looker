"""
Deduplication utilities for Power BI table elements.

Reusable module for handling name conflicts between columns and measures.
"""

import re
from typing import List, Dict, Set, Optional, Tuple, Any
from dataclasses import dataclass

from .log_utils import log_info, log_debug, log_warning, logger


@dataclass
class DeduplicationResult:
    """Result of a deduplication operation."""
    original_name: str
    final_name: str
    was_renamed: bool
    was_skipped: bool
    skip_reason: Optional[str] = None


class TableDeduplicator:
    """
    Handles deduplication of columns and measures within a Power BI table.

    This class provides methods to:
    1. Detect name conflicts between measures and columns
    2. Rename conflicting measures with appropriate prefixes
    3. Skip simple aggregation measures that duplicate column functionality
    """

    # Aggregation function patterns for detecting simple measures
    SIMPLE_AGGREGATION_PATTERN = re.compile(
        r'^\s*(SUM|COUNT|AVERAGE|AVG|MIN|MAX|DISTINCTCOUNT)\s*\(\s*'
        r"(?:\w+\[)?(\w+)\]?\s*\)\s*$",
        re.IGNORECASE
    )

    # Prefix mappings for aggregation-based renaming
    AGGREGATION_PREFIXES = {
        'sum': 'Total',
        'count': 'Count of',
        'average': 'Avg',
        'avg': 'Avg',
        'min': 'Min',
        'max': 'Max',
        'distinctcount': 'Distinct',
    }

    def __init__(self, skip_simple_aggregations: bool = True):
        """
        Initialize the deduplicator.

        Args:
            skip_simple_aggregations: If True, skip measures that are simple
                aggregations of columns with the same name (e.g., SUM(Table[Freight])
                for a column named "Freight")
        """
        self.skip_simple_aggregations = skip_simple_aggregations

    def deduplicate_table_elements(
        self,
        columns: List[Any],
        measures: List[Any],
        column_name_attr: str = 'name',
        measure_name_attr: str = 'name',
        measure_expression_attr: str = 'expression',
    ) -> Tuple[List[Any], List[DeduplicationResult]]:
        """
        Deduplicate measures against columns in a table.

        Args:
            columns: List of column objects
            measures: List of measure objects
            column_name_attr: Attribute name to get column name
            measure_name_attr: Attribute name to get/set measure name
            measure_expression_attr: Attribute name to get measure expression

        Returns:
            Tuple of (updated measures list, deduplication results)
        """
        # Build column name set (lowercase for comparison)
        column_names = self._build_name_set(columns, column_name_attr)

        # Track seen measure names to handle measure-to-measure duplicates
        seen_measure_names: Set[str] = set()

        # Process measures
        updated_measures = []
        results = []

        for measure in measures:
            measure_name = getattr(measure, measure_name_attr, None)
            if not measure_name:
                continue

            measure_key = measure_name.lower()
            expression = getattr(measure, measure_expression_attr, '') or ''

            result = DeduplicationResult(
                original_name=measure_name,
                final_name=measure_name,
                was_renamed=False,
                was_skipped=False,
            )

            # Check for measure-to-measure duplicate
            if measure_key in seen_measure_names:
                result.was_skipped = True
                result.skip_reason = 'Duplicate measure name'
                results.append(result)
                log_debug(f"Skipping duplicate measure: {measure_name}")
                continue

            # Check for column name conflict
            if measure_key in column_names:
                # Check if it's a simple aggregation
                if self.skip_simple_aggregations and self._is_simple_aggregation(
                    expression, measure_name
                ):
                    result.was_skipped = True
                    result.skip_reason = 'Simple aggregation of same-named column'
                    results.append(result)
                    log_debug(
                        f"Skipping simple aggregation measure '{measure_name}' "
                        f"(column with same name exists)"
                    )
                    continue

                # Rename the measure
                new_name = self._generate_unique_name(
                    measure_name, expression, column_names, seen_measure_names
                )
                setattr(measure, measure_name_attr, new_name)
                result.final_name = new_name
                result.was_renamed = True
                log_info(f"Renamed measure '{measure_name}' to '{new_name}' (column conflict)")

            # Add to tracking sets
            seen_measure_names.add(result.final_name.lower())
            updated_measures.append(measure)
            results.append(result)

        return updated_measures, results

    def _build_name_set(self, items: List[Any], name_attr: str) -> Set[str]:
        """Build a set of lowercase names from a list of objects."""
        names = set()
        for item in items:
            name = getattr(item, name_attr, None)
            if name:
                names.add(name.lower())
        return names

    def _is_simple_aggregation(self, expression: str, measure_name: str) -> bool:
        """
        Check if a measure is a simple aggregation of a column with the same name.

        Examples:
            - SUM(Table[Freight]) where measure is named "Freight" -> True
            - SUM(Table[Freight]) + 100 -> False
            - CALCULATE(SUM(Table[Freight]), ...) -> False
        """
        if not expression:
            return False

        match = self.SIMPLE_AGGREGATION_PATTERN.match(expression.strip())
        if match:
            column_name = match.group(2)
            # Check if the column name matches the measure name
            if column_name.lower() == measure_name.lower():
                return True

        return False

    def _generate_unique_name(
        self,
        original_name: str,
        expression: str,
        column_names: Set[str],
        measure_names: Set[str],
    ) -> str:
        """
        Generate a unique name for a measure that conflicts with a column.

        Strategy:
        1. Try aggregation-based prefix (Total, Avg, etc.)
        2. Fall back to "_Measure" suffix
        3. Add numeric suffix if still conflicting
        """
        # Try to detect aggregation type for appropriate prefix
        prefix = self._detect_aggregation_prefix(expression)

        if prefix:
            new_name = f"{prefix} {original_name}"
        else:
            new_name = f"{original_name} Measure"

        # Check if new name is unique
        all_names = column_names | measure_names
        if new_name.lower() not in all_names:
            return new_name

        # Add numeric suffix
        counter = 2
        base_name = new_name
        while new_name.lower() in all_names:
            new_name = f"{base_name} {counter}"
            counter += 1

        return new_name

    def _detect_aggregation_prefix(self, expression: str) -> Optional[str]:
        """Detect the appropriate prefix based on the aggregation function used."""
        if not expression:
            return None

        expression_upper = expression.upper().strip()

        for func, prefix in self.AGGREGATION_PREFIXES.items():
            if expression_upper.startswith(func.upper() + '('):
                return prefix

        return None


def deduplicate_measures_for_table(
    columns: List[Any],
    measures: List[Any],
    column_name_attr: str = 'name',
    measure_name_attr: str = 'name',
    measure_expression_attr: str = 'expression',
    skip_simple_aggregations: bool = True,
) -> Tuple[List[Any], List[DeduplicationResult]]:
    """
    Convenience function to deduplicate measures for a table.

    Args:
        columns: List of column objects
        measures: List of measure objects
        column_name_attr: Attribute name to get column name
        measure_name_attr: Attribute name to get/set measure name
        measure_expression_attr: Attribute name to get measure expression
        skip_simple_aggregations: Skip simple aggregations of same-named columns

    Returns:
        Tuple of (updated measures list, deduplication results)
    """
    deduplicator = TableDeduplicator(skip_simple_aggregations=skip_simple_aggregations)
    return deduplicator.deduplicate_table_elements(
        columns=columns,
        measures=measures,
        column_name_attr=column_name_attr,
        measure_name_attr=measure_name_attr,
        measure_expression_attr=measure_expression_attr,
    )
