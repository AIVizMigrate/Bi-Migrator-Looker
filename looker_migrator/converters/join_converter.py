"""
Join Converter for Looker to Power BI conversion.

Converts Looker explore joins to Power BI relationships.
"""

import re
from typing import Optional

from ..models import (
    LookmlJoin,
    PbiRelationship,
    Cardinality,
    CrossFilterDirection,
)
from ..common.log_utils import log_debug, log_warning


class JoinConverter:
    """Converts Looker joins to Power BI relationships."""

    # Relationship mapping
    RELATIONSHIP_MAP: dict[str, Cardinality] = {
        'many_to_one': Cardinality.MANY_TO_ONE,
        'one_to_many': Cardinality.ONE_TO_MANY,
        'one_to_one': Cardinality.ONE_TO_ONE,
        'many_to_many': Cardinality.MANY_TO_MANY,
    }

    def convert(
        self,
        join: LookmlJoin,
        base_view: Optional[str] = None,
        view_to_table: Optional[dict[str, str]] = None,
        primary_keys_by_view: Optional[dict[str, set[str]]] = None,
        *,
        from_table: Optional[str] = None,
        view_mapping: Optional[dict[str, str]] = None,
    ) -> Optional[PbiRelationship]:
        """
        Convert a Looker join to a Power BI relationship.

        Args:
            join: Looker join definition
            base_view: Base view of the explore
            view_to_table: Mapping of view names to table names
            from_table: Backward-compatible alias for base_view as table name
            view_mapping: Backward-compatible alias for view_to_table

        Returns:
            Power BI relationship definition, or None if conversion fails
        """
        table_mapping = view_to_table or view_mapping or {}

        # Backward-compatible call pattern used by tests:
        # convert(join, from_table="Orders", view_mapping={"orders": "Orders", ...})
        if not base_view and from_table:
            inverse_mapping = {v: k for k, v in table_mapping.items()}
            base_view = inverse_mapping.get(from_table, from_table)

        if not base_view:
            base_view = join.from_view or join.name

        # Parse sql_on to extract columns
        if not join.sql_on and not join.sql_foreign_key:
            log_warning(f"Join {join.name} has no sql_on or sql_foreign_key")
            return None

        # Determine from/to views from sql_on. Keep base_view as relationship source.
        joined_view = join.from_view or join.name
        from_view_default = base_view
        to_view_default = joined_view

        # Extract columns and actual views from sql_on (as written)
        left_view, left_column, right_view, right_column = self._parse_sql_on(
            join.sql_on, from_view_default, to_view_default
        )

        if not left_column or not right_column:
            # Try sql_foreign_key
            if join.sql_foreign_key:
                from_column = self._extract_column_from_ref(join.sql_foreign_key)
                to_column = from_column  # Assumes same column name
                from_view = from_view_default
                to_view = to_view_default
            else:
                log_warning(f"Could not extract columns from join {join.name}")
                return None
        else:
            joined_view_lower = joined_view.lower()
            left_view_lower = (left_view or "").lower()
            right_view_lower = (right_view or "").lower()

            # Relationship direction should be base-side/foreign-key-side -> joined side.
            if right_view_lower == joined_view_lower and left_view_lower != joined_view_lower:
                from_view, from_column = left_view, left_column
                to_view, to_column = right_view, right_column
            elif left_view_lower == joined_view_lower and right_view_lower != joined_view_lower:
                from_view, from_column = right_view, right_column
                to_view, to_column = left_view, left_column
            elif left_view_lower == (base_view or "").lower():
                from_view, from_column = left_view, left_column
                to_view, to_column = right_view, right_column
            elif right_view_lower == (base_view or "").lower():
                from_view, from_column = right_view, right_column
                to_view, to_column = left_view, left_column
            else:
                # Fallback to expression order.
                from_view, from_column = left_view, left_column
                to_view, to_column = right_view, right_column

        from_table_name = table_mapping.get(from_view, from_view)
        to_table_name = table_mapping.get(to_view, to_view)

        # Map cardinality
        cardinality = self.RELATIONSHIP_MAP.get(
            join.relationship.lower() if join.relationship else 'many_to_one',
            Cardinality.MANY_TO_ONE,
        )
        cardinality = self._normalize_cardinality_against_keys(
            cardinality=cardinality,
            from_view=from_view,
            from_column=from_column,
            to_view=to_view,
            to_column=to_column,
            primary_keys_by_view=primary_keys_by_view or {},
            join_name=join.name,
        )

        # Determine cross-filter direction
        cross_filter = self._determine_cross_filter(join.type, cardinality)

        return PbiRelationship(
            name=f"{from_table_name}_{to_table_name}",
            from_table=from_table_name,
            from_column=from_column,
            to_table=to_table_name,
            to_column=to_column,
            cardinality=cardinality,
            cross_filter_direction=cross_filter,
            is_active=True,
        )

    def _parse_sql_on(
        self,
        sql_on: Optional[str],
        from_view: str,
        to_view: str,
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Parse sql_on to extract view and column references.

        Example: ${orders.user_id} = ${users.id}

        Returns:
            Tuple of (from_view, from_column, to_view, to_column)
        """
        if not sql_on:
            return None, None, None, None

        # Pattern for ${view.field} = ${view.field}
        pattern = re.compile(
            r'\$\{(\w+)\.(\w+)\}\s*=\s*\$\{(\w+)\.(\w+)\}'
        )

        match = pattern.search(sql_on)
        if match:
            left_view = match.group(1)
            left_col = match.group(2)
            right_view = match.group(3)
            right_col = match.group(4)

            return left_view, left_col, right_view, right_col

        # Try simpler pattern without view prefix
        simple_pattern = re.compile(r'\$\{(\w+)\}\s*=\s*\$\{(\w+)\}')
        match = simple_pattern.search(sql_on)
        if match:
            return from_view, match.group(1), to_view, match.group(2)

        return None, None, None, None

    def _extract_column_from_ref(self, ref: str) -> Optional[str]:
        """Extract column name from ${view.column} reference."""
        pattern = re.compile(r'\$\{(?:\w+\.)?(\w+)\}')
        match = pattern.search(ref)
        if match:
            return match.group(1)
        return None

    def _determine_cross_filter(
        self,
        join_type: Optional[str],
        cardinality: Cardinality,
    ) -> CrossFilterDirection:
        """Determine cross-filter direction based on join type."""
        if not join_type:
            return CrossFilterDirection.SINGLE

        join_lower = join_type.lower()

        # Full outer joins typically need bidirectional
        if join_lower == 'full_outer':
            return CrossFilterDirection.BOTH

        # Many-to-many requires special handling
        if cardinality == Cardinality.MANY_TO_MANY:
            return CrossFilterDirection.BOTH

        return CrossFilterDirection.SINGLE

    def _normalize_cardinality_against_keys(
        self,
        cardinality: Cardinality,
        from_view: str,
        from_column: str,
        to_view: str,
        to_column: str,
        primary_keys_by_view: dict[str, set[str]],
        join_name: str,
    ) -> Cardinality:
        """
        Downgrade cardinality when LookML doesn't declare primary keys on the one side.

        Power BI enforces uniqueness on the one-side of relationships; if that side is
        not an explicit LookML primary key, prefer many-to-many to avoid refresh failures.
        """
        pk_map = {
            str(view).lower(): {str(col).lower() for col in cols}
            for view, cols in (primary_keys_by_view or {}).items()
        }
        if not pk_map:
            return cardinality

        from_view_key = str(from_view).lower()
        to_view_key = str(to_view).lower()
        if from_view_key not in pk_map or to_view_key not in pk_map:
            # No reliable key metadata for at least one side; keep declared cardinality.
            return cardinality

        from_is_pk = str(from_column).lower() in pk_map.get(str(from_view).lower(), set())
        to_is_pk = str(to_column).lower() in pk_map.get(str(to_view).lower(), set())

        if cardinality == Cardinality.MANY_TO_ONE and not to_is_pk:
            log_warning(
                f"Join '{join_name}' expected one-side key on {to_view}.{to_column} "
                "but no LookML primary_key was found; using many_to_many."
            )
            return Cardinality.MANY_TO_MANY

        if cardinality == Cardinality.ONE_TO_MANY and not from_is_pk:
            log_warning(
                f"Join '{join_name}' expected one-side key on {from_view}.{from_column} "
                "but no LookML primary_key was found; using many_to_many."
            )
            return Cardinality.MANY_TO_MANY

        if cardinality == Cardinality.ONE_TO_ONE and (not from_is_pk or not to_is_pk):
            log_warning(
                f"Join '{join_name}' expected one_to_one keys on both sides but at least one "
                "LookML primary_key is missing; using many_to_many."
            )
            return Cardinality.MANY_TO_MANY

        return cardinality

    def convert_all(
        self,
        joins: list[LookmlJoin],
        base_view: str,
        view_to_table: dict[str, str] = None,
        primary_keys_by_view: Optional[dict[str, set[str]]] = None,
    ) -> list[PbiRelationship]:
        """
        Convert multiple joins to relationships.

        Args:
            joins: List of Looker joins
            base_view: Base view of the explore
            view_to_table: Mapping of view names to table names

        Returns:
            List of Power BI relationships
        """
        relationships = []
        seen_pairs = set()

        for join in joins:
            rel = self.convert(
                join,
                base_view,
                view_to_table,
                primary_keys_by_view=primary_keys_by_view,
            )
            if rel:
                # Check for duplicates
                pair_key = tuple(sorted([
                    f"{rel.from_table}.{rel.from_column}",
                    f"{rel.to_table}.{rel.to_column}",
                ]))

                if pair_key not in seen_pairs:
                    relationships.append(rel)
                    seen_pairs.add(pair_key)
                else:
                    log_debug(f"Skipping duplicate relationship: {rel.name}")

        return relationships
