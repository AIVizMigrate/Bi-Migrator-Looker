"""
Relationship Validator for Looker Migrator.

Validates Power BI model relationships converted from Looker explore joins.
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class RelationshipSeverity(Enum):
    """Severity levels for relationship validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class RelationshipIssue:
    """Represents a relationship validation issue."""
    severity: RelationshipSeverity
    message: str
    relationship_name: Optional[str] = None
    suggestion: Optional[str] = None


@dataclass
class RelationshipValidationResult:
    """Result of relationship validation."""
    is_valid: bool
    total_relationships: int
    issues: list[RelationshipIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == RelationshipSeverity.ERROR for i in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == RelationshipSeverity.ERROR)

    @property
    def has_warnings(self) -> bool:
        return any(i.severity == RelationshipSeverity.WARNING for i in self.issues)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == RelationshipSeverity.WARNING)


@dataclass
class RelationshipInfo:
    """Information about a relationship for validation."""
    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: str  # one_to_one, one_to_many, many_to_one, many_to_many
    cross_filter: str  # single, both
    is_active: bool = True


class RelationshipValidator:
    """Validates Power BI model relationships."""

    # Valid cardinality values
    VALID_CARDINALITIES = {
        "one_to_one",
        "one_to_many",
        "many_to_one",
        "many_to_many",
    }

    # Valid cross-filter directions
    VALID_CROSS_FILTERS = {
        "single",
        "both",
        "singleDirection",  # TMDL format
        "bothDirections",   # TMDL format
    }

    def __init__(self, tables: Optional[list[str]] = None):
        """
        Initialize relationship validator.

        Args:
            tables: List of valid table names in the model
        """
        self.tables = set(tables) if tables else set()

    def validate(
        self,
        relationships: list[RelationshipInfo],
    ) -> RelationshipValidationResult:
        """
        Validate a list of relationships.

        Args:
            relationships: List of RelationshipInfo objects

        Returns:
            RelationshipValidationResult
        """
        issues: list[RelationshipIssue] = []

        if not relationships:
            return RelationshipValidationResult(
                is_valid=True,
                total_relationships=0,
                issues=[RelationshipIssue(
                    severity=RelationshipSeverity.INFO,
                    message="No relationships to validate",
                )],
            )

        # Track for duplicate detection
        relationship_keys = set()
        active_paths = {}  # Track active relationship paths

        for rel in relationships:
            rel_issues = self._validate_single_relationship(rel)
            issues.extend(rel_issues)

            # Check for duplicates
            key = self._relationship_key(rel)
            if key in relationship_keys:
                issues.append(RelationshipIssue(
                    severity=RelationshipSeverity.ERROR,
                    message=f"Duplicate relationship: {rel.from_table}[{rel.from_column}] -> {rel.to_table}[{rel.to_column}]",
                    relationship_name=rel.name,
                ))
            relationship_keys.add(key)

            # Track active paths for ambiguity detection
            if rel.is_active:
                path_key = (rel.from_table, rel.to_table)
                if path_key in active_paths:
                    issues.append(RelationshipIssue(
                        severity=RelationshipSeverity.WARNING,
                        message=f"Multiple active relationships between {rel.from_table} and {rel.to_table}",
                        relationship_name=rel.name,
                        suggestion="Consider deactivating one relationship and using USERELATIONSHIP()",
                    ))
                active_paths[path_key] = rel.name

        # Check for circular relationships
        circular_issues = self._check_circular_relationships(relationships)
        issues.extend(circular_issues)

        # Check for many-to-many ambiguity
        m2m_issues = self._check_many_to_many(relationships)
        issues.extend(m2m_issues)

        is_valid = not any(i.severity == RelationshipSeverity.ERROR for i in issues)

        return RelationshipValidationResult(
            is_valid=is_valid,
            total_relationships=len(relationships),
            issues=issues,
        )

    def _validate_single_relationship(
        self,
        rel: RelationshipInfo,
    ) -> list[RelationshipIssue]:
        """Validate a single relationship."""
        issues = []

        # Check for empty values
        if not rel.from_table:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message="Missing from_table",
                relationship_name=rel.name,
            ))

        if not rel.from_column:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message="Missing from_column",
                relationship_name=rel.name,
            ))

        if not rel.to_table:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message="Missing to_table",
                relationship_name=rel.name,
            ))

        if not rel.to_column:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message="Missing to_column",
                relationship_name=rel.name,
            ))

        # Check self-referencing
        if rel.from_table == rel.to_table:
            if rel.from_column == rel.to_column:
                issues.append(RelationshipIssue(
                    severity=RelationshipSeverity.ERROR,
                    message="Relationship references same column on same table",
                    relationship_name=rel.name,
                ))
            else:
                issues.append(RelationshipIssue(
                    severity=RelationshipSeverity.INFO,
                    message="Self-referencing (hierarchical) relationship detected",
                    relationship_name=rel.name,
                ))

        # Check cardinality
        if rel.cardinality and rel.cardinality not in self.VALID_CARDINALITIES:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message=f"Invalid cardinality: {rel.cardinality}",
                relationship_name=rel.name,
                suggestion=f"Valid values: {', '.join(self.VALID_CARDINALITIES)}",
            ))

        # Check cross-filter direction
        if rel.cross_filter and rel.cross_filter not in self.VALID_CROSS_FILTERS:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.ERROR,
                message=f"Invalid cross-filter direction: {rel.cross_filter}",
                relationship_name=rel.name,
                suggestion=f"Valid values: {', '.join(self.VALID_CROSS_FILTERS)}",
            ))

        # Check bidirectional filter warnings
        if rel.cross_filter in ("both", "bothDirections"):
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.WARNING,
                message="Bidirectional cross-filtering may cause performance issues",
                relationship_name=rel.name,
                suggestion="Consider single-direction filtering unless bidirectional is required",
            ))

        # Check if tables exist (if we have the list)
        if self.tables:
            if rel.from_table and rel.from_table not in self.tables:
                issues.append(RelationshipIssue(
                    severity=RelationshipSeverity.ERROR,
                    message=f"From table '{rel.from_table}' not found in model",
                    relationship_name=rel.name,
                ))
            if rel.to_table and rel.to_table not in self.tables:
                issues.append(RelationshipIssue(
                    severity=RelationshipSeverity.ERROR,
                    message=f"To table '{rel.to_table}' not found in model",
                    relationship_name=rel.name,
                ))

        return issues

    def _relationship_key(self, rel: RelationshipInfo) -> tuple:
        """Create a unique key for a relationship."""
        return (rel.from_table, rel.from_column, rel.to_table, rel.to_column)

    def _check_circular_relationships(
        self,
        relationships: list[RelationshipInfo],
    ) -> list[RelationshipIssue]:
        """Check for circular relationship paths (simplified check)."""
        issues = []

        # Build adjacency graph
        graph: dict[str, set[str]] = {}
        for rel in relationships:
            if not rel.is_active:
                continue
            if rel.from_table not in graph:
                graph[rel.from_table] = set()
            graph[rel.from_table].add(rel.to_table)

        # Simple cycle detection using DFS
        def has_cycle(start: str, visited: set, path: set) -> bool:
            visited.add(start)
            path.add(start)

            for neighbor in graph.get(start, set()):
                if neighbor in path:
                    return True
                if neighbor not in visited:
                    if has_cycle(neighbor, visited, path):
                        return True

            path.remove(start)
            return False

        visited: set[str] = set()
        for node in graph:
            if node not in visited:
                if has_cycle(node, visited, set()):
                    issues.append(RelationshipIssue(
                        severity=RelationshipSeverity.WARNING,
                        message="Circular relationship path detected in model",
                        suggestion="Power BI may not handle circular relationships correctly. Consider using inactive relationships.",
                    ))
                    break  # Only report once

        return issues

    def _check_many_to_many(
        self,
        relationships: list[RelationshipInfo],
    ) -> list[RelationshipIssue]:
        """Check for many-to-many relationship warnings."""
        issues = []

        m2m_count = sum(1 for r in relationships if r.cardinality == "many_to_many")

        if m2m_count > 0:
            issues.append(RelationshipIssue(
                severity=RelationshipSeverity.WARNING,
                message=f"Model contains {m2m_count} many-to-many relationship(s)",
                suggestion="Many-to-many relationships may require special handling with TREATAS or bridge tables",
            ))

        return issues


def validate_relationships(
    relationships: list[dict],
    tables: Optional[list[str]] = None,
) -> RelationshipValidationResult:
    """
    Convenience function to validate relationships from dictionaries.

    Args:
        relationships: List of relationship dictionaries
        tables: Optional list of valid table names

    Returns:
        RelationshipValidationResult
    """
    rel_infos = []
    for i, rel in enumerate(relationships):
        rel_infos.append(RelationshipInfo(
            name=rel.get("name", f"Relationship_{i+1}"),
            from_table=rel.get("from_table", ""),
            from_column=rel.get("from_column", ""),
            to_table=rel.get("to_table", ""),
            to_column=rel.get("to_column", ""),
            cardinality=rel.get("cardinality", "one_to_many"),
            cross_filter=rel.get("cross_filter", "single"),
            is_active=rel.get("is_active", True),
        ))

    validator = RelationshipValidator(tables=tables)
    return validator.validate(rel_infos)
