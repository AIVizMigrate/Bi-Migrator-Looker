"""
TMDL File Validator for Looker Migrator.

Validates TMDL (Tabular Model Definition Language) output files.
"""

import re
import shutil
import tempfile
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class TMDLSeverity(Enum):
    """Severity levels for TMDL validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class TMDLIssue:
    """Represents a TMDL validation issue."""
    severity: TMDLSeverity
    message: str
    file_path: Optional[str] = None
    line_number: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class TMDLValidationResult:
    """Result of TMDL validation."""
    is_valid: bool
    files_checked: int
    issues: list[TMDLIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == TMDLSeverity.ERROR for i in self.issues)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == TMDLSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == TMDLSeverity.WARNING)


class TMDLValidator:
    """Validates TMDL files for structure and syntax."""

    # Accepted required file layouts for a valid TMDL model.
    # Tableau-standard layout is first (output/pbit/Model/*).
    REQUIRED_LAYOUTS = (
        ("pbit/Model/model.tmdl", "pbit/Model/database.tmdl"),
        ("model.tmdl", "definition/database.tmdl"),  # legacy compatibility
    )

    # Valid TMDL keywords
    TMDL_KEYWORDS = {
        "model", "table", "column", "measure", "partition",
        "relationship", "annotation", "expression", "role",
        "dataType", "lineageTag", "isHidden", "formatString",
        "description", "displayFolder", "summarizeBy",
        "sourceColumn", "fromColumn", "toColumn",
        "fromCardinality", "toCardinality", "crossFilteringBehavior",
        "isActive", "mode", "culture", "compatibilityLevel",
    }

    # Valid data types
    VALID_DATA_TYPES = {
        "string", "int64", "double", "dateTime", "datetime", "decimal",
        "boolean", "binary", "variant", "automatic",
    }

    # Patterns
    TABLE_PATTERN = re.compile(r"^table\s+(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))", re.MULTILINE)
    COLUMN_PATTERN = re.compile(r"^\s+column\s+'([^']+)'", re.MULTILINE)
    MEASURE_PATTERN = re.compile(r"^\s+measure\s+'([^']+)'", re.MULTILINE)
    DATATYPE_PATTERN = re.compile(r"dataType:\s*(\w+)")
    EXPRESSION_BLOCK = re.compile(r'expression:\s*```([^`]*)```', re.DOTALL)
    LINEAGE_TAG_PATTERN = re.compile(r'lineageTag:\s*([a-f0-9-]+)')

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize TMDL validator.

        Args:
            output_dir: Directory containing TMDL files
        """
        self.output_dir = Path(output_dir) if output_dir else None

    def validate_directory(self, directory: Optional[str] = None) -> TMDLValidationResult:
        """
        Validate all TMDL files in a directory.

        Args:
            directory: Path to TMDL output directory

        Returns:
            TMDLValidationResult
        """
        issues: list[TMDLIssue] = []
        dir_path = Path(directory) if directory else self.output_dir

        if not dir_path:
            return TMDLValidationResult(
                is_valid=False,
                files_checked=0,
                issues=[TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message="No directory specified for validation",
                )],
            )

        if not dir_path.exists():
            return TMDLValidationResult(
                is_valid=False,
                files_checked=0,
                issues=[TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message=f"Directory not found: {dir_path}",
                )],
            )

        # Check required files (any accepted layout)
        has_valid_layout = False
        for layout in self.REQUIRED_LAYOUTS:
            if all((dir_path / required).exists() for required in layout):
                has_valid_layout = True
                break

        if not has_valid_layout:
            # Report against Tableau-standard layout first.
            for required in self.REQUIRED_LAYOUTS[0]:
                file_path = dir_path / required
                if not file_path.exists():
                    issues.append(TMDLIssue(
                        severity=TMDLSeverity.ERROR,
                        message=f"Required file missing: {required}",
                        file_path=str(file_path),
                    ))

        # Find and validate all .tmdl files
        tmdl_files = list(dir_path.rglob("*.tmdl"))
        files_checked = len(tmdl_files)

        if files_checked == 0:
            issues.append(TMDLIssue(
                severity=TMDLSeverity.ERROR,
                message="No .tmdl files found in directory",
            ))

        for tmdl_file in tmdl_files:
            file_issues = self.validate_file(tmdl_file)
            issues.extend(file_issues)

        is_valid = not any(i.severity == TMDLSeverity.ERROR for i in issues)

        return TMDLValidationResult(
            is_valid=is_valid,
            files_checked=files_checked,
            issues=issues,
        )

    def validate_file(self, file_path: Path) -> list[TMDLIssue]:
        """
        Validate a single TMDL file.

        Args:
            file_path: Path to TMDL file

        Returns:
            List of validation issues
        """
        issues = []

        try:
            content = file_path.read_text(encoding='utf-8')
        except Exception as e:
            return [TMDLIssue(
                severity=TMDLSeverity.ERROR,
                message=f"Failed to read file: {e}",
                file_path=str(file_path),
            )]

        # Check for empty file
        if not content.strip():
            issues.append(TMDLIssue(
                severity=TMDLSeverity.WARNING,
                message="Empty TMDL file",
                file_path=str(file_path),
            ))
            return issues

        # File-specific validation
        filename = file_path.name.lower()

        if filename == "model.tmdl":
            issues.extend(self._validate_model_file(content, file_path))
        elif filename == "database.tmdl":
            issues.extend(self._validate_database_file(content, file_path))
        elif filename.endswith(".tmdl"):
            # Table or relationship file
            if "relationship" in content.lower():
                issues.extend(self._validate_relationship_content(content, file_path))
            elif "table" in content.lower():
                issues.extend(self._validate_table_content(content, file_path))

        # General validation
        issues.extend(self._validate_general_syntax(content, file_path))

        return issues

    def _validate_model_file(self, content: str, file_path: Path) -> list[TMDLIssue]:
        """Validate model.tmdl file."""
        issues = []

        if not content.strip().startswith("model"):
            issues.append(TMDLIssue(
                severity=TMDLSeverity.ERROR,
                message="model.tmdl must start with 'model' declaration",
                file_path=str(file_path),
            ))

        # Check for culture setting
        if "culture:" not in content.lower():
            issues.append(TMDLIssue(
                severity=TMDLSeverity.WARNING,
                message="No culture setting found in model",
                file_path=str(file_path),
                suggestion="Add 'culture: en-US' or appropriate locale",
            ))

        return issues

    def _validate_database_file(self, content: str, file_path: Path) -> list[TMDLIssue]:
        """Validate database.tmdl file."""
        issues = []

        if "compatibilitylevel:" not in content.lower():
            issues.append(TMDLIssue(
                severity=TMDLSeverity.WARNING,
                message="No compatibilityLevel found in database.tmdl",
                file_path=str(file_path),
                suggestion="Add 'compatibilityLevel: 1567' or appropriate version",
            ))

        return issues

    def _validate_table_content(self, content: str, file_path: Path) -> list[TMDLIssue]:
        """Validate table definition content."""
        issues = []
        lines = content.split('\n')

        # Check for table declaration
        if not self.TABLE_PATTERN.search(content):
            issues.append(TMDLIssue(
                severity=TMDLSeverity.WARNING,
                message="No valid table declaration found",
                file_path=str(file_path),
            ))

        # Check columns for dataType
        column_matches = list(self.COLUMN_PATTERN.finditer(content))
        for match in column_matches:
            col_name = match.group(1)
            # Find the column's content block
            start_pos = match.start()
            # Look for dataType after the column declaration
            remaining = content[start_pos:start_pos + 500]  # Check next 500 chars
            if "dataType:" not in remaining:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.WARNING,
                    message=f"Column '{col_name}' may be missing dataType",
                    file_path=str(file_path),
                ))

        # Check data types are valid
        for dt_match in self.DATATYPE_PATTERN.finditer(content):
            dtype = dt_match.group(1).lower()
            if dtype not in self.VALID_DATA_TYPES:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.WARNING,
                    message=f"Unrecognized data type: {dtype}",
                    file_path=str(file_path),
                    suggestion=f"Valid types: {', '.join(sorted(self.VALID_DATA_TYPES))}",
                ))

        # Check measure expressions
        measure_matches = list(self.MEASURE_PATTERN.finditer(content))
        for match in measure_matches:
            measure_name = match.group(1)
            start_pos = match.start()
            remaining = content[start_pos:start_pos + 1000]
            # Support both forms:
            # - expression: ``` ... ```
            # - measure 'Name' = ``` ... ```
            has_expression = "expression:" in remaining or "= ```" in remaining
            if not has_expression:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message=f"Measure '{measure_name}' is missing expression",
                    file_path=str(file_path),
                ))

        return issues

    def _validate_relationship_content(self, content: str, file_path: Path) -> list[TMDLIssue]:
        """Validate relationship definitions."""
        issues = []

        # Check for required relationship properties
        rel_pattern = re.compile(r'relationship\s+(\w+)', re.MULTILINE)
        rel_matches = list(rel_pattern.finditer(content))

        for match in rel_matches:
            rel_name = match.group(1)
            start_pos = match.start()
            rel_block = content[start_pos:start_pos + 500]

            if "fromColumn:" not in rel_block:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message=f"Relationship '{rel_name}' missing fromColumn",
                    file_path=str(file_path),
                ))

            if "toColumn:" not in rel_block:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message=f"Relationship '{rel_name}' missing toColumn",
                    file_path=str(file_path),
                ))

        return issues

    def _validate_general_syntax(self, content: str, file_path: Path) -> list[TMDLIssue]:
        """Validate general TMDL syntax."""
        issues = []
        lines = content.split('\n')

        # Check for common syntax issues
        for i, line in enumerate(lines, 1):
            stripped = line.strip()

            # Skip comments and empty lines
            if not stripped or stripped.startswith('///'):
                continue

            # Check for tabs vs spaces consistency (TMDL uses tabs)
            if line.startswith(' ') and not line.startswith('  '):
                # Mixed indentation
                pass  # This is common, skip warning

            # Check for unclosed quotes
            quote_count = stripped.count("'") - stripped.count("\\'")
            if quote_count % 2 != 0:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.WARNING,
                    message="Potentially unclosed single quote",
                    file_path=str(file_path),
                    line_number=i,
                ))

            # Check for Looker syntax leakage
            if "${" in line and "}" in line:
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.ERROR,
                    message="Unconverted Looker field reference found",
                    file_path=str(file_path),
                    line_number=i,
                ))

            # Check for SQL comments that should be DAX comments
            if "-- " in stripped and not stripped.startswith("///"):
                issues.append(TMDLIssue(
                    severity=TMDLSeverity.WARNING,
                    message="SQL-style comment found (use /// for TMDL comments)",
                    file_path=str(file_path),
                    line_number=i,
                ))

        # Check expression blocks are properly closed
        expr_opens = content.count("expression: ```") + content.count("expression:\n\t\t\t```")
        expr_closes = content.count("```\n")
        # This is approximate - triple backticks might be used elsewhere
        if expr_opens > 0 and expr_closes < expr_opens:
            issues.append(TMDLIssue(
                severity=TMDLSeverity.WARNING,
                message="Possible unclosed expression block",
                file_path=str(file_path),
            ))

        return issues

    def validate_content(self, content: str, filename: str = "inline.tmdl") -> list[TMDLIssue]:
        """
        Validate TMDL content directly.

        Args:
            content: TMDL content string
            filename: Optional filename for error messages

        Returns:
            List of validation issues
        """
        temp_dir = Path(tempfile.mkdtemp(prefix="tmdl_validate_"))
        temp_path = temp_dir / filename
        try:
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(content, encoding="utf-8")
            return self.validate_file(temp_path)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


def validate_tmdl_output(
    output_dir: str,
) -> TMDLValidationResult:
    """
    Convenience function to validate TMDL output directory.

    Args:
        output_dir: Path to TMDL output directory

    Returns:
        TMDLValidationResult
    """
    validator = TMDLValidator()
    return validator.validate_directory(output_dir)
