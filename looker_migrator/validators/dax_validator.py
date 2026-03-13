"""
DAX Expression Validator for Looker Migrator.

Validates DAX expressions converted from Looker LookML measures.
"""

import re
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class ValidationSeverity(Enum):
    """Severity levels for validation issues."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    severity: ValidationSeverity
    message: str
    expression: Optional[str] = None
    position: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class DAXValidationResult:
    """Result of DAX validation."""
    is_valid: bool
    expression: str
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(i.severity == ValidationSeverity.ERROR for i in self.issues)

    @property
    def has_warnings(self) -> bool:
        return any(i.severity == ValidationSeverity.WARNING for i in self.issues)


class DAXValidator:
    """Validates DAX expressions for syntax and common issues."""

    # Known DAX functions (subset of most common)
    KNOWN_FUNCTIONS = {
        # Aggregation
        "SUM", "SUMX", "COUNT", "COUNTA", "COUNTX", "COUNTROWS",
        "COUNTBLANK", "DISTINCTCOUNT", "DISTINCTCOUNTNOBLANK",
        "AVERAGE", "AVERAGEX", "MIN", "MINX", "MAX", "MAXX",
        "PRODUCT", "PRODUCTX", "MEDIAN", "MEDIANX",
        # Math
        "ABS", "CEILING", "FLOOR", "ROUND", "ROUNDUP", "ROUNDDOWN",
        "TRUNC", "INT", "SIGN", "SQRT", "POWER", "EXP", "LN", "LOG",
        "LOG10", "MOD", "QUOTIENT", "DIVIDE", "RAND", "RANDBETWEEN",
        # Logical
        "IF", "AND", "OR", "NOT", "TRUE", "FALSE", "IFERROR", "SWITCH",
        "COALESCE", "ISBLANK", "ISERROR", "ISLOGICAL", "ISNONTEXT",
        "ISNUMBER", "ISTEXT", "BLANK",
        # Text
        "CONCATENATE", "CONCATENATEX", "LEFT", "RIGHT", "MID", "LEN",
        "UPPER", "LOWER", "PROPER", "TRIM", "SUBSTITUTE", "REPLACE",
        "FIND", "SEARCH", "EXACT", "FIXED", "FORMAT", "VALUE", "UNICHAR",
        # Date/Time
        "DATE", "TIME", "DATETIME", "NOW", "TODAY", "YEAR", "MONTH",
        "DAY", "HOUR", "MINUTE", "SECOND", "WEEKDAY", "WEEKNUM",
        "YEARFRAC", "QUARTER", "EOMONTH", "EDATE", "DATEDIFF", "DATEVALUE",
        "CALENDAR", "CALENDARAUTO",
        # Filter
        "CALCULATE", "CALCULATETABLE", "FILTER", "ALL", "ALLEXCEPT",
        "ALLSELECTED", "ALLNOBLANKROW", "VALUES", "DISTINCT",
        "RELATEDTABLE", "RELATED", "EARLIER", "EARLIEST", "HASONEVALUE",
        "HASONEFILTER", "ISCROSSFILTERED", "ISFILTERED", "USERELATIONSHIP",
        "TREATAS", "KEEPFILTERS", "REMOVEFILTERS",
        # Table
        "ADDCOLUMNS", "SELECTCOLUMNS", "SUMMARIZE", "SUMMARIZECOLUMNS",
        "GROUPBY", "TOPN", "SAMPLE", "GENERATE", "GENERATEALL",
        "GENERATESERIES", "ROW", "UNION", "EXCEPT", "INTERSECT",
        "NATURALINNERJOIN", "NATURALLEFTOUTERJOIN", "CROSSJOIN",
        "DATATABLE", "CURRENTGROUP", "CONTAINS", "CONTAINSROW",
        # Time Intelligence
        "DATESYTD", "DATESMTD", "DATESQTD", "TOTALYTD", "TOTALMTD",
        "TOTALQTD", "SAMEPERIODLASTYEAR", "PREVIOUSMONTH", "PREVIOUSQUARTER",
        "PREVIOUSYEAR", "NEXTMONTH", "NEXTQUARTER", "NEXTYEAR",
        "PARALLELPERIOD", "DATEADD", "DATESBETWEEN", "DATESINPERIOD",
        "STARTOFYEAR", "STARTOFQUARTER", "STARTOFMONTH", "ENDOFYEAR",
        "ENDOFQUARTER", "ENDOFMONTH", "OPENINGBALANCEMONTH",
        "OPENINGBALANCEQUARTER", "OPENINGBALANCEYEAR",
        "CLOSINGBALANCEMONTH", "CLOSINGBALANCEQUARTER", "CLOSINGBALANCEYEAR",
        # Info
        "USERCULTURE", "USERNAME", "USERPRINCIPALNAME",
        # Parent-Child
        "PATH", "PATHCONTAINS", "PATHITEM", "PATHITEMREVERSE", "PATHLENGTH",
        # Other
        "RANKX", "PERCENTILEX.INC", "PERCENTILEX.EXC", "VAR", "RETURN",
        "SELECTEDVALUE", "ERROR", "EVALUATEANDLOG",
    }

    # Patterns
    FUNCTION_CALL_PATTERN = re.compile(r'([A-Z_][A-Z0-9_.]*)\s*\(', re.IGNORECASE)
    TABLE_COLUMN_PATTERN = re.compile(r"'([^']+)'\s*\[\s*([^\]]+)\s*\]")
    UNQUOTED_TABLE_PATTERN = re.compile(r'\b([A-Z][A-Za-z0-9_]*)\s*\[', re.IGNORECASE)
    STRING_LITERAL_PATTERN = re.compile(r'"([^"]*)"')

    # Balance patterns
    PAREN_OPEN = '('
    PAREN_CLOSE = ')'
    BRACKET_OPEN = '['
    BRACKET_CLOSE = ']'

    def __init__(self, strict_mode: bool = False):
        """
        Initialize DAX validator.

        Args:
            strict_mode: If True, treat warnings as errors
        """
        self.strict_mode = strict_mode

    def validate(self, expression: str) -> DAXValidationResult:
        """
        Validate a DAX expression.

        Args:
            expression: The DAX expression to validate

        Returns:
            DAXValidationResult with validation status and issues
        """
        issues: list[ValidationIssue] = []

        if not expression or not expression.strip():
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="Empty expression",
            ))
            return DAXValidationResult(
                is_valid=False,
                expression=expression or "",
                issues=issues,
            )

        # Run validation checks
        issues.extend(self._check_balanced_delimiters(expression))
        issues.extend(self._check_functions(expression))
        issues.extend(self._check_column_references(expression))
        issues.extend(self._check_common_issues(expression))

        # Determine validity
        is_valid = not any(
            i.severity == ValidationSeverity.ERROR for i in issues
        )

        if self.strict_mode and any(
            i.severity == ValidationSeverity.WARNING for i in issues
        ):
            is_valid = False

        return DAXValidationResult(
            is_valid=is_valid,
            expression=expression,
            issues=issues,
        )

    def validate_all(self, expressions: list[str]) -> list[DAXValidationResult]:
        """Validate multiple expressions."""
        return [self.validate(expr) for expr in expressions]

    def _check_balanced_delimiters(self, expression: str) -> list[ValidationIssue]:
        """Check for balanced parentheses and brackets."""
        issues = []

        # Track parentheses
        paren_count = 0
        bracket_count = 0
        in_string = False

        for i, char in enumerate(expression):
            if char == '"' and (i == 0 or expression[i-1] != '\\'):
                in_string = not in_string
                continue

            if in_string:
                continue

            if char == self.PAREN_OPEN:
                paren_count += 1
            elif char == self.PAREN_CLOSE:
                paren_count -= 1
                if paren_count < 0:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        message="Unmatched closing parenthesis",
                        expression=expression,
                        position=i,
                    ))
                    paren_count = 0
            elif char == self.BRACKET_OPEN:
                bracket_count += 1
            elif char == self.BRACKET_CLOSE:
                bracket_count -= 1
                if bracket_count < 0:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        message="Unmatched closing bracket",
                        expression=expression,
                        position=i,
                    ))
                    bracket_count = 0

        if paren_count > 0:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message=f"Missing {paren_count} closing parenthesis(es)",
                expression=expression,
            ))

        if bracket_count > 0:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message=f"Missing {bracket_count} closing bracket(s)",
                expression=expression,
            ))

        return issues

    def _check_functions(self, expression: str) -> list[ValidationIssue]:
        """Check function names and usage."""
        issues = []

        # Find all function calls
        for match in self.FUNCTION_CALL_PATTERN.finditer(expression):
            func_name = match.group(1).upper()

            # Skip if it looks like a table reference
            if func_name.startswith("'") or "." in func_name:
                continue

            if func_name not in self.KNOWN_FUNCTIONS:
                # Check if it's a close match
                suggestion = self._find_similar_function(func_name)
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    message=f"Unknown function: {func_name}",
                    expression=expression,
                    position=match.start(),
                    suggestion=suggestion,
                ))

        return issues

    def _check_column_references(self, expression: str) -> list[ValidationIssue]:
        """Check column reference syntax."""
        issues = []

        # Check for potentially unquoted table names with spaces
        words = expression.split()
        for i, word in enumerate(words):
            if '[' in word and "'" not in word:
                # Table name before bracket might need quoting
                bracket_pos = word.find('[')
                table_part = word[:bracket_pos]
                if table_part and not table_part.startswith('['):
                    # Only warn if table name looks like it might have issues
                    if table_part.isalnum() or table_part.replace('_', '').isalnum():
                        continue
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        message=f"Table name '{table_part}' may need quoting",
                        expression=expression,
                        suggestion=f"Use '{table_part}'[column] syntax",
                    ))

        return issues

    def _check_common_issues(self, expression: str) -> list[ValidationIssue]:
        """Check for common DAX issues."""
        issues = []

        # Check for Looker syntax that wasn't converted
        if "${" in expression and "}" in expression:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="Unconverted Looker field reference: ${...}",
                expression=expression,
                suggestion="Field references should be converted to DAX column syntax",
            ))

        # Check for SQL-style operators
        if " <> " in expression:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.INFO,
                message="SQL-style '<>' operator used (valid but '!=' is more common)",
                expression=expression,
            ))

        # Check for double operators
        if "==" in expression:
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                message="Double equals '==' found - DAX uses single '='",
                expression=expression,
                suggestion="Replace '==' with '='",
            ))

        # Check for potentially problematic NULL handling
        if re.search(r'\bNULL\b', expression, re.IGNORECASE):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.WARNING,
                message="'NULL' found - DAX uses BLANK() for null values",
                expression=expression,
                suggestion="Use BLANK() or ISBLANK() for null handling",
            ))

        # Check for date format strings
        if re.search(r'%[YyMmDdHhIiSs]', expression):
            issues.append(ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message="SQL-style date format codes found",
                expression=expression,
                suggestion="Convert to DAX FORMAT function patterns",
            ))

        return issues

    def _find_similar_function(self, func_name: str) -> Optional[str]:
        """Find a similar known function name."""
        func_upper = func_name.upper()

        # Direct partial matches
        for known in self.KNOWN_FUNCTIONS:
            if func_upper in known or known in func_upper:
                return f"Did you mean {known}?"

        # Common typos/alternatives
        alternatives = {
            "CONCAT": "CONCATENATE",
            "STRLEN": "LEN",
            "LENGTH": "LEN",
            "SUBSTR": "MID",
            "SUBSTRING": "MID",
            "NULLIF": "IF + ISBLANK",
            "COALESCE": "COALESCE",
            "NVL": "COALESCE or IF + ISBLANK",
            "IFNULL": "IF + ISBLANK",
            "DATEPART": "YEAR/MONTH/DAY",
            "GETDATE": "TODAY or NOW",
            "CURDATE": "TODAY",
        }

        if func_upper in alternatives:
            return f"Use {alternatives[func_upper]} instead"

        return None


def validate_dax_expression(expression: str, strict: bool = False) -> DAXValidationResult:
    """
    Convenience function to validate a single DAX expression.

    Args:
        expression: DAX expression to validate
        strict: Whether to use strict validation mode

    Returns:
        DAXValidationResult
    """
    validator = DAXValidator(strict_mode=strict)
    return validator.validate(expression)
