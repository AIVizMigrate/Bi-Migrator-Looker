"""
Data Models for Looker to Power BI Migration.

Contains models for:
- Looker LookML structures (views, explores, models, dimensions, measures)
- Power BI structures (tables, columns, measures, relationships)
- Migration results and metadata
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any


# =============================================================================
# Enums
# =============================================================================

class DataType(Enum):
    """Power BI data types."""
    STRING = "string"
    INT64 = "int64"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATETIME = "dateTime"
    DATE = "dateTime"
    TIME = "dateTime"
    BINARY = "binary"


class Cardinality(Enum):
    """Relationship cardinality."""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class CrossFilterDirection(Enum):
    """Cross-filter direction for relationships."""
    SINGLE = "single"
    BOTH = "both"


class LookerFieldType(Enum):
    """Looker field types."""
    DIMENSION = "dimension"
    DIMENSION_GROUP = "dimension_group"
    MEASURE = "measure"
    FILTER = "filter"
    PARAMETER = "parameter"


class LookerMeasureType(Enum):
    """Looker measure types."""
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    SUM_DISTINCT = "sum_distinct"
    AVERAGE = "average"
    AVERAGE_DISTINCT = "average_distinct"
    MIN = "min"
    MAX = "max"
    LIST = "list"
    PERCENTILE = "percentile"
    MEDIAN = "median"
    NUMBER = "number"
    STRING = "string"
    YESNO = "yesno"
    DATE = "date"


class LookerDataType(Enum):
    """Legacy Looker data type enum retained for backward compatibility."""
    STRING = "string"
    NUMBER = "number"
    INT = "int"
    INTEGER = "integer"
    DECIMAL = "decimal"
    FLOAT = "float"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    TIME = "time"
    YESNO = "yesno"
    BOOLEAN = "boolean"


# Backward-compatible alias used by tests and existing integrations.
PbiDataType = DataType


# =============================================================================
# Looker Source Models
# =============================================================================

@dataclass
class LookmlDimension:
    """Represents a Looker dimension."""
    name: str
    type: str = "string"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    primary_key: bool = False
    value_format: Optional[str] = None
    value_format_name: Optional[str] = None
    group_label: Optional[str] = None
    drill_fields: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    # For dimension_group
    timeframes: list[str] = field(default_factory=list)
    convert_tz: bool = True
    datatype: Optional[str] = None


@dataclass
class LookmlMeasure:
    """Represents a Looker measure."""
    name: str
    type: str = "count"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    value_format: Optional[str] = None
    value_format_name: Optional[str] = None
    group_label: Optional[str] = None
    drill_fields: list[str] = field(default_factory=list)
    filters: dict[str, str] = field(default_factory=dict)
    # For percentile/median
    percentile: Optional[int] = None


@dataclass
class LookmlFilter:
    """Represents a Looker filter field."""
    name: str
    type: str = "string"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    default_value: Optional[str] = None
    suggestions: list[str] = field(default_factory=list)


@dataclass
class LookmlParameter:
    """Represents a Looker parameter."""
    name: str
    type: str = "string"
    label: Optional[str] = None
    description: Optional[str] = None
    default_value: Optional[str] = None
    allowed_values: list[dict] = field(default_factory=list)


@dataclass
class LookmlJoin:
    """Represents a join in an explore."""
    name: str  # View name being joined
    type: str = "left_outer"  # left_outer, inner, full_outer, cross
    relationship: str = "many_to_one"  # many_to_one, one_to_many, one_to_one, many_to_many
    sql_on: Optional[str] = None
    sql_foreign_key: Optional[str] = None
    from_view: Optional[str] = None  # Alias
    fields: list[str] = field(default_factory=list)
    required_joins: list[str] = field(default_factory=list)


@dataclass
class LookmlView:
    """Represents a Looker view."""
    name: str
    sql_table_name: Optional[str] = None
    derived_table: Optional[dict] = None
    label: Optional[str] = None
    description: Optional[str] = None
    dimensions: list[LookmlDimension] = field(default_factory=list)
    measures: list[LookmlMeasure] = field(default_factory=list)
    filters: list[LookmlFilter] = field(default_factory=list)
    parameters: list[LookmlParameter] = field(default_factory=list)
    sets: dict[str, list[str]] = field(default_factory=dict)
    extends: list[str] = field(default_factory=list)
    extension: str = "required"  # required, optional


@dataclass
class LookmlExplore:
    """Represents a Looker explore."""
    name: str
    view_name: Optional[str] = None  # Base view (defaults to explore name)
    from_view: Optional[str] = None  # Backward-compatible alias for view_name
    label: Optional[str] = None
    description: Optional[str] = None
    hidden: bool = False
    joins: list[LookmlJoin] = field(default_factory=list)
    always_filter: dict[str, str] = field(default_factory=dict)
    conditionally_filter: dict[str, Any] = field(default_factory=dict)
    sql_always_where: Optional[str] = None
    sql_always_having: Optional[str] = None
    extends: list[str] = field(default_factory=list)
    fields: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Keep both names in sync for compatibility with older callers.
        if self.view_name and not self.from_view:
            self.from_view = self.view_name
        elif self.from_view and not self.view_name:
            self.view_name = self.from_view


@dataclass
class LookmlModel:
    """Represents a Looker model file."""
    name: str
    connection: Optional[str] = None
    label: Optional[str] = None
    includes: list[str] = field(default_factory=list)
    explores: list[LookmlExplore] = field(default_factory=list)
    datagroups: list[dict] = field(default_factory=list)
    access_grants: list[dict] = field(default_factory=list)
    file_path: Optional[str] = None


@dataclass
class LookmlProject:
    """Represents a complete Looker LookML project."""
    name: str
    models: list[LookmlModel] = field(default_factory=list)
    views: list[LookmlView] = field(default_factory=list)
    connection: Optional[str] = None
    project_path: Optional[str] = None


# =============================================================================
# Power BI Target Models
# =============================================================================

@dataclass
class PbiColumn:
    """Power BI table column definition."""
    name: str
    data_type: DataType
    source_column: Optional[str] = None
    expression: Optional[str] = None
    is_calculated: bool = False
    is_hidden: bool = False
    format_string: Optional[str] = None
    summarize_by: str = "none"
    description: Optional[str] = None
    sort_by_column: Optional[str] = None
    display_folder: Optional[str] = None
    lineage_tag: Optional[str] = None
    # Looker source tracking (for TMDL annotations)
    looker_name: Optional[str] = None
    formula_looker: Optional[str] = None
    original_expression: Optional[str] = None


@dataclass
class PbiMeasure:
    """Power BI measure definition."""
    name: str
    expression: str
    format_string: Optional[str] = None
    description: Optional[str] = None
    display_folder: Optional[str] = None
    is_hidden: bool = False
    lineage_tag: Optional[str] = None
    # Looker source tracking (for TMDL annotations)
    looker_name: Optional[str] = None
    formula_looker: Optional[str] = None
    original_expression: Optional[str] = None


@dataclass
class PbiPartition:
    """Power BI table partition (data source)."""
    name: str
    source_type: str = "m"  # m (Power Query), calculated, etc.
    expression: str = ""
    mode: str = "import"


@dataclass
class PbiTable:
    """Power BI table definition."""
    name: str
    columns: list[PbiColumn] = field(default_factory=list)
    measures: list[PbiMeasure] = field(default_factory=list)
    partitions: list[PbiPartition] = field(default_factory=list)
    description: Optional[str] = None
    is_hidden: bool = False
    lineage_tag: Optional[str] = None


@dataclass
class PbiRelationship:
    """Power BI relationship definition."""
    name: str
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cardinality: Cardinality = Cardinality.MANY_TO_ONE
    cross_filter_direction: CrossFilterDirection = CrossFilterDirection.SINGLE
    is_active: bool = True
    lineage_tag: Optional[str] = None


@dataclass
class PbiModel:
    """Complete Power BI model definition."""
    name: str
    tables: list[PbiTable] = field(default_factory=list)
    relationships: list[PbiRelationship] = field(default_factory=list)
    description: Optional[str] = None
    culture: str = "en-US"


# =============================================================================
# Migration Result Models
# =============================================================================

@dataclass
class MigrationWarning:
    """Warning generated during migration."""
    code: str = "MIGRATION_WARNING"
    message: str = ""
    source_element: Optional[str] = None
    suggestion: Optional[str] = None


@dataclass
class MigrationError:
    """Error generated during migration."""
    code: str
    message: str
    source_element: Optional[str] = None
    details: Optional[str] = None


@dataclass
class ConversionResult:
    """Result of expression conversion."""
    dax_expression: str
    confidence: float = 1.0
    warnings: list[str] = field(default_factory=list)
    used_llm: bool = False
    used_api: bool = False  # Whether DAX API was used for AI-powered conversion
    original_expression: Optional[str] = None

    @property
    def expression(self) -> str:
        """Backward-compatible alias for dax_expression."""
        return self.dax_expression

    @expression.setter
    def expression(self, value: str) -> None:
        self.dax_expression = value

    @property
    def success(self) -> bool:
        """Compatibility flag used by older tests/callers."""
        return bool(self.dax_expression and self.dax_expression.strip())


@dataclass
class MigrationResult:
    """Result of a migration operation."""
    success: bool
    output_path: Optional[str] = None
    source_file: Optional[str] = None
    model_name: Optional[str] = None
    tables_count: Optional[int] = None
    measures_count: Optional[int] = None
    relationships_count: Optional[int] = None
    views_converted: Optional[int] = None
    explores_converted: Optional[int] = None
    warnings: list[MigrationWarning] = field(default_factory=list)
    errors: list[MigrationError] = field(default_factory=list)
    duration_seconds: Optional[float] = None
    generated_files: list[str] = field(default_factory=list)
    calculation_summary: Optional[dict] = None  # AI vs rule-based conversion stats
    conversion_stats: dict[str, int] = field(default_factory=dict)
    # Internal snapshots to avoid reparsing/reconverting in downstream orchestration.
    project_snapshot: Optional[LookmlProject] = None
    pbi_model_snapshot: Optional[PbiModel] = None
