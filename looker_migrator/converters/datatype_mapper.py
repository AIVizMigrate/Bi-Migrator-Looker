"""
Data Type Mapper for Looker to Power BI conversion.

Maps Looker data types to Power BI data types.
"""

from enum import Enum

from ..models import DataType


class DatatypeMapper:
    """Maps Looker data types to Power BI data types."""

    @staticmethod
    def _normalize_looker_type(looker_type: object) -> str:
        """Normalize type input from strings or enums."""
        if looker_type is None:
            return ""
        if isinstance(looker_type, Enum):
            return str(looker_type.value).lower().strip()
        return str(looker_type).lower().strip()

    # Looker type to Power BI type mapping
    TYPE_MAP: dict[str, DataType] = {
        # String types
        'string': DataType.STRING,
        'tier': DataType.STRING,
        'zipcode': DataType.STRING,

        # Numeric types
        'number': DataType.DOUBLE,
        'int': DataType.INT64,
        'integer': DataType.INT64,
        'decimal': DataType.DECIMAL,
        'float': DataType.DOUBLE,

        # Date/Time types
        'date': DataType.DATE,
        'datetime': DataType.DATETIME,
        'timestamp': DataType.DATETIME,
        'time': DataType.TIME,

        # Boolean
        'yesno': DataType.BOOLEAN,
        'boolean': DataType.BOOLEAN,

        # Location
        'location': DataType.STRING,

        # Duration
        'duration': DataType.DOUBLE,
    }

    # Looker dimension group timeframes to Power BI types
    TIMEFRAME_TYPES: dict[str, DataType] = {
        'raw': DataType.DATETIME,
        'date': DataType.DATE,
        'week': DataType.DATE,
        'month': DataType.DATE,
        'quarter': DataType.DATE,
        'year': DataType.INT64,
        'day_of_week': DataType.STRING,
        'day_of_month': DataType.INT64,
        'month_name': DataType.STRING,
        'month_num': DataType.INT64,
        'quarter_of_year': DataType.INT64,
        'week_of_year': DataType.INT64,
        'hour': DataType.INT64,
        'minute': DataType.INT64,
        'second': DataType.INT64,
        'time': DataType.TIME,
        'time_of_day': DataType.TIME,
    }

    # TMDL type names
    TMDL_TYPE_MAP: dict[DataType, str] = {
        DataType.STRING: 'string',
        DataType.INT64: 'int64',
        DataType.DOUBLE: 'double',
        DataType.DECIMAL: 'decimal',
        DataType.BOOLEAN: 'boolean',
        DataType.DATETIME: 'dateTime',
        DataType.DATE: 'dateTime',
        DataType.TIME: 'dateTime',
        DataType.BINARY: 'binary',
    }

    @classmethod
    def map_type(cls, looker_type: object) -> DataType:
        """
        Map a Looker data type to Power BI data type.

        Args:
            looker_type: Looker type string or enum value

        Returns:
            Power BI DataType enum value
        """
        type_lower = cls._normalize_looker_type(looker_type)
        if not type_lower:
            return DataType.STRING

        return cls.TYPE_MAP.get(type_lower, DataType.STRING)

    @classmethod
    def looker_to_pbi(cls, looker_type: object) -> DataType:
        """Backward-compatible alias for map_type()."""
        return cls.map_type(looker_type)

    @classmethod
    def map_timeframe(cls, timeframe: str) -> DataType:
        """
        Map a Looker dimension_group timeframe to Power BI type.

        Args:
            timeframe: Looker timeframe string

        Returns:
            Power BI DataType enum value
        """
        if not timeframe:
            return DataType.DATETIME

        tf_lower = timeframe.lower().strip()
        return cls.TIMEFRAME_TYPES.get(tf_lower, DataType.STRING)

    @classmethod
    def get_tmdl_type(cls, data_type: DataType) -> str:
        """
        Get the TMDL type string for a DataType.

        Args:
            data_type: Power BI DataType enum value

        Returns:
            TMDL type string
        """
        return cls.TMDL_TYPE_MAP.get(data_type, 'string')

    @classmethod
    def get_summarize_by(cls, data_type: DataType) -> str:
        """
        Get the default summarizeBy value for a data type.

        Args:
            data_type: Power BI DataType

        Returns:
            summarizeBy value string
        """
        if data_type in (DataType.INT64, DataType.DOUBLE, DataType.DECIMAL):
            return 'sum'
        return 'none'

    @classmethod
    def get_format_string(cls, data_type: DataType) -> str:
        """
        Get a default format string for a data type.

        Args:
            data_type: Power BI DataType

        Returns:
            Power BI format string
        """
        defaults = {
            DataType.INT64: '0',
            DataType.DOUBLE: '#,##0.00',
            DataType.DECIMAL: '#,##0.00',
            DataType.DATE: 'Short Date',
            DataType.DATETIME: 'General Date',
            DataType.TIME: 'Long Time',
            DataType.BOOLEAN: 'TRUE/FALSE',
        }

        return defaults.get(data_type, '')
