"""
SQL to DAX Converter for derived tables.

Converts Looker derived table SQL to Power Query M.
"""

import os
import re
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from ..common.log_utils import log_debug, log_warning


@dataclass
class SqlConversionResult:
    """Result of SQL to M conversion."""
    m_expression: str
    success: bool
    warnings: list[str]
    original_sql: str


class SqlToDaxConverter:
    """
    Converts SQL queries to Power Query M expressions.

    Used for converting Looker derived tables to Power BI.
    """

    def __init__(self, config: Optional[dict] = None):
        """Initialize the converter."""
        self.config = config or {}

    @staticmethod
    def _clean_identifier_part(part: str) -> str:
        """Remove outer quoting characters from an identifier segment."""
        value = str(part or "").strip()
        if not value:
            return value
        if (
            (value.startswith('"') and value.endswith('"'))
            or (value.startswith("'") and value.endswith("'"))
            or (value.startswith("`") and value.endswith("`"))
            or (value.startswith("[") and value.endswith("]"))
        ):
            return value[1:-1]
        return value

    def _split_table_name(self, table_name: str) -> list[str]:
        """Split a dotted table reference into cleaned parts."""
        cleaned = str(table_name or "").strip()
        if not cleaned:
            return []
        # Handle quoted whole identifiers such as `project.dataset.table`.
        if (
            (cleaned.startswith("`") and cleaned.endswith("`"))
            or (cleaned.startswith('"') and cleaned.endswith('"'))
            or (cleaned.startswith("'") and cleaned.endswith("'"))
        ):
            cleaned = cleaned[1:-1]
        parts = [self._clean_identifier_part(p) for p in cleaned.split(".")]
        return [p for p in parts if p]

    @staticmethod
    def _quote_sql_server_identifier(identifier: str) -> str:
        value = str(identifier or "").replace("]", "]]")
        return f"[{value}]"

    @staticmethod
    def _quote_ansi_identifier(identifier: str) -> str:
        value = str(identifier or "").replace('"', '""')
        return f'"{value}"'

    @staticmethod
    def _escape_m_string(value: str) -> str:
        """Escape a string literal for embedding inside M double quotes."""
        return str(value or "").replace('"', '""')

    def convert_derived_table(
        self,
        sql: str,
        connection_type: str = "sql_server",
    ) -> SqlConversionResult:
        """
        Convert a derived table SQL to Power Query M.

        Args:
            sql: SQL query from derived table
            connection_type: Database type (sql_server, bigquery, etc.)

        Returns:
            SqlConversionResult with M expression
        """
        warnings = []

        if not sql or not sql.strip():
            return SqlConversionResult(
                m_expression="",
                success=False,
                warnings=["Empty SQL"],
                original_sql=sql,
            )

        # Clean SQL
        clean_sql = self._clean_sql(sql)

        # Generate M expression based on connection type
        if connection_type.lower() in ("bigquery", "google_bigquery"):
            m_expr = self._generate_bigquery_m(clean_sql)
        elif connection_type.lower() in ("snowflake",):
            m_expr = self._generate_snowflake_m(clean_sql)
        elif connection_type.lower() in ("redshift", "amazon_redshift"):
            m_expr = self._generate_redshift_m(clean_sql)
        else:
            # Default SQL Server pattern
            m_expr = self._generate_sql_server_m(clean_sql)

        return SqlConversionResult(
            m_expression=m_expr,
            success=True,
            warnings=warnings,
            original_sql=sql,
        )

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL for embedding in M."""
        # Remove trailing semicolons
        sql = sql.strip().rstrip(';')

        # Escape double quotes
        sql = sql.replace('"', '""')

        # Remove Looker-specific syntax
        sql = re.sub(r'\$\{[^}]+\}', '/* LOOKER_REF */', sql)

        return sql

    def _generate_sql_server_m(self, sql: str) -> str:
        """Generate M expression for SQL Server."""
        server = os.getenv("LOOKER_SQL_SERVER", "SERVER")
        database = os.getenv("LOOKER_SQL_DATABASE", "DATABASE")
        return f'''let
    Source = Sql.Database("{server}", "{database}"),
    Query = Value.NativeQuery(Source, "
{sql}
", null, [EnableFolding=true])
in
    Query'''

    def _generate_bigquery_m(self, sql: str) -> str:
        """Generate M expression for BigQuery."""
        billing_project = os.getenv("LOOKER_BIGQUERY_BILLING_PROJECT")
        configured_query_project = os.getenv("LOOKER_BIGQUERY_PROJECT") or billing_project
        inferred_query_project = self._infer_bigquery_project_from_sql(sql)
        query_project = configured_query_project or inferred_query_project or "PROJECT"
        query_project_m = self._escape_m_string(query_project)
        if billing_project:
            billing_project_m = self._escape_m_string(billing_project)
            source_line = f'Source = GoogleBigQuery.Database([BillingProject="{billing_project_m}"]),'
        else:
            source_line = "Source = GoogleBigQuery.Database(),"
        return f'''let
    {source_line}
    Project = try Source{{[Name="{query_project_m}"]}}[Data] otherwise Source{{0}}[Data],
    Query = Value.NativeQuery(Project, "
{sql}
")
in
    Query'''

    @staticmethod
    def _infer_bigquery_project_from_sql(sql: str) -> Optional[str]:
        """Infer BigQuery project id from first fully-qualified table reference."""
        if not sql:
            return None
        match = re.search(r'`([^`]+)\.([^`]+)\.([^`]+)`', sql)
        if not match:
            return None
        return match.group(1)

    def _generate_snowflake_m(self, sql: str) -> str:
        """Generate M expression for Snowflake."""
        account = os.getenv("LOOKER_SNOWFLAKE_ACCOUNT", "ACCOUNT")
        warehouse = os.getenv("LOOKER_SNOWFLAKE_WAREHOUSE", "WAREHOUSE")
        database = os.getenv("LOOKER_SNOWFLAKE_DATABASE", "DATABASE")
        return f'''let
    Source = Snowflake.Databases("{account}.snowflakecomputing.com", "{warehouse}"),
    Database = Source{{[Name="{database}"]}}[Data],
    Query = Value.NativeQuery(Database, "
{sql}
")
in
    Query'''

    def _generate_redshift_m(self, sql: str) -> str:
        """Generate M expression for Redshift."""
        cluster = os.getenv("LOOKER_REDSHIFT_CLUSTER", "CLUSTER")
        database = os.getenv("LOOKER_REDSHIFT_DATABASE", "DATABASE")
        return f'''let
    Source = AmazonRedshift.Database("{cluster}.redshift.amazonaws.com", "{database}"),
    Query = Value.NativeQuery(Source, "
{sql}
")
in
    Query'''

    def convert_simple_select(
        self,
        table_name: str,
        columns: list[str],
        schema: Optional[str] = None,
        connection_type: str = "sql_server",
    ) -> str:
        """
        Generate M expression for simple table select.

        Args:
            table_name: Source table name
            columns: List of columns to select
            schema: Optional schema name

        Returns:
            Power Query M expression
        """
        conn = str(connection_type or "sql_server").lower()
        table_parts = self._split_table_name(table_name)

        if conn in ("local_csv", "csv", "file_csv"):
            config_data_dir = None
            if isinstance(self.config, dict):
                converter_cfg = self.config.get("converter", {})
                if isinstance(converter_cfg, dict):
                    config_data_dir = converter_cfg.get("local_data_dir")
            local_data_dir = (
                os.getenv("LOOKER_LOCAL_DATA_DIR")
                or config_data_dir
                or "DATA_DIR"
            )

            if table_parts:
                file_stem = table_parts[-1]
            else:
                file_stem = str(table_name or "table").strip() or "table"
            if not file_stem.lower().endswith(".csv"):
                file_stem = f"{file_stem}.csv"

            csv_path = str(Path(local_data_dir) / file_stem)
            csv_path_m = self._escape_m_string(csv_path)

            if columns and columns != ['*']:
                selected_cols = ", ".join(
                    f'"{self._escape_m_string(self._clean_identifier_part(c))}"'
                    for c in columns
                )
                select_step = f"""
    Selected = Table.SelectColumns(Headers, {{{selected_cols}}})"""
                final_step = "Selected"
            else:
                select_step = ""
                final_step = "Headers"

            return f'''let
    Source = Csv.Document(File.Contents("{csv_path_m}"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]){select_step}
in
    {final_step}'''

        if columns and columns != ['*']:
            select_expr = ", ".join(
                self._quote_ansi_identifier(self._clean_identifier_part(c))
                for c in columns
            )
        else:
            select_expr = "*"

        if conn in ("bigquery", "google_bigquery"):
            billing_project = os.getenv("LOOKER_BIGQUERY_BILLING_PROJECT")
            query_project = os.getenv("LOOKER_BIGQUERY_PROJECT") or billing_project
            default_project = query_project or "PROJECT"
            default_dataset = os.getenv("LOOKER_BIGQUERY_DATASET", "DATASET")

            if len(table_parts) >= 3:
                source_project = table_parts[-3]
                dataset_name = table_parts[-2]
                table_only = table_parts[-1]
            elif len(table_parts) == 2:
                source_project = default_project
                dataset_name = table_parts[0]
                table_only = table_parts[1]
            elif table_parts:
                source_project = default_project
                dataset_name = default_dataset
                table_only = table_parts[0]
            else:
                source_project = default_project
                dataset_name = default_dataset
                table_only = str(table_name or "table")

            lookup_project = query_project or source_project

            if columns and columns != ['*']:
                selected_cols = ", ".join(
                    f'"{self._escape_m_string(self._clean_identifier_part(c))}"'
                    for c in columns
                )
                select_step = f"""
    Selected = Table.SelectColumns(Query, {{{selected_cols}}})"""
                final_step = "Selected"
            else:
                select_step = ""
                final_step = "Query"

            if billing_project:
                billing_project_m = self._escape_m_string(billing_project)
                source_line = f'Source = GoogleBigQuery.Database([BillingProject="{billing_project_m}"]),'
            else:
                source_line = "Source = GoogleBigQuery.Database(),"
            lookup_project_m = self._escape_m_string(lookup_project)
            source_project_m = self._escape_m_string(source_project)
            dataset_name_m = self._escape_m_string(dataset_name)
            table_only_m = self._escape_m_string(table_only)
            native_query_m = self._escape_m_string(
                f"SELECT * FROM `{source_project_m}.{dataset_name_m}.{table_only_m}`"
            )
            return f'''let
    {source_line}
    Project = try Source{{[Name="{lookup_project_m}"]}}[Data] otherwise Source{{0}}[Data],
    Query = Value.NativeQuery(Project, "{native_query_m}"){select_step}
in
    {final_step}'''

        if conn in ("snowflake",):
            account = os.getenv("LOOKER_SNOWFLAKE_ACCOUNT", "ACCOUNT")
            warehouse = os.getenv("LOOKER_SNOWFLAKE_WAREHOUSE", "WAREHOUSE")
            env_database = os.getenv("LOOKER_SNOWFLAKE_DATABASE", "DATABASE")

            if len(table_parts) >= 3:
                database_name = table_parts[-3]
                schema_name = table_parts[-2]
                table_only = table_parts[-1]
            elif len(table_parts) == 2:
                database_name = env_database
                schema_name = table_parts[0]
                table_only = table_parts[1]
            elif table_parts:
                database_name = env_database
                schema_name = "PUBLIC"
                table_only = table_parts[0]
            else:
                database_name = env_database
                schema_name = "PUBLIC"
                table_only = table_name

            query = (
                f"SELECT {select_expr} FROM "
                f"{self._quote_ansi_identifier(schema_name)}.{self._quote_ansi_identifier(table_only)}"
            )
            query_m = self._escape_m_string(query)
            return f'''let
    Source = Snowflake.Databases("{account}.snowflakecomputing.com", "{warehouse}"),
    Database = Source{{[Name="{database_name}"]}}[Data],
    Query = Value.NativeQuery(Database, "{query_m}")
in
    Query'''

        if conn in ("redshift", "amazon_redshift"):
            cluster = os.getenv("LOOKER_REDSHIFT_CLUSTER", "CLUSTER")
            database = os.getenv("LOOKER_REDSHIFT_DATABASE", "DATABASE")

            if len(table_parts) >= 2:
                schema_name = table_parts[-2]
                table_only = table_parts[-1]
            elif table_parts:
                schema_name = schema or "public"
                table_only = table_parts[0]
            else:
                schema_name = schema or "public"
                table_only = table_name

            query = (
                f"SELECT {select_expr} FROM "
                f"{self._quote_ansi_identifier(schema_name)}.{self._quote_ansi_identifier(table_only)}"
            )
            query_m = self._escape_m_string(query)
            return f'''let
    Source = AmazonRedshift.Database("{cluster}.redshift.amazonaws.com", "{database}"),
    Query = Value.NativeQuery(Source, "{query_m}")
in
    Query'''

        # SQL Server default
        server = os.getenv("LOOKER_SQL_SERVER", "SERVER")
        database = os.getenv("LOOKER_SQL_DATABASE", "DATABASE")

        if len(table_parts) >= 3:
            database = table_parts[-3]
            schema_name = table_parts[-2]
            table_only = table_parts[-1]
        elif len(table_parts) == 2:
            schema_name = table_parts[0]
            table_only = table_parts[1]
        elif table_parts:
            schema_name = schema or "dbo"
            table_only = table_parts[0]
        else:
            schema_name = schema or "dbo"
            table_only = table_name

        table_ref = (
            f"{self._quote_sql_server_identifier(schema_name)}."
            f"{self._quote_sql_server_identifier(table_only)}"
        )
        query = f"SELECT {select_expr} FROM {table_ref}"
        query_m = self._escape_m_string(query)
        return f'''let
    Source = Sql.Database("{server}", "{database}"),
    Query = Value.NativeQuery(Source, "{query_m}", null, [EnableFolding=true])
in
    Query'''
