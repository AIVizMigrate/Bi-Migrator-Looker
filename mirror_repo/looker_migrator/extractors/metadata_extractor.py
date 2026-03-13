"""
Metadata Extractor for Looker LookML files.

Extracts metadata and saves to JSON files in Tableau-aligned format.
This ensures consistency across all migrators (Tableau, SAP BO, Looker).
"""

import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..models import (
    LookmlView,
    LookmlDimension,
    LookmlMeasure,
    LookmlModel,
    LookmlExplore,
    LookmlJoin,
    LookmlProject,
    PbiModel,
    PbiTable,
    PbiRelationship,
)
from ..common.log_utils import log_info, log_debug, log_warning


class MetadataExtractor:
    """
    Extracts metadata from Looker LookML and saves in Tableau-aligned format.

    Output structure matches Tableau migrator for consistency:
    - extracted/model.json - Project/model metadata
    - extracted/table_*.json - Table metadata per view
    - extracted/calculations.json - Formulas needing conversion
    - extracted/relationships.json - Join/relationship metadata
    """

    def __init__(self, output_dir: str):
        """
        Initialize the extractor.

        Args:
            output_dir: Base output directory for migration
        """
        self.output_dir = Path(output_dir)
        self.extracted_dir = self.output_dir / "extracted"
        self.pbit_dir = self.output_dir / "pbit"
        self.source_dir = self.extracted_dir / "source"

        # Ensure directories exist
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self.pbit_dir.mkdir(parents=True, exist_ok=True)

    def save_source_files(self, file_paths: List[str]) -> List[Path]:
        """
        Save copies of source LookML files.

        Args:
            file_paths: List of source file paths

        Returns:
            List of saved file paths
        """
        saved_files = []
        self.source_dir.mkdir(parents=True, exist_ok=True)
        for file_path in file_paths:
            src = Path(file_path)
            if src.exists():
                dest = self.source_dir / src.name
                shutil.copy2(src, dest)
                saved_files.append(dest)
                log_info(f"Saved source file: {src.name}")
        return saved_files

    def save_model_metadata(self, project: LookmlProject) -> Path:
        """
        Save project/model metadata to JSON.

        Args:
            project: LookML project

        Returns:
            Path to saved JSON file
        """
        data = {
            "name": project.name,
            "connection": project.connection,
            "extraction_timestamp": datetime.now().isoformat(),
            "models_count": len(project.models),
            "views_count": len(project.views),
            "models": [
                {
                    "name": m.name,
                    "connection": m.connection,
                    "label": m.label,
                    "explores_count": len(m.explores),
                }
                for m in project.models
            ],
        }

        file_path = self.extracted_dir / "model.json"
        self._write_json(file_path, data)
        log_info(f"Saved model metadata: {file_path.name}")
        return file_path

    def save_views_metadata(
        self,
        views: List[LookmlView],
        explores: List[LookmlExplore] = None,
    ) -> List[Path]:
        """
        Save view metadata to JSON files in Tableau-aligned format.

        Each view becomes a table_*.json file with the same structure
        as Tableau migrator output.

        Args:
            views: List of LookML views
            explores: List of explores for context (joins, etc.)

        Returns:
            List of saved file paths
        """
        saved_files = []
        explores = explores or []

        for view in views:
            table_data = self._create_tableau_aligned_table_json(view, explores)

            safe_name = self._sanitize_filename(view.name)
            file_path = self.extracted_dir / f"table_{safe_name}.json"
            self._write_json(file_path, table_data)
            saved_files.append(file_path)

        log_info(f"Saved {len(views)} view metadata files")
        return saved_files

    def _create_tableau_aligned_table_json(
        self,
        view: LookmlView,
        explores: List[LookmlExplore],
    ) -> Dict[str, Any]:
        """
        Create table JSON in Tableau-aligned format.

        This matches the structure of Tableau migrator's table_*.json files.
        """
        # Prepare columns from dimensions
        columns = []
        for dim in view.dimensions:
            pbi_datatype = self._map_lookml_type_to_pbi(dim.type)
            is_calculation = self._is_calculation(dim.sql)

            col_data = {
                "source_name": dim.name,
                "pbi_datatype": pbi_datatype,
                "source_column": dim.sql or dim.name,
                "description": dim.description or dim.label or f"Dimension from {view.name}",
                "format_string": self._get_format_string(pbi_datatype, dim.value_format_name),
                "is_hidden": dim.hidden,
                "summarize_by": "none",
                "dataCategory": self._get_data_category(dim),
                "is_calculated": is_calculation,
                "is_data_type_inferred": False,
                # Looker specific fields (equivalent to Tableau's tableau_name/formula_tableau)
                "looker_name": dim.name,
                "formula_looker": dim.sql,
                "relationship_info": None,
            }
            columns.append(col_data)

        # Prepare measures
        measures = []
        for measure in view.measures:
            measure_data = {
                "source_name": measure.name,
                "expression": measure.sql or "",
                "description": measure.description or measure.label or f"Measure from {view.name}",
                "format_string": self._get_format_string("double", measure.value_format_name),
                "is_hidden": measure.hidden,
                # Looker specific fields
                "looker_name": measure.name,
                "formula_looker": measure.sql,
                "measure_type": measure.type,
            }
            measures.append(measure_data)

        # Prepare partition (M-query source)
        partitions = []
        if view.sql_table_name:
            partition_data = {
                "name": f"{view.name}_Partition",
                "source_type": "m",
                "expression": self._generate_mquery_expression(view),
                "mode": "import",
                "metadata": {
                    "table_name": view.sql_table_name,
                    "column_count": len(view.dimensions),
                    "mode": "import",
                },
            }
            partitions.append(partition_data)
        elif view.derived_table:
            partition_data = {
                "name": f"{view.name}_Partition",
                "source_type": "m",
                "expression": self._generate_derived_table_mquery(view),
                "mode": "import",
                "metadata": {
                    "derived_table": True,
                    "column_count": len(view.dimensions),
                    "mode": "import",
                },
            }
            partitions.append(partition_data)

        return {
            "source_name": view.name,
            "description": view.description or view.label or f"View from Looker: {view.name}",
            "is_hidden": False,
            "columns": columns,
            "measures": measures,
            "hierarchies": [],  # Looker doesn't have hierarchies like SAP BO
            "partitions": partitions,
            "annotations": {},
        }

    def _map_lookml_type_to_pbi(self, lookml_type: str) -> str:
        """Map LookML data types to Power BI data types."""
        type_mapping = {
            "string": "string",
            "number": "double",
            "int": "int64",
            "integer": "int64",
            "float": "double",
            "double": "double",
            "decimal": "decimal",
            "yesno": "boolean",
            "boolean": "boolean",
            "date": "dateTime",
            "datetime": "dateTime",
            "timestamp": "dateTime",
            "time": "dateTime",
            "zipcode": "string",
            "location": "string",
            "tier": "string",
            "duration": "double",
            "distance": "double",
        }
        return type_mapping.get(lookml_type.lower(), "string")

    def _get_format_string(self, pbi_type: str, value_format_name: str = None) -> str:
        """Get Power BI format string based on data type and Looker format."""
        if value_format_name:
            format_mapping = {
                "usd": "$#,##0.00",
                "usd_0": "$#,##0",
                "decimal_0": "0",
                "decimal_1": "0.0",
                "decimal_2": "0.00",
                "percent_0": "0%",
                "percent_1": "0.0%",
                "percent_2": "0.00%",
            }
            return format_mapping.get(value_format_name.lower(), "#,##0.00")

        type_formats = {
            "int64": "0",
            "double": "#,##0.00",
            "decimal": "#,##0.00",
            "dateTime": "Long Date",
            "string": "",
            "boolean": "",
        }
        return type_formats.get(pbi_type, "")

    def _get_data_category(self, dim: LookmlDimension) -> Optional[str]:
        """Determine data category from dimension type."""
        if dim.type == "zipcode":
            return "PostalCode"
        elif dim.type == "location":
            return "Geography"
        elif "latitude" in dim.name.lower():
            return "Latitude"
        elif "longitude" in dim.name.lower():
            return "Longitude"
        elif "city" in dim.name.lower():
            return "City"
        elif "state" in dim.name.lower():
            return "StateOrProvince"
        elif "country" in dim.name.lower():
            return "Country"
        return None

    def _is_calculation(self, sql: str) -> bool:
        """Check if SQL expression is a calculation vs simple column reference."""
        if not sql:
            return False

        # Simple column reference: ${TABLE}.column_name
        if re.match(r'^\$\{TABLE\}\.\w+$', sql.strip()):
            return False

        # Contains functions, operators, or complex expressions
        if any(x in sql.upper() for x in ['(', ')', '+', '-', '*', '/', 'CASE', 'IF', 'CONCAT', 'COALESCE']):
            return True

        # References other fields: ${field_name}
        if re.search(r'\$\{(?!TABLE)\w+\}', sql):
            return True

        return False

    def _generate_mquery_expression(self, view: LookmlView) -> str:
        """Generate M-query expression for table partition."""
        table_name = view.sql_table_name or view.name

        # Parse BigQuery table reference
        if '`' in table_name:
            # BigQuery format: `project.dataset.table`
            clean_name = table_name.replace('`', '')
            parts = clean_name.split('.')
            if len(parts) >= 3:
                project, dataset, table = parts[-3], parts[-2], parts[-1]
                return f'''let
    Source = GoogleBigQuery.Database(),
    Project = Source{{[Name="{project}"]}}[Data],
    Dataset = Project{{[Name="{dataset}"]}}[Data],
    Table = Dataset{{[Name="{table}"]}}[Data]
in
    Table'''

        # Generic SQL table
        return f'''let
    Source = Sql.Database("SERVER", "DATABASE"),
    Table = Source{{[Schema="dbo", Item="{table_name}"]}}[Data]
in
    Table'''

    def _generate_derived_table_mquery(self, view: LookmlView) -> str:
        """Generate M-query for derived table (SQL-based)."""
        sql = view.derived_table.get('sql', '') if view.derived_table else ''

        return f'''let
    Source = Sql.Database("SERVER", "DATABASE"),
    Query = Value.NativeQuery(Source, "{sql.replace('"', '""')}")
in
    Query'''

    def save_explores_metadata(self, explores: List[LookmlExplore]) -> Path:
        """
        Save explores metadata to JSON.

        Args:
            explores: List of LookML explores

        Returns:
            Path to saved JSON file
        """
        data = {
            "explores_count": len(explores),
            "extraction_timestamp": datetime.now().isoformat(),
            "explores": [
                {
                    "name": e.name,
                    "view_name": e.view_name or e.name,
                    "label": e.label,
                    "description": e.description,
                    "hidden": e.hidden,
                    "joins_count": len(e.joins),
                    "joins": [
                        {
                            "name": j.name,
                            "type": j.type,
                            "relationship": j.relationship,
                            "sql_on": j.sql_on,
                        }
                        for j in e.joins
                    ],
                }
                for e in explores
            ],
        }

        file_path = self.extracted_dir / "explores.json"
        self._write_json(file_path, data)
        log_info(f"Saved {len(explores)} explores metadata")
        return file_path

    def save_relationships_metadata(
        self,
        explores: List[LookmlExplore],
        views: List[LookmlView],
    ) -> Path:
        """
        Save relationships (joins) metadata to JSON.

        Converts Looker joins to Power BI relationship format.

        Args:
            explores: List of explores with joins
            views: List of views for column lookup

        Returns:
            Path to saved JSON file
        """
        relationships = []
        view_map = {v.name: v for v in views}

        for explore in explores:
            base_view = explore.view_name or explore.name

            for join in explore.joins:
                rel_data = self._convert_join_to_relationship(
                    join, base_view, explore.name, view_map
                )
                if rel_data:
                    relationships.append(rel_data)

        data = {
            "relationships_count": len(relationships),
            "extraction_timestamp": datetime.now().isoformat(),
            "relationships": relationships,
        }

        file_path = self.extracted_dir / "relationships.json"
        self._write_json(file_path, data)
        log_info(f"Saved {len(relationships)} relationships metadata")
        return file_path

    def _convert_join_to_relationship(
        self,
        join: LookmlJoin,
        base_view: str,
        explore_name: str,
        view_map: Dict[str, LookmlView],
    ) -> Optional[Dict[str, Any]]:
        """Convert a Looker join to Power BI relationship format."""
        if not join.sql_on:
            return None

        # Parse sql_on to extract column references
        # Pattern: ${view.column} = ${other_view.column}
        matches = re.findall(r'\$\{(\w+)\.(\w+)\}', join.sql_on)

        if len(matches) >= 2:
            from_view, from_col = matches[0]
            to_view, to_col = matches[1]

            # Map relationship type
            cardinality_map = {
                "one_to_one": "OneToOne",
                "one_to_many": "OneToMany",
                "many_to_one": "ManyToOne",
                "many_to_many": "ManyToMany",
            }

            return {
                "name": f"{from_view}_{to_view}",
                "from_table": from_view,
                "from_column": from_col,
                "to_table": to_view,
                "to_column": to_col,
                "cardinality": cardinality_map.get(join.relationship, "ManyToOne"),
                "cross_filter_direction": "Single",
                "is_active": True,
                "explore": explore_name,
                "original_sql_on": join.sql_on,
            }

        return None

    def save_conversion_mapping(
        self,
        views: List[LookmlView],
        converted_measures: List[dict] = None,
    ) -> Path:
        """
        Save mapping between Looker formulas and converted DAX.
        FORMAT: Aligned with Tableau's calculations.json structure.

        IMPORTANT: Only includes actual calculations (with functions/operators),
        NOT simple column references like ${TABLE}.column_name.

        Args:
            views: LookML views with dimensions and measures
            converted_measures: Converted measures with DAX expressions

        Returns:
            Path to saved JSON file
        """
        converted_measures = converted_measures or []
        converted_by_name = {m.get("name", ""): m for m in converted_measures}

        data = {
            "calculations": [],
        }

        skipped_count = 0
        for view in views:
            # Process dimensions with calculations
            for dim in view.dimensions:
                if not self._is_actual_calculation(dim.sql, "dimension"):
                    skipped_count += 1
                    continue

                converted = converted_by_name.get(dim.name, {})

                calc_data = {
                    # Aligned with Tableau's calculations.json structure
                    "TableName": view.name,
                    "FormulaCaptionLooker": dim.label or dim.name,
                    "LookerName": dim.name,
                    "FormulaLooker": dim.sql or "",
                    "FormulaTypeLooker": "calculated_column",
                    "PowerBIName": converted.get("name", dim.name),
                    "FormulaDax": converted.get("expression", ""),
                    "Status": "converted" if converted else "pending",
                    "DataType": self._map_lookml_type_to_pbi(dim.type),
                    "SummarizeBy": "none",
                    # Looker specific
                    "GroupLabel": dim.group_label,
                }
                data["calculations"].append(calc_data)

            # Process measures (all measures are calculations)
            for measure in view.measures:
                if not self._is_actual_calculation(measure.sql, measure.type):
                    skipped_count += 1
                    continue

                converted = converted_by_name.get(measure.name, {})

                calc_data = {
                    "TableName": view.name,
                    "FormulaCaptionLooker": measure.label or measure.name,
                    "LookerName": measure.name,
                    "FormulaLooker": self._build_measure_formula(measure),
                    "FormulaTypeLooker": "measure",
                    "PowerBIName": converted.get("name", measure.name),
                    "FormulaDax": converted.get("expression", ""),
                    "Status": "converted" if converted else "pending",
                    "DataType": "double",
                    "SummarizeBy": "sum",
                    # Looker specific
                    "MeasureType": measure.type,
                    "GroupLabel": measure.group_label,
                }
                data["calculations"].append(calc_data)

        file_path = self.extracted_dir / "calculations.json"
        self._write_json(file_path, data)
        log_info(f"Saved conversion mapping: {len(data['calculations'])} calculations ({skipped_count} simple refs skipped)")
        return file_path

    def _is_actual_calculation(self, sql: str, field_type: str) -> bool:
        """
        Determine if a field is an actual calculation needing conversion.

        Args:
            sql: The SQL expression
            field_type: The field type (dimension, measure type like sum/count)

        Returns:
            True if it's an actual calculation
        """
        # Measures with aggregation types are always calculations
        if field_type in ('sum', 'count', 'count_distinct', 'average', 'min', 'max',
                          'sum_distinct', 'average_distinct', 'list', 'percentile', 'median'):
            return True

        if not sql:
            return False

        sql = sql.strip()

        # Simple column reference: ${TABLE}.column_name
        if re.match(r'^\$\{TABLE\}\.\w+$', sql):
            return False

        # Check for function calls
        function_patterns = [
            r'\bSUM\s*\(', r'\bCOUNT\s*\(', r'\bAVG\s*\(', r'\bMIN\s*\(', r'\bMAX\s*\(',
            r'\bCASE\b', r'\bWHEN\b', r'\bIF\s*\(', r'\bIIF\s*\(',
            r'\bCONCAT\s*\(', r'\bCOALESCE\s*\(', r'\bNULLIF\s*\(',
            r'\bDATE_DIFF\b', r'\bTIMESTAMP_DIFF\b', r'\bDATE_ADD\b',
            r'\bYEAR\s*\(', r'\bMONTH\s*\(', r'\bDAY\s*\(',
        ]

        for pattern in function_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True

        # Check for arithmetic operators
        if re.search(r'[\+\-\*/]', sql):
            return True

        # Check for field references (not just ${TABLE}.column)
        if re.search(r'\$\{(?!TABLE)\w+\}', sql):
            return True

        return False

    def _build_measure_formula(self, measure: LookmlMeasure) -> str:
        """Build the full Looker measure formula including aggregation type."""
        measure_type = measure.type.upper()
        sql = measure.sql or "${TABLE}.id"

        if measure.type == "count":
            return f"COUNT({sql})" if sql and sql != "${TABLE}.id" else "COUNT(*)"
        elif measure.type == "count_distinct":
            return f"COUNT(DISTINCT {sql})"
        elif measure.type in ("sum", "sum_distinct"):
            return f"SUM({sql})"
        elif measure.type in ("average", "average_distinct"):
            return f"AVG({sql})"
        elif measure.type == "min":
            return f"MIN({sql})"
        elif measure.type == "max":
            return f"MAX({sql})"
        elif measure.type == "number":
            return sql  # Custom calculation
        else:
            return sql

    def save_config(self, settings: dict) -> Path:
        """
        Save migration configuration settings.

        Args:
            settings: Migration settings dictionary

        Returns:
            Path to saved JSON file
        """
        data = {
            "extraction_timestamp": datetime.now().isoformat(),
            **settings,
        }

        file_path = self.extracted_dir / "config.json"
        self._write_json(file_path, data)
        log_info(f"Saved config: {file_path.name}")
        return file_path

    def save_pbi_model_metadata(self, model: PbiModel) -> Path:
        """
        Save Power BI model metadata.

        Args:
            model: Power BI model

        Returns:
            Path to saved JSON file
        """
        data = {
            "name": model.name,
            "extraction_timestamp": datetime.now().isoformat(),
            "tables_count": len(model.tables),
            "relationships_count": len(model.relationships),
            "culture": model.culture,
            "tables": [t.name for t in model.tables],
        }

        file_path = self.extracted_dir / "pbi_model.json"
        self._write_json(file_path, data)
        log_info(f"Saved PBI model metadata: {file_path.name}")
        return file_path

    def save_pbi_tables_metadata(self, tables: List[PbiTable]) -> List[Path]:
        """
        Save Power BI table metadata files.

        Args:
            tables: List of Power BI tables

        Returns:
            List of saved file paths
        """
        saved_files = []

        for table in tables:
            data = {
                "name": table.name,
                "columns_count": len(table.columns),
                "measures_count": len(table.measures),
                "is_hidden": table.is_hidden,
                "columns": [
                    {
                        "name": c.name,
                        "data_type": c.data_type.value if hasattr(c.data_type, 'value') else str(c.data_type),
                        "source_column": c.source_column,
                        "is_hidden": c.is_hidden,
                        "format_string": c.format_string,
                        "summarize_by": c.summarize_by,
                    }
                    for c in table.columns
                ],
                "measures": [
                    {
                        "name": m.name,
                        "expression": m.expression,
                        "format_string": m.format_string,
                        "is_hidden": m.is_hidden,
                    }
                    for m in table.measures
                ],
            }

            safe_name = self._sanitize_filename(table.name)
            file_path = self.extracted_dir / f"pbi_table_{safe_name}.json"
            self._write_json(file_path, data)
            saved_files.append(file_path)

        log_info(f"Saved {len(tables)} PBI table metadata files")
        return saved_files

    def save_pbi_relationships_metadata(
        self,
        relationships: List[PbiRelationship],
    ) -> Path:
        """
        Save Power BI relationships metadata.

        Args:
            relationships: List of Power BI relationships

        Returns:
            Path to saved JSON file
        """
        data = {
            "relationships_count": len(relationships),
            "extraction_timestamp": datetime.now().isoformat(),
            "relationships": [
                {
                    "name": r.name,
                    "from_table": r.from_table,
                    "from_column": r.from_column,
                    "to_table": r.to_table,
                    "to_column": r.to_column,
                    "cardinality": r.cardinality.value if hasattr(r.cardinality, 'value') else str(r.cardinality),
                    "cross_filter_direction": r.cross_filter_direction.value if hasattr(r.cross_filter_direction, 'value') else str(r.cross_filter_direction),
                    "is_active": r.is_active,
                }
                for r in relationships
            ],
        }

        file_path = self.extracted_dir / "pbi_relationships.json"
        self._write_json(file_path, data)
        log_info(f"Saved {len(relationships)} relationships metadata")
        return file_path

    def _write_json(self, file_path: Path, data: Any) -> None:
        """Write data to JSON file with pretty formatting."""
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """Sanitize a name for use as filename."""
        if not name:
            return "unnamed"

        # Replace problematic characters
        result = re.sub(r'[<>:"/\\|?*]', '_', name)
        result = result.strip('. ')

        return result if result else "unnamed"
