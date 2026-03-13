"""
TMDL Generator for Power BI model output.

Generates TMDL (Tabular Model Definition Language) files from
converted Looker LookML models.
"""

import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from ..models import PbiModel, PbiTable, PbiRelationship, PbiColumn, PbiMeasure
from ..common.log_utils import log_info, log_debug, log_warning
from .template_engine import TemplateEngine


class TmdlGenerator:
    """
    Generates TMDL output files for Power BI.

    Creates the directory structure and TMDL files following
    Power BI's expected pbit format for compilation.
    """

    def __init__(
        self,
        template_dir: Optional[str] = None,
        output_encoding: str = "utf-8",
    ):
        """
        Initialize the TMDL generator.

        Args:
            template_dir: Directory containing templates.
                         Defaults to package templates directory.
            output_encoding: Encoding for output files.
        """
        if template_dir is None:
            # Use package templates directory
            template_dir = str(
                Path(__file__).parent.parent / "templates"
            )

        self.template_dir = template_dir
        self.output_encoding = output_encoding

        # Initialize template engine
        self.template_engine = TemplateEngine(template_dir)

    def _has_special_chars(self, name: str) -> bool:
        """Check if name has spaces or special characters requiring quoting."""
        return bool(re.search(r'[^a-zA-Z0-9_]', name))

    @staticmethod
    def _escape_annotation_value(value: str) -> str:
        """Escape special characters in annotation values for TMDL format."""
        if not value:
            return ""
        # Escape backslashes first, then quotes, then newlines
        escaped = value.replace('\\', '\\\\')
        escaped = escaped.replace('"', '\\"')
        escaped = escaped.replace('\n', '\\n')
        escaped = escaped.replace('\r', '\\r')
        escaped = escaped.replace('\t', '\\t')
        return escaped

    def _format_partition_expression(self, expression: str) -> str:
        """Format M code expression with proper TMDL indentation."""
        if not expression:
            return ""

        # Normalize tabs to spaces
        expr = expression.replace('\t', '        ')

        # Split into lines and add proper indentation (12 spaces = 3 tabs)
        lines = expr.split('\n')
        indented_lines = []
        for line in lines:
            stripped = line.lstrip()
            if stripped:
                indented_lines.append('            ' + stripped)
            else:
                indented_lines.append('')

        return '\n'.join(indented_lines)

    def _prepare_table_context(self, table: PbiTable) -> Dict[str, Any]:
        """Prepare context data for table template (Handlebars format)."""
        # Prepare columns
        columns_data = []
        seen_column_keys: set[str] = set()
        for col in table.columns:
            column_key = self._normalize_identifier(col.name)
            if column_key in seen_column_keys:
                log_warning(
                    f"Skipping duplicate column '{col.name}' in table '{table.name}'"
                )
                continue
            seen_column_keys.add(column_key)

            is_calculated = bool(
                getattr(col, 'is_calculated', False) or getattr(col, 'expression', None)
            )
            calc_expression = getattr(col, 'expression', None)
            # Get looker_name and formula_looker for annotations
            looker_name = getattr(col, 'looker_name', None) or getattr(col, 'original_name', None)
            formula_looker = getattr(col, 'formula_looker', None) or getattr(col, 'original_expression', None)
            col_data = {
                'source_name': col.name,
                'datatype': col.data_type.value.lower() if hasattr(col.data_type, 'value') else str(col.data_type).lower(),
                'summarize_by': col.summarize_by or 'none',
                'source_column': (col.source_column or col.name) if not is_calculated else None,
                'is_calculated': is_calculated,
                'calculated_expression': calc_expression,
                'format_string': col.format_string,
                'is_hidden': col.is_hidden,
                'data_category': getattr(col, 'data_category', None),
                'is_data_type_inferred': getattr(col, 'is_data_type_inferred', False),
                'relationship_info': getattr(col, 'relationship_info', None),
                'annotations': getattr(col, 'annotations', {}) or {},
                'looker_name': looker_name,
                'formula_looker': formula_looker,
                'formula_looker_escaped': self._escape_annotation_value(formula_looker) if formula_looker else None
            }
            columns_data.append(col_data)

        # Prepare measures
        measures_data = []
        seen_measure_keys: set[str] = set()
        for measure in table.measures:
            measure_key = self._normalize_identifier(measure.name)
            if measure_key in seen_measure_keys:
                log_debug(
                    f"Skipping duplicate measure '{measure.name}' in table '{table.name}'"
                )
                continue
            seen_measure_keys.add(measure_key)

            # Get looker_name and formula_looker for annotations
            looker_name = getattr(measure, 'looker_name', None) or getattr(measure, 'original_name', None)
            formula_looker = getattr(measure, 'formula_looker', None) or getattr(measure, 'original_expression', None)
            measure_data = {
                'source_name': measure.name,
                'expression': measure.expression,
                'format_string': measure.format_string,
                'is_hidden': measure.is_hidden,
                'looker_name': looker_name,
                'formula_looker': formula_looker,
                'formula_looker_escaped': self._escape_annotation_value(formula_looker) if formula_looker else None
            }
            measures_data.append(measure_data)

        # Prepare hierarchies (if any)
        hierarchies_data = []
        if hasattr(table, 'hierarchies') and table.hierarchies:
            for hierarchy in table.hierarchies:
                levels_data = []
                for level in hierarchy.levels:
                    levels_data.append({
                        'name': level.name,
                        'column_name': level.column_name,
                        'ordinal': getattr(level, 'ordinal', None)
                    })
                hierarchies_data.append({
                    'name': hierarchy.name,
                    'is_hidden': getattr(hierarchy, 'is_hidden', False),
                    'levels': levels_data
                })

        # Prepare partitions
        partitions_data = []
        for partition in table.partitions:
            partition_data = {
                'name': partition.name,
                'source_type': partition.source_type,
                'mode': partition.mode or 'import',
                'expression': self._format_partition_expression(partition.expression)
            }
            partitions_data.append(partition_data)

        return {
            'source_name': table.name,
            'has_spaces_or_special_chars': self._has_special_chars(table.name),
            'is_hidden': table.is_hidden,
            'columns': columns_data,
            'measures': measures_data,
            'hierarchies': hierarchies_data,
            'partitions': partitions_data,
            'has_widget_serialization': False
        }

    def _prepare_model_context(self, model: PbiModel) -> Dict[str, Any]:
        """Prepare context data for model template (Jinja2 format)."""
        return {
            'model_name': model.name or 'Model',
            'default_culture': model.culture or 'en-US',
            'time_intelligence_enabled': '0',
            'desktop_version': '2.141.1253.0 (25.03)',
            'tables': [table.name for table in model.tables]
        }

    @staticmethod
    def _normalize_identifier(value: str) -> str:
        """Normalize identifiers for tolerant matching."""
        return re.sub(r"[^a-zA-Z0-9]", "", str(value or "")).lower()

    def _build_table_column_lookup(self, tables: List[PbiTable]) -> Dict[str, Dict[str, Any]]:
        """Build canonical table/column lookup for relationship resolution."""
        lookup: Dict[str, Dict[str, Any]] = {}
        for table in tables:
            table_key = self._normalize_identifier(table.name)
            columns: Dict[str, str] = {}
            for column in table.columns:
                candidates = {column.name}
                if getattr(column, "source_column", None):
                    candidates.add(column.source_column)
                for candidate in candidates:
                    col_key = self._normalize_identifier(candidate)
                    if col_key and col_key not in columns:
                        columns[col_key] = column.name
            lookup[table_key] = {
                "table_name": table.name,
                "columns": columns,
            }
        return lookup

    def _prepare_relationship_context(
        self,
        relationships: List[PbiRelationship],
        tables: List[PbiTable],
    ) -> Dict[str, Any]:
        """Prepare context data for relationship template (Jinja2 format)."""
        rel_data = []
        seen_signatures: set[tuple[str, str, str, str, str, str, bool]] = set()
        used_ids: dict[str, int] = {}
        table_lookup = self._build_table_column_lookup(tables)

        for rel in relationships:
            from_table_info = table_lookup.get(self._normalize_identifier(rel.from_table))
            to_table_info = table_lookup.get(self._normalize_identifier(rel.to_table))
            if from_table_info is None or to_table_info is None:
                log_warning(
                    "Skipping relationship with missing table reference: "
                    f"{rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"
                )
                continue

            from_column_name = from_table_info["columns"].get(
                self._normalize_identifier(rel.from_column)
            )
            to_column_name = to_table_info["columns"].get(
                self._normalize_identifier(rel.to_column)
            )
            if from_column_name is None or to_column_name is None:
                log_warning(
                    "Skipping relationship with missing column reference: "
                    f"{rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"
                )
                continue

            from_table_name = from_table_info["table_name"]
            to_table_name = to_table_info["table_name"]
            card_value = getattr(rel.cardinality, "value", str(rel.cardinality))
            cross_value = getattr(rel.cross_filter_direction, "value", str(rel.cross_filter_direction))
            signature = (
                from_table_name,
                from_column_name,
                to_table_name,
                to_column_name,
                str(card_value),
                str(cross_value),
                bool(getattr(rel, "is_active", True)),
            )
            # Skip exact duplicates to avoid serializer merge conflicts.
            if signature in seen_signatures:
                log_warning(
                    "Skipping duplicate relationship: "
                    f"{rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"
                )
                continue
            seen_signatures.add(signature)

            # Determine cross filtering behavior
            cross_filter = 'oneDirection'
            if hasattr(rel, 'cross_filter_direction'):
                if hasattr(rel.cross_filter_direction, 'value'):
                    cross_filter = (
                        'bothDirections'
                        if rel.cross_filter_direction.value == 'both'
                        else 'oneDirection'
                    )
                elif rel.cross_filter_direction == 'both':
                    cross_filter = 'bothDirections'

            from_cardinality = "many"
            to_cardinality = "one"
            if hasattr(rel, "cardinality") and hasattr(rel.cardinality, "value"):
                if rel.cardinality.value == "one_to_many":
                    from_cardinality = "one"
                    to_cardinality = "many"
                elif rel.cardinality.value == "one_to_one":
                    from_cardinality = "one"
                    to_cardinality = "one"
                elif rel.cardinality.value == "many_to_many":
                    from_cardinality = "many"
                    to_cardinality = "many"

            rel_id = self._unique_relationship_id(rel.name, used_ids)

            rel_data.append({
                'id': rel_id,
                'from_table': from_table_name,
                'from_column': from_column_name,
                'to_table': to_table_name,
                'to_column': to_column_name,
                'cross_filtering_behavior': cross_filter,
                'from_cardinality': from_cardinality,
                'to_cardinality': to_cardinality,
                'is_active': rel.is_active if hasattr(rel, 'is_active') else True,
                'join_on_date_behavior': None
            })

        return {'relationships': rel_data}

    def _unique_relationship_id(self, raw_name: Optional[str], used_ids: Dict[str, int]) -> str:
        """Build a deterministic unique relationship identifier for TMDL."""
        if raw_name:
            base_name = re.sub(r"[^A-Za-z0-9_]", "_", raw_name).strip("_")
        else:
            base_name = str(uuid.uuid4()).replace("-", "_")

        if not base_name:
            base_name = "relationship"
        if base_name[0].isdigit():
            base_name = f"rel_{base_name}"

        key = base_name.lower()
        if key not in used_ids:
            used_ids[key] = 1
            return base_name

        used_ids[key] += 1
        return f"{base_name}_{used_ids[key]}"

    def _prepare_database_context(self, model: PbiModel) -> Dict[str, Any]:
        """Prepare context data for database template (Jinja2 format)."""
        return {
            'name': 'Model',
            'compatibility_level': 1550
        }

    def _prepare_culture_context(self, culture: str) -> Dict[str, Any]:
        """Prepare context data for culture template (Jinja2 format)."""
        return {
            'culture': culture,
            'version': '1.0.0'
        }

    def generate(
        self,
        model: PbiModel,
        output_dir: str,
    ) -> list[str]:
        """
        Generate TMDL files for a Power BI model.

        Args:
            model: Power BI model to generate
            output_dir: Output directory for TMDL files

        Returns:
            List of generated file paths
        """
        log_info(f"Generating TMDL output to: {output_dir}")

        generated_files = []
        output_path = Path(output_dir)
        pbit_path = output_path / "pbit"
        extracted_path = output_path / "extracted"

        # Create directory structure
        self._create_directory_structure(pbit_path, model.name)
        extracted_path.mkdir(parents=True, exist_ok=True)

        # Generate extracted JSON files (Tableau pattern - one file per table)
        self._generate_extracted_files(model, extracted_path)

        # Generate pbit root files
        generated_files.extend(self._generate_pbit_root_files(pbit_path, model))

        # Generate database.tmdl
        db_file = self._generate_database_file(model, pbit_path)
        if db_file:
            generated_files.append(db_file)

        # Generate model.tmdl
        model_file = self._generate_model_file(model, pbit_path)
        if model_file:
            generated_files.append(model_file)

        # Generate culture file
        culture_file = self._generate_culture_file(pbit_path, model)
        if culture_file:
            generated_files.append(culture_file)

        # Generate table files
        total_tables = len(model.tables)
        for i, table in enumerate(model.tables, 1):
            # Send per-table progress matching Tableau pattern (progress 45-50%)
            col_count = len(table.columns)
            measure_count = len(table.measures)
            progress_msg = (
                f"Generating table {i}/{total_tables}: {table.name} "
                f"({col_count} columns, {measure_count} measures)"
            )

            from ..common.websocket_client import logging_helper
            logging_helper(
                message=progress_msg,
                progress=45 + int((i / total_tables) * 5),
                message_type='info',
                phase='generation',
                options={
                    'table_name': table.name,
                    'table_index': i,
                    'total_tables': total_tables,
                    'column_count': col_count,
                    'measure_count': measure_count,
                }
            )

            table_file = self._generate_table_file(table, pbit_path, model.name)
            if table_file:
                generated_files.append(table_file)

        # Generate relationships file
        if model.relationships:
            rel_file = self._generate_relationships_file(
                model.relationships, model.tables, pbit_path, model.name
            )
            if rel_file:
                generated_files.append(rel_file)

        # Generate report files
        generated_files.extend(self._generate_report_files(pbit_path, model))

        log_info(f"Generated {len(generated_files)} TMDL files")
        return generated_files

    def _create_directory_structure(
        self,
        pbit_path: Path,
        model_name: str,
    ) -> None:
        """Create the pbit directory structure."""
        dirs = [
            pbit_path,
            pbit_path / "Model",
            pbit_path / "Model" / "tables",
            pbit_path / "Model" / "cultures",
            pbit_path / "Report",
            pbit_path / "Report" / "sections",
        ]

        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
            log_debug(f"Created directory: {dir_path}")

    def _generate_pbit_root_files(
        self,
        pbit_path: Path,
        model: PbiModel,
    ) -> list[str]:
        """Generate pbit root files."""
        generated = []
        now = datetime.now().isoformat()

        # Generate .pbixproj.json
        try:
            if self.template_engine.has_template('pbixproj'):
                content = self.template_engine.render('pbixproj', {
                    'version': '1.0',
                    'created': now,
                    'lastModified': now,
                })
                file_path = pbit_path / ".pbixproj.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate .pbixproj.json: {e}")

        # Generate DiagramLayout.json
        try:
            if self.template_engine.has_template('diagram_layout'):
                nodes = []
                for i, table in enumerate(model.tables):
                    nodes.append({
                        'location': {
                            'x': (i % 4) * 250,
                            'y': (i // 4) * 250,
                        },
                        'nodeIndex': str(i),
                        'nodeLineageTag': table.lineage_tag or str(uuid.uuid4()),
                        'size': {
                            'height': 200,
                            'width': 200,
                        },
                        'zIndex': i,
                    })
                content = self.template_engine.render('diagram_layout', {'version': '1.0', 'nodes': nodes})
                file_path = pbit_path / "DiagramLayout.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate DiagramLayout.json: {e}")

        # Generate ReportMetadata.json
        try:
            if self.template_engine.has_template('report_metadata'):
                content = self.template_engine.render('report_metadata', {})
                file_path = pbit_path / "ReportMetadata.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate ReportMetadata.json: {e}")

        # Generate ReportSettings.json
        try:
            if self.template_engine.has_template('report_settings'):
                content = self.template_engine.render('report_settings', {})
                file_path = pbit_path / "ReportSettings.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate ReportSettings.json: {e}")

        # Copy/generate Version.txt
        try:
            version_src = Path(self.template_dir) / "version.txt"
            version_dst = pbit_path / "Version.txt"
            if version_src.exists():
                shutil.copy(version_src, version_dst)
            else:
                version_dst.write_text("1.28\n", encoding=self.output_encoding)
            generated.append(str(version_dst))
        except Exception as e:
            log_warning(f"Failed to generate Version.txt: {e}")

        return generated

    def _generate_database_file(
        self,
        model: PbiModel,
        pbit_path: Path,
    ) -> Optional[str]:
        """Generate the database.tmdl file."""
        try:
            context = self._prepare_database_context(model)
            content = self.template_engine.render('database', context)

            file_path = pbit_path / "Model" / "database.tmdl"
            file_path.write_text(content, encoding=self.output_encoding)

            log_debug(f"Generated: {file_path}")
            return str(file_path)

        except Exception as e:
            log_warning(f"Failed to generate database.tmdl: {e}")
            return None

    def _generate_model_file(
        self,
        model: PbiModel,
        pbit_path: Path,
    ) -> Optional[str]:
        """Generate the model.tmdl file."""
        try:
            context = self._prepare_model_context(model)
            content = self.template_engine.render('model', context)

            file_path = pbit_path / "Model" / "model.tmdl"
            file_path.write_text(content, encoding=self.output_encoding)

            log_debug(f"Generated: {file_path}")
            return str(file_path)

        except Exception as e:
            log_warning(f"Failed to generate model.tmdl: {e}")
            return None

    def _generate_culture_file(
        self,
        pbit_path: Path,
        model: PbiModel,
    ) -> Optional[str]:
        """Generate the culture TMDL file."""
        try:
            culture = model.culture or 'en-US'
            context = self._prepare_culture_context(culture)
            content = self.template_engine.render('culture', context)

            file_path = pbit_path / "Model" / "cultures" / f"{culture}.tmdl"
            file_path.write_text(content, encoding=self.output_encoding)

            log_debug(f"Generated: {file_path}")
            return str(file_path)

        except Exception as e:
            log_warning(f"Failed to generate culture file: {e}")
            return None

    def _generate_table_file(
        self,
        table: PbiTable,
        pbit_path: Path,
        model_name: str,
    ) -> Optional[str]:
        """Generate a table TMDL file."""
        try:
            context = self._prepare_table_context(table)
            content = self.template_engine.render('table', context)

            # Sanitize table name for filename
            safe_name = self._sanitize_filename(table.name)
            file_path = pbit_path / "Model" / "tables" / f"{safe_name}.tmdl"
            file_path.write_text(content, encoding=self.output_encoding)

            log_debug(f"Generated: {file_path}")
            return str(file_path)

        except Exception as e:
            log_warning(f"Failed to generate table file for {table.name}: {e}")
            return None

    def _generate_relationships_file(
        self,
        relationships: list[PbiRelationship],
        tables: list[PbiTable],
        pbit_path: Path,
        model_name: str,
    ) -> Optional[str]:
        """Generate the relationships TMDL file."""
        try:
            context = self._prepare_relationship_context(relationships, tables)
            if not context["relationships"]:
                log_warning("No valid relationships to generate after relationship resolution")
                return None
            content = self.template_engine.render('relationship', context)

            file_path = pbit_path / "Model" / "relationships.tmdl"
            file_path.write_text(content, encoding=self.output_encoding)

            log_debug(f"Generated: {file_path}")
            return str(file_path)

        except Exception as e:
            log_warning(f"Failed to generate relationships file: {e}")
            return None

    def _generate_report_files(
        self,
        pbit_path: Path,
        model: PbiModel,
    ) -> list[str]:
        """Generate Report folder files."""
        generated = []

        # Generate Report/config.json
        try:
            if self.template_engine.has_template('config'):
                content = self.template_engine.render('config', {})
                file_path = pbit_path / "Report" / "config.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate Report/config.json: {e}")

        # Generate Report/report.json
        try:
            if self.template_engine.has_template('report'):
                content = self.template_engine.render('report', {})
                file_path = pbit_path / "Report" / "report.json"
                file_path.write_text(content, encoding=self.output_encoding)
                generated.append(str(file_path))
        except Exception as e:
            log_warning(f"Failed to generate Report/report.json: {e}")

        return generated

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """Sanitize a name for use as a filename."""
        invalid_chars = '<>:"/\\|?*'
        result = name
        for char in invalid_chars:
            result = result.replace(char, '_')
        return result

    def generate_single_table(
        self,
        table: PbiTable,
    ) -> str:
        """Generate TMDL content for a single table."""
        try:
            context = self._prepare_table_context(table)
            return self.template_engine.render('table', context)
        except Exception as e:
            log_warning(f"Failed to generate table preview: {e}")
            return f"// Error generating table: {e}"

    def generate_single_measure(
        self,
        measure: PbiMeasure,
        table_name: str,
    ) -> str:
        """Generate TMDL content for a single measure."""
        return f"measure '{measure.name}' = ```\n        {measure.expression}\n```"

    def _generate_extracted_files(
        self,
        model: PbiModel,
        extracted_path: Path,
    ) -> None:
        """
        Generate extracted JSON files matching Tableau's structure.
        Creates individual table_*.json files, calculations.json, etc.
        """
        import json

        # Generate individual table JSON files (Tableau pattern)
        for table in model.tables:
            table_data = self._prepare_table_json(table)
            table_json_path = extracted_path / f"table_{table.name}.json"
            with open(table_json_path, 'w', encoding='utf-8') as f:
                json.dump(table_data, f, indent=2)
            log_debug(f"Generated: {table_json_path}")

        # Generate calculations.json (tracks LookML → DAX conversions)
        calculations = []
        for table in model.tables:
            for measure in table.measures:
                calc_data = {
                    "TableName": table.name,
                    "FormulaCaptionLooker": getattr(measure, 'looker_name', None) or measure.name,
                    "LookerName": getattr(measure, 'looker_name', None),
                    "FormulaLooker": getattr(measure, 'formula_looker', None) or getattr(measure, 'original_expression', None),
                    "FormulaTypeLooker": "measure",
                    "PowerBIName": measure.name,
                    "FormulaDax": measure.expression,
                    "Status": "converted",
                    "DataType": "decimal",
                    "SummarizeBy": "none"
                }
                calculations.append(calc_data)
            # Include calculated columns
            for col in table.columns:
                if getattr(col, 'is_calculated', False) or getattr(col, 'expression', None):
                    calc_data = {
                        "TableName": table.name,
                        "FormulaCaptionLooker": getattr(col, 'looker_name', None) or col.name,
                        "LookerName": getattr(col, 'looker_name', None),
                        "FormulaLooker": getattr(col, 'formula_looker', None),
                        "FormulaTypeLooker": "dimension",
                        "PowerBIName": col.name,
                        "FormulaDax": getattr(col, 'expression', None) or col.source_column,
                        "Status": "converted",
                        "DataType": col.data_type.value if hasattr(col.data_type, 'value') else str(col.data_type),
                        "SummarizeBy": col.summarize_by or "none"
                    }
                    calculations.append(calc_data)

        if calculations:
            calc_json_path = extracted_path / "calculations.json"
            with open(calc_json_path, 'w', encoding='utf-8') as f:
                json.dump({"calculations": calculations}, f, indent=2)
            log_debug(f"Generated: {calc_json_path}")

        # Generate relationships.json
        if model.relationships:
            relationships_data = []
            for rel in model.relationships:
                rel_data = {
                    "name": rel.name,
                    "fromTable": rel.from_table,
                    "fromColumn": rel.from_column,
                    "toTable": rel.to_table,
                    "toColumn": rel.to_column,
                    "cardinality": rel.cardinality.value if hasattr(rel.cardinality, 'value') else str(rel.cardinality),
                    "crossFilterDirection": rel.cross_filter_direction.value if hasattr(rel.cross_filter_direction, 'value') else str(rel.cross_filter_direction),
                    "isActive": getattr(rel, 'is_active', True)
                }
                relationships_data.append(rel_data)
            rel_json_path = extracted_path / "relationships.json"
            with open(rel_json_path, 'w', encoding='utf-8') as f:
                json.dump(relationships_data, f, indent=2)
            log_debug(f"Generated: {rel_json_path}")

        # Generate partitions.json
        partitions_data = []
        for table in model.tables:
            for partition in table.partitions:
                partition_data = {
                    "tableName": table.name,
                    "partitionName": partition.name,
                    "sourceType": partition.source_type,
                    "mode": partition.mode or "import",
                    "expression": partition.expression
                }
                partitions_data.append(partition_data)
        if partitions_data:
            partitions_json_path = extracted_path / "partitions.json"
            with open(partitions_json_path, 'w', encoding='utf-8') as f:
                json.dump(partitions_data, f, indent=2)
            log_debug(f"Generated: {partitions_json_path}")

        # Generate model.json
        model_data = {
            "name": model.name,
            "culture": model.culture,
            "tables_count": len(model.tables),
            "relationships_count": len(model.relationships),
            "measures_count": sum(len(t.measures) for t in model.tables)
        }
        model_json_path = extracted_path / "model.json"
        with open(model_json_path, 'w', encoding='utf-8') as f:
            json.dump(model_data, f, indent=2)
        log_debug(f"Generated: {model_json_path}")

        log_info(f"Generated extracted files to: {extracted_path}")

    def _prepare_table_json(self, table: PbiTable) -> dict:
        """
        Prepare table data as JSON matching Tableau's table_*.json structure.
        """
        columns_data = []
        for col in table.columns:
            is_calculated = bool(
                getattr(col, 'is_calculated', False) or getattr(col, 'expression', None)
            )
            col_data = {
                "source_name": col.name,
                "pbi_datatype": col.data_type.value.lower() if hasattr(col.data_type, 'value') else str(col.data_type).lower(),
                "source_column": getattr(col, 'expression', None) if is_calculated else (col.source_column or col.name),
                "description": getattr(col, 'description', None),
                "format_string": col.format_string,
                "is_hidden": col.is_hidden,
                "summarize_by": col.summarize_by or "none",
                "dataCategory": getattr(col, 'data_category', None),
                "is_calculated": is_calculated,
                "is_data_type_inferred": getattr(col, 'is_data_type_inferred', False),
                "looker_name": getattr(col, 'looker_name', None) or getattr(col, 'original_name', None),
                "formula_looker": getattr(col, 'formula_looker', None) or getattr(col, 'original_expression', None),
                "relationship_info": getattr(col, 'relationship_info', None)
            }
            columns_data.append(col_data)

        measures_data = []
        for measure in table.measures:
            measure_data = {
                "source_name": measure.name,
                "expression": measure.expression,
                "description": getattr(measure, 'description', None),
                "format_string": measure.format_string,
                "is_hidden": measure.is_hidden,
                "looker_name": getattr(measure, 'looker_name', None) or getattr(measure, 'original_name', None),
                "formula_looker": getattr(measure, 'formula_looker', None) or getattr(measure, 'original_expression', None)
            }
            measures_data.append(measure_data)

        hierarchies_data = []
        if hasattr(table, 'hierarchies') and table.hierarchies:
            for hierarchy in table.hierarchies:
                levels_data = []
                for level in hierarchy.levels:
                    levels_data.append({
                        "name": level.name,
                        "column_name": level.column_name,
                        "ordinal": getattr(level, 'ordinal', None)
                    })
                hierarchies_data.append({
                    "name": hierarchy.name,
                    "is_hidden": getattr(hierarchy, 'is_hidden', False),
                    "levels": levels_data
                })

        partitions_data = []
        for partition in table.partitions:
            partition_data = {
                "name": partition.name,
                "source_type": partition.source_type,
                "expression": partition.expression,
                "mode": partition.mode or "import",
                "metadata": {
                    "mode": partition.mode or "import"
                }
            }
            partitions_data.append(partition_data)

        return {
            "source_name": table.name,
            "description": getattr(table, 'description', None),
            "is_hidden": table.is_hidden,
            "columns": columns_data,
            "measures": measures_data,
            "hierarchies": hierarchies_data,
            "partitions": partitions_data,
            "annotations": getattr(table, 'annotations', {}) or {}
        }
