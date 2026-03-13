"""
Template engine for rendering Power BI project templates.
Supports both Handlebars (for table templates) and Jinja2 (for other templates).
"""
import os
from pathlib import Path
import logging
from typing import Dict, Any, Optional

from pybars import Compiler
from jinja2 import Environment, FileSystemLoader


class TemplateEngine:
    """Template engine for rendering Power BI project templates."""

    def __init__(self, template_directory: str):
        """Initialize template engine with template directory."""
        self.logger = logging.getLogger(__name__)

        # Convert to Path object
        if isinstance(template_directory, str):
            self.template_directory = Path(template_directory)
        else:
            self.template_directory = template_directory

        self.logger.info(f"Using template directory: {self.template_directory}")

        # Initialize template cache
        self.templates = {}
        self.template_info = {}
        self.handlebars_compiler = Compiler()

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.template_directory)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True
        )

        # Load all templates
        self._load_templates()

    def _load_templates(self):
        """Load all template files."""
        if not self.template_directory.exists():
            raise FileNotFoundError(f"Template directory not found: {self.template_directory}")

        # Define which templates use Handlebars
        handlebars_templates = ['table', 'parameters']

        template_files = {
            # Model templates
            'database': {'filename': 'database.tmdl', 'path': 'Model', 'target_filename': 'database.tmdl'},
            'table': {'filename': 'Table.tmdl', 'path': 'Model/tables', 'target_filename': '{table_name}.tmdl'},
            'parameters': {'filename': 'parameters.tmdl', 'path': 'Model/tables', 'target_filename': '{table_name}.tmdl'},
            'relationship': {'filename': 'relationship.tmdl', 'path': 'Model', 'target_filename': 'relationships.tmdl'},
            'model': {'filename': 'model.tmdl', 'path': 'Model', 'target_filename': 'model.tmdl'},
            'culture': {'filename': 'culture.tmdl', 'path': 'Model/cultures', 'target_filename': '{culture_name}.tmdl'},
            'expressions': {'filename': 'expressions.tmdl', 'path': 'Model', 'target_filename': 'expressions.tmdl'},

            # Project templates
            'pbixproj': {'filename': 'pbixproj.json', 'path': '', 'target_filename': '.pbixproj.json'},

            # Report templates
            'report': {'filename': 'report.json', 'path': 'Report', 'target_filename': 'report.json'},
            'config': {'filename': 'report.config.json', 'path': 'Report', 'target_filename': 'config.json'},
            'report_metadata': {'filename': 'report.metadata.json', 'path': '', 'target_filename': 'ReportMetadata.json'},
            'report_settings': {'filename': 'report.settings.json', 'path': '', 'target_filename': 'ReportSettings.json'},

            'diagram_layout': {'filename': 'diagram.layout.json', 'path': '', 'target_filename': 'DiagramLayout.json'},

            # Metadata templates
            'version': {'filename': 'version.txt', 'path': '', 'target_filename': 'Version.txt'}
        }

        # Load each template
        for template_name, template_info in template_files.items():
            template_path = self.template_directory / template_info['filename']
            if not template_path.exists():
                self.logger.warning(f"Template file not found: {template_path}")
                continue

            with open(template_path, 'r', encoding='utf-8') as f:
                template_content = f.read()

            if template_name in handlebars_templates:
                # Compile handlebars template
                self.templates[template_name] = self.handlebars_compiler.compile(template_content)
            else:
                # Compile Jinja2 template
                self.templates[template_name] = self.jinja_env.from_string(template_content)

        # Store the template info for later use
        self.template_info = template_files

    def get_template_info(self, template_name: str) -> Dict[str, Any]:
        """Get information about a template."""
        if template_name not in self.template_info:
            raise ValueError(f"Template info not found: {template_name}")
        return self.template_info[template_name]

    def render(self, template_name: str, context: Dict[str, Any]) -> str:
        """Render a template with the given context."""
        if template_name not in self.templates:
            raise ValueError(f"Template not found: {template_name}")

        template = self.templates[template_name]

        if template_name in ['table', 'parameters']:
            # Use handlebars for table and parameters templates
            result = template(context)
            return result
        else:
            # Use Jinja2 for other templates
            return template.render(**context)

    def has_template(self, template_name: str) -> bool:
        """Check if a template exists."""
        return template_name in self.templates
