"""
Project Parser for Looker LookML projects.

Parses entire LookML project directories.
"""

import re
from pathlib import Path
from typing import Optional, Union

from ..models import (
    LookmlProject,
    LookmlModel,
    LookmlView,
    LookmlExplore,
)
from ..common.log_utils import log_info, log_debug, log_warning
from .lookml_parser import LookmlParser, LookmlBlock


class ProjectParser:
    """
    Parser for complete LookML projects.

    Handles project directory structure:
    - *.model.lkml files
    - *.view.lkml files
    - Manifest file
    - Include resolution
    """

    def __init__(self):
        """Initialize the project parser."""
        self.lookml_parser = LookmlParser()
        self._views: dict[str, LookmlView] = {}
        self._models: dict[str, LookmlModel] = {}
        self._includes: dict[str, list[str]] = {}

    def parse(self, project_path: Union[str, Path]) -> LookmlProject:
        """
        Parse a complete LookML project.

        Args:
            project_path: Path to project directory

        Returns:
            Parsed LookmlProject
        """
        path = Path(project_path)
        log_info(f"Parsing LookML project: {path}")

        if not path.is_dir():
            raise ValueError(f"Project path must be a directory: {path}")

        # Initialize project
        project = LookmlProject(
            name=path.name,
            project_path=str(path),
        )

        # Find and parse all LookML files
        self._parse_all_files(path)

        # Build project from parsed components
        project.views = list(self._views.values())
        project.models = list(self._models.values())

        # Extract connection from first model
        if project.models:
            project.connection = project.models[0].connection

        log_info(
            f"Parsed project: {len(project.views)} views, "
            f"{len(project.models)} models"
        )

        return project

    def _parse_all_files(self, project_path: Path) -> None:
        """Parse all LookML files in the project."""
        # Reset state
        self._views = {}
        self._models = {}
        self._includes = {}

        # Find all .lkml files
        lkml_files = list(project_path.rglob("*.lkml"))
        log_debug(f"Found {len(lkml_files)} LookML files")

        # Parse view files first
        for file_path in lkml_files:
            if '.view.lkml' in file_path.name or '.view.lookml' in file_path.name:
                self._parse_view_file(file_path)

        # Parse model files
        for file_path in lkml_files:
            if '.model.lkml' in file_path.name or '.model.lookml' in file_path.name:
                self._parse_model_file(file_path)

        # Parse standalone explore files
        for file_path in lkml_files:
            if '.explore.lkml' in file_path.name:
                self._parse_explore_file(file_path)

    def _parse_view_file(self, file_path: Path) -> None:
        """Parse a view file."""
        log_debug(f"Parsing view file: {file_path.name}")

        try:
            blocks = self.lookml_parser.parse_file(file_path)

            for block in blocks:
                if block.type == 'view':
                    view = self.lookml_parser.parse_view(block)
                    self._views[view.name] = view
                    log_debug(f"  Parsed view: {view.name}")

        except Exception as e:
            log_warning(f"Failed to parse {file_path}: {e}")

    def _parse_model_file(self, file_path: Path) -> None:
        """Parse a model file."""
        log_debug(f"Parsing model file: {file_path.name}")

        try:
            blocks = self.lookml_parser.parse_file(file_path)
            file_content = file_path.read_text(encoding="utf-8")

            # Extract model name from filename
            model_name = file_path.name.replace('.model.lkml', '').replace('.model.lookml', '')
            model = LookmlModel(name=model_name, file_path=str(file_path))
            top_level_props = self._extract_top_level_model_properties(file_content)
            if top_level_props.get("connection"):
                model.connection = top_level_props["connection"]
            if top_level_props.get("label"):
                model.label = top_level_props["label"]

            for block in blocks:
                if block.type == 'model':
                    # Update model properties
                    model.connection = block.properties.get('connection', model.connection)
                    model.label = block.properties.get('label', model.label)

                elif block.type == 'explore':
                    explore = self.lookml_parser.parse_explore(block)
                    model.explores.append(explore)
                    log_debug(f"  Parsed explore: {explore.name}")

                elif block.type == 'include':
                    model.includes.append(block.name)

            self._models[model_name] = model

        except Exception as e:
            log_warning(f"Failed to parse {file_path}: {e}")

    @staticmethod
    def _extract_top_level_model_properties(content: str) -> dict[str, str]:
        """
        Extract top-level properties from a model file header.

        Model files commonly declare `connection` and `label` at the top level
        (outside any `model: {}` block). Capture these before the first block.
        """
        first_block_match = re.search(
            r'^\s*(explore|datagroup|access_grant|persist_with)\s*:',
            content,
            re.MULTILINE,
        )
        header = content[:first_block_match.start()] if first_block_match else content

        properties: dict[str, str] = {}
        for key in ("connection", "label"):
            match = re.search(rf'^\s*{key}\s*:\s*(.+?)\s*$', header, re.MULTILINE)
            if not match:
                continue
            value = match.group(1).split("#", 1)[0].strip()
            if value.endswith(";;"):
                value = value[:-2].strip()
            if (
                (value.startswith('"') and value.endswith('"'))
                or (value.startswith("'") and value.endswith("'"))
            ):
                value = value[1:-1]
            if value:
                properties[key] = value

        return properties

    def _parse_explore_file(self, file_path: Path) -> None:
        """Parse a standalone explore file."""
        log_debug(f"Parsing explore file: {file_path.name}")

        try:
            blocks = self.lookml_parser.parse_file(file_path)

            for block in blocks:
                if block.type == 'explore':
                    explore = self.lookml_parser.parse_explore(block)

                    # Add to first model or create default
                    if self._models:
                        first_model = list(self._models.values())[0]
                        first_model.explores.append(explore)
                    else:
                        default_model = LookmlModel(name="default")
                        default_model.explores.append(explore)
                        self._models["default"] = default_model

        except Exception as e:
            log_warning(f"Failed to parse {file_path}: {e}")

    def parse_single_view(self, file_path: Union[str, Path]) -> Optional[LookmlView]:
        """
        Parse a single view file.

        Args:
            file_path: Path to .view.lkml file

        Returns:
            Parsed LookmlView or None
        """
        path = Path(file_path)

        try:
            blocks = self.lookml_parser.parse_file(path)

            for block in blocks:
                if block.type == 'view':
                    return self.lookml_parser.parse_view(block)

        except Exception as e:
            log_warning(f"Failed to parse view {path}: {e}")

        return None

    def get_view(self, view_name: str) -> Optional[LookmlView]:
        """Get a parsed view by name."""
        return self._views.get(view_name)

    def get_model(self, model_name: str) -> Optional[LookmlModel]:
        """Get a parsed model by name."""
        return self._models.get(model_name)

    def resolve_view_extends(self, view: LookmlView) -> LookmlView:
        """
        Resolve view extends (inheritance).

        Merges properties from parent views.
        """
        if not view.extends:
            return view

        # Merge from parent views
        for parent_name in view.extends:
            parent = self._views.get(parent_name)
            if parent:
                # Merge dimensions (child overrides parent)
                parent_dims = {d.name: d for d in parent.dimensions}
                for dim in view.dimensions:
                    parent_dims[dim.name] = dim
                view.dimensions = list(parent_dims.values())

                # Merge measures
                parent_measures = {m.name: m for m in parent.measures}
                for measure in view.measures:
                    parent_measures[measure.name] = measure
                view.measures = list(parent_measures.values())

                # Merge sets
                for set_name, fields in parent.sets.items():
                    if set_name not in view.sets:
                        view.sets[set_name] = fields

                # Inherit sql_table_name if not set
                if not view.sql_table_name and parent.sql_table_name:
                    view.sql_table_name = parent.sql_table_name

                # Inherit derived_table if not set
                if not view.derived_table and parent.derived_table:
                    view.derived_table = parent.derived_table

        return view
