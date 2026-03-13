"""
LookML Parser for Looker files.

Parses LookML syntax into structured objects using the lkml library.
"""

import re
from pathlib import Path
from typing import Optional, Any, Union
from dataclasses import dataclass, field

try:
    import lkml
    HAS_LKML = True
except ImportError:
    HAS_LKML = False

from ..models import (
    LookmlView,
    LookmlDimension,
    LookmlMeasure,
    LookmlFilter,
    LookmlParameter,
    LookmlModel,
    LookmlExplore,
    LookmlJoin,
)
from ..common.log_utils import log_info, log_debug, log_warning


@dataclass
class LookmlBlock:
    """Represents a parsed LookML block."""
    type: str  # view, explore, model, dimension, measure, etc.
    name: str
    properties: dict[str, Any] = field(default_factory=dict)
    children: list["LookmlBlock"] = field(default_factory=list)


class LookmlParser:
    """
    Parser for LookML files.

    Handles .lkml files including:
    - View files (.view.lkml)
    - Model files (.model.lkml)
    - Explore files
    """

    # LookML block patterns
    BLOCK_START_PATTERN = re.compile(
        r'^(\s*)(view|explore|model|dimension|dimension_group|measure|'
        r'filter|parameter|join|set|derived_table|datagroup|access_grant):\s*(\w+)?\s*\{?\s*$',
        re.MULTILINE
    )

    # Property patterns
    PROPERTY_PATTERN = re.compile(
        r'^(\s*)(\w+):\s*(.+?)\s*$',
        re.MULTILINE
    )

    # SQL block patterns (multiline)
    SQL_BLOCK_PATTERN = re.compile(
        r'(\w+):\s*(;;|""")(.*?)(;;|""")',
        re.DOTALL
    )

    def __init__(self):
        """Initialize the parser."""
        self._current_file: Optional[str] = None

    def parse_file(self, file_path: Union[str, Path]) -> list[LookmlBlock]:
        """
        Parse a LookML file.

        Args:
            file_path: Path to .lkml file

        Returns:
            List of top-level LookML blocks
        """
        path = Path(file_path)
        self._current_file = str(path)

        log_info(f"Parsing LookML file: {path.name}")

        content = path.read_text(encoding='utf-8')
        return self.parse_content(content, return_blocks=True)

    def parse_content(self, content: str, return_blocks: bool = False) -> list[Union[LookmlBlock, LookmlView]]:
        """
        Parse LookML content string.

        Args:
            content: LookML content
            return_blocks: When True, return raw LookmlBlock objects

        Returns:
            Parsed views by default, or top-level blocks when return_blocks=True
        """
        if HAS_LKML:
            blocks = self._parse_with_lkml(content)
        else:
            # Fallback to manual parsing (may have issues with complex files)
            content = self._remove_comments(content)
            blocks = self._parse_blocks(content)

        if return_blocks:
            return blocks

        # Backward-compatible convenience behavior used by tests and callers:
        # if content defines views, return parsed LookmlView objects directly.
        views = [self.parse_view(block) for block in blocks if block.type == 'view']
        if views:
            return views

        return blocks

    def _parse_with_lkml(self, content: str) -> list[LookmlBlock]:
        """Parse using the lkml library."""
        parsed = lkml.load(content)
        blocks = []

        # Extract views
        for view_data in parsed.get('views', []):
            block = LookmlBlock(
                type='view',
                name=view_data.get('name', ''),
                properties={
                    k: v for k, v in view_data.items()
                    if k not in ('name', 'dimensions', 'dimension_groups', 'measures', 'filters', 'parameters', 'sets', 'derived_table')
                },
            )
            # Add dimensions as children
            for dim in view_data.get('dimensions', []):
                block.children.append(LookmlBlock(
                    type='dimension',
                    name=dim.get('name', ''),
                    properties={k: v for k, v in dim.items() if k != 'name'},
                ))
            # Add dimension_groups as children
            for dim in view_data.get('dimension_groups', []):
                block.children.append(LookmlBlock(
                    type='dimension_group',
                    name=dim.get('name', ''),
                    properties={k: v for k, v in dim.items() if k != 'name'},
                ))
            # Add measures as children
            for measure in view_data.get('measures', []):
                block.children.append(LookmlBlock(
                    type='measure',
                    name=measure.get('name', ''),
                    properties={k: v for k, v in measure.items() if k != 'name'},
                ))
            # Add filters as children
            for flt in view_data.get('filters', []):
                block.children.append(LookmlBlock(
                    type='filter',
                    name=flt.get('name', ''),
                    properties={k: v for k, v in flt.items() if k != 'name'},
                ))
            # Add parameters as children
            for param in view_data.get('parameters', []):
                block.children.append(LookmlBlock(
                    type='parameter',
                    name=param.get('name', ''),
                    properties={k: v for k, v in param.items() if k != 'name'},
                ))
            # Add derived_table
            if 'derived_table' in view_data:
                block.children.append(LookmlBlock(
                    type='derived_table',
                    name='derived_table',
                    properties=view_data['derived_table'],
                ))
            # Add sets
            for set_data in view_data.get('sets', []):
                block.children.append(LookmlBlock(
                    type='set',
                    name=set_data.get('name', ''),
                    properties={k: v for k, v in set_data.items() if k != 'name'},
                ))
            blocks.append(block)

        # Extract explores
        for explore_data in parsed.get('explores', []):
            block = LookmlBlock(
                type='explore',
                name=explore_data.get('name', ''),
                properties={
                    k: v for k, v in explore_data.items()
                    if k not in ('name', 'joins')
                },
            )
            # Add joins as children
            for join in explore_data.get('joins', []):
                block.children.append(LookmlBlock(
                    type='join',
                    name=join.get('name', ''),
                    properties={k: v for k, v in join.items() if k != 'name'},
                ))
            blocks.append(block)

        # Extract datagroups
        for dg in parsed.get('datagroups', []):
            blocks.append(LookmlBlock(
                type='datagroup',
                name=dg.get('name', ''),
                properties={k: v for k, v in dg.items() if k != 'name'},
            ))

        # Extract includes
        for inc in parsed.get('includes', []):
            blocks.append(LookmlBlock(
                type='include',
                name=inc if isinstance(inc, str) else inc.get('include', ''),
            ))

        return blocks

    def _remove_comments(self, content: str) -> str:
        """Remove LookML comments."""
        # Remove # comments (but not inside strings)
        lines = []
        in_sql = False

        for line in content.split('\n'):
            # Track SQL blocks (;; delimited)
            if ';;' in line:
                in_sql = not in_sql

            if not in_sql and '#' in line:
                # Find # that's not in a string
                result = []
                in_string = False
                for i, char in enumerate(line):
                    if char == '"' and (i == 0 or line[i-1] != '\\'):
                        in_string = not in_string
                    elif char == '#' and not in_string:
                        break
                    result.append(char)
                line = ''.join(result)

            lines.append(line)

        return '\n'.join(lines)

    def _parse_blocks(self, content: str) -> list[LookmlBlock]:
        """Parse top-level blocks from content."""
        blocks = []
        lines = content.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # Skip empty lines
            if not stripped:
                i += 1
                continue

            # Check for block start
            block_match = re.match(
                r'^(view|explore|model|datagroup|access_grant):\s*(\w+)\s*\{?\s*$',
                stripped
            )

            if block_match:
                block_type = block_match.group(1)
                block_name = block_match.group(2)

                # Find block content
                block_content, end_idx = self._extract_block_content(lines, i)
                i = end_idx + 1

                # Parse block
                block = self._parse_block(block_type, block_name, block_content)
                blocks.append(block)
            else:
                # Check for include statements
                include_match = re.match(r'^include:\s*["\']?([^"\']+)["\']?\s*$', stripped)
                if include_match:
                    blocks.append(LookmlBlock(
                        type='include',
                        name=include_match.group(1),
                    ))
                i += 1

        return blocks

    def _extract_block_content(
        self,
        lines: list[str],
        start_idx: int,
    ) -> tuple[str, int]:
        """Extract content of a block (between { and })."""
        brace_count = 0
        content_lines = []
        found_open = False

        for i in range(start_idx, len(lines)):
            line = lines[i]

            # Count braces (not in strings)
            for char in line:
                if char == '{':
                    brace_count += 1
                    found_open = True
                elif char == '}':
                    brace_count -= 1

            if i != start_idx:
                content_lines.append(line)

            if found_open and brace_count == 0:
                return '\n'.join(content_lines), i

        return '\n'.join(content_lines), len(lines) - 1

    def _parse_block(
        self,
        block_type: str,
        block_name: str,
        content: str,
    ) -> LookmlBlock:
        """Parse a single block and its contents."""
        block = LookmlBlock(type=block_type, name=block_name)

        # Extract properties and child blocks
        lines = content.split('\n')
        i = 0

        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            if not stripped or stripped in ('{', '}'):
                i += 1
                continue

            # Check for child block
            child_match = re.match(
                r'^(dimension|dimension_group|measure|filter|parameter|'
                r'join|set|derived_table|filters):(?:\s*(\w+))?\s*\{?\s*$',
                stripped
            )

            if child_match:
                child_type = child_match.group(1)
                child_name = child_match.group(2) or child_type

                child_content, end_idx = self._extract_block_content(lines, i)
                i = end_idx + 1

                child_block = self._parse_block(child_type, child_name, child_content)
                block.children.append(child_block)
                continue

            # Check for SQL property (multiline)
            sql_match = re.match(r'^(sql\w*):\s*$', stripped)
            if sql_match:
                prop_name = sql_match.group(1)
                sql_content, end_idx = self._extract_sql_content(lines, i + 1)
                block.properties[prop_name] = sql_content
                i = end_idx + 1
                continue

            # Check for simple property
            prop_match = re.match(r'^(\w+):\s*(.+?)\s*$', stripped)
            if prop_match:
                prop_name = prop_match.group(1)
                prop_value = prop_match.group(2)
                list_props = ('timeframes', 'fields', 'drill_fields', 'suggestions', 'filters')

                # Handle SQL property that starts on same line and continues
                # across subsequent lines until a terminating `;;`.
                if prop_name.startswith('sql') and not prop_value.endswith(';;'):
                    sql_lines = [prop_value]
                    end_idx = i
                    for j in range(i + 1, len(lines)):
                        cont_line = lines[j]
                        if ';;' in cont_line:
                            sql_lines.append(cont_line.split(';;')[0])
                            end_idx = j
                            break
                        sql_lines.append(cont_line)
                        end_idx = j

                    block.properties[prop_name] = '\n'.join(sql_lines).strip()
                    i = end_idx + 1
                    continue

                # Handle multiline list values:
                # field: [
                #   a,
                #   b
                # ]
                if prop_name in list_props and prop_value.startswith('[') and ']' not in prop_value:
                    list_lines = [prop_value]
                    end_idx = i
                    for j in range(i + 1, len(lines)):
                        cont_line = lines[j].strip()
                        list_lines.append(cont_line)
                        end_idx = j
                        if ']' in cont_line:
                            break
                    prop_value = ' '.join(list_lines)
                    block.properties[prop_name] = self._parse_list(prop_value)
                    i = end_idx + 1
                    continue

                # Handle SQL on same line
                if prop_value.endswith(';;'):
                    prop_value = prop_value[:-2].strip()

                # Clean up string values
                prop_value = self._clean_value(prop_value)

                # Handle list values
                if prop_name in list_props:
                    prop_value = self._parse_list(prop_value)

                block.properties[prop_name] = prop_value

            i += 1

        return block

    def _extract_sql_content(
        self,
        lines: list[str],
        start_idx: int,
    ) -> tuple[str, int]:
        """Extract SQL content (ends with ;;)."""
        content_lines = []

        for i in range(start_idx, len(lines)):
            line = lines[i]

            if ';;' in line:
                # Get content before ;;
                content_lines.append(line.split(';;')[0])
                return '\n'.join(content_lines).strip(), i

            content_lines.append(line)

        return '\n'.join(content_lines).strip(), len(lines) - 1

    def _clean_value(self, value: str) -> str:
        """Clean a property value."""
        # Remove quotes
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        return value

    def _parse_list(self, value: str) -> list[str]:
        """Parse a LookML list value."""
        # Handle [item1, item2] format
        if value.startswith('[') and value.endswith(']'):
            value = value[1:-1]

        items = []
        for item in value.split(','):
            item = item.strip()
            item = self._clean_value(item)
            if item:
                items.append(item)

        return items

    def parse_view(self, block: LookmlBlock) -> LookmlView:
        """Convert a parsed block to a LookmlView."""
        view = LookmlView(
            name=block.name,
            sql_table_name=block.properties.get('sql_table_name'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
        )

        # Parse extends
        if 'extends' in block.properties:
            extends = block.properties['extends']
            if isinstance(extends, str):
                view.extends = [extends]
            else:
                view.extends = extends

        # Parse children
        for child in block.children:
            if child.type == 'dimension':
                view.dimensions.append(self._parse_dimension(child))
            elif child.type == 'dimension_group':
                view.dimensions.append(self._parse_dimension_group(child))
            elif child.type == 'measure':
                view.measures.append(self._parse_measure(child))
            elif child.type == 'filter':
                view.filters.append(self._parse_filter(child))
            elif child.type == 'parameter':
                view.parameters.append(self._parse_parameter(child))
            elif child.type == 'set':
                view.sets[child.name] = child.properties.get('fields', [])
            elif child.type == 'derived_table':
                view.derived_table = child.properties

        return view

    def _parse_dimension(self, block: LookmlBlock) -> LookmlDimension:
        """Parse a dimension block."""
        return LookmlDimension(
            name=block.name,
            type=block.properties.get('type', 'string'),
            sql=block.properties.get('sql'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            hidden=block.properties.get('hidden', 'no') == 'yes',
            primary_key=block.properties.get('primary_key', 'no') == 'yes',
            value_format=block.properties.get('value_format'),
            value_format_name=block.properties.get('value_format_name'),
            group_label=block.properties.get('group_label'),
            drill_fields=block.properties.get('drill_fields', []),
            suggestions=block.properties.get('suggestions', []),
        )

    def _parse_dimension_group(self, block: LookmlBlock) -> LookmlDimension:
        """Parse a dimension_group block."""
        return LookmlDimension(
            name=block.name,
            type='time',
            sql=block.properties.get('sql'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            hidden=block.properties.get('hidden', 'no') == 'yes',
            timeframes=block.properties.get('timeframes', []),
            convert_tz=block.properties.get('convert_tz', 'yes') == 'yes',
            datatype=block.properties.get('datatype'),
        )

    def _parse_measure(self, block: LookmlBlock) -> LookmlMeasure:
        """Parse a measure block."""
        parsed_filters = self._parse_measure_filters(block)
        return LookmlMeasure(
            name=block.name,
            type=block.properties.get('type', 'count'),
            sql=block.properties.get('sql'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            hidden=block.properties.get('hidden', 'no') == 'yes',
            value_format=block.properties.get('value_format'),
            value_format_name=block.properties.get('value_format_name'),
            group_label=block.properties.get('group_label'),
            drill_fields=block.properties.get('drill_fields', []),
            filters=parsed_filters,
        )

    def _parse_measure_filters(self, block: LookmlBlock) -> dict[str, str]:
        """Parse LookML measure filters into a normalized {field: value} map."""
        filters: dict[str, str] = {}
        raw_filters = block.properties.get('filters')

        if isinstance(raw_filters, dict):
            for key, value in raw_filters.items():
                if key:
                    filters[str(key).strip()] = str(value).strip()
        elif isinstance(raw_filters, list):
            for item in raw_filters:
                self._merge_filter_item(filters, item)
        elif isinstance(raw_filters, str):
            self._merge_filter_item(filters, raw_filters)

        # Parse block syntax, e.g. filters: { field: status value: "Complete" }.
        for child in block.children:
            if child.type != 'filters':
                continue
            field_name = child.properties.get('field')
            filter_value = child.properties.get('value')
            if field_name and filter_value is not None:
                filters[str(field_name).strip()] = str(filter_value).strip().strip('"').strip("'")

        return filters

    @staticmethod
    def _merge_filter_item(filters: dict[str, str], item: Any) -> None:
        """Merge one filter entry into an existing filter map."""
        if isinstance(item, dict):
            for key, value in item.items():
                if key:
                    filters[str(key).strip()] = str(value).strip()
            return

        text = str(item or "").strip()
        if not text:
            return

        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1].strip()

        if ":" not in text:
            return

        field_name, filter_value = text.split(":", 1)
        field_name = field_name.strip()
        filter_value = filter_value.strip().strip('"').strip("'")
        if field_name:
            filters[field_name] = filter_value

    def _parse_filter(self, block: LookmlBlock) -> LookmlFilter:
        """Parse a filter block."""
        return LookmlFilter(
            name=block.name,
            type=block.properties.get('type', 'string'),
            sql=block.properties.get('sql'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            default_value=block.properties.get('default_value'),
            suggestions=block.properties.get('suggestions', []),
        )

    def _parse_parameter(self, block: LookmlBlock) -> LookmlParameter:
        """Parse a parameter block."""
        return LookmlParameter(
            name=block.name,
            type=block.properties.get('type', 'string'),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            default_value=block.properties.get('default_value'),
        )

    def parse_explore(self, block: LookmlBlock) -> LookmlExplore:
        """Convert a parsed block to a LookmlExplore."""
        explore = LookmlExplore(
            name=block.name,
            view_name=block.properties.get('view_name', block.properties.get('from')),
            label=block.properties.get('label'),
            description=block.properties.get('description'),
            hidden=block.properties.get('hidden', 'no') == 'yes',
            sql_always_where=block.properties.get('sql_always_where'),
            sql_always_having=block.properties.get('sql_always_having'),
        )

        # Parse joins
        for child in block.children:
            if child.type == 'join':
                explore.joins.append(self._parse_join(child))

        return explore

    def _parse_join(self, block: LookmlBlock) -> LookmlJoin:
        """Parse a join block."""
        return LookmlJoin(
            name=block.name,
            type=block.properties.get('type', 'left_outer'),
            relationship=block.properties.get('relationship', 'many_to_one'),
            sql_on=block.properties.get('sql_on'),
            sql_foreign_key=block.properties.get('sql_foreign_key'),
            from_view=block.properties.get('from'),
            fields=block.properties.get('fields', []),
            required_joins=block.properties.get('required_joins', []),
        )

    def parse_model(self, block: LookmlBlock) -> LookmlModel:
        """Convert a parsed block to a LookmlModel."""
        model = LookmlModel(
            name=block.name,
            connection=block.properties.get('connection'),
            label=block.properties.get('label'),
        )

        # Parse explores from children
        for child in block.children:
            if child.type == 'explore':
                model.explores.append(self.parse_explore(child))

        return model
