# Looker Migrator

Migrate Looker LookML projects to Power BI TMDL format.

## Overview

Looker Migrator converts Looker LookML model files (.lkml) to Power BI's Tabular Model Definition Language (TMDL) format. This enables migration of Looker data models, including views, dimensions, measures, and explores, to Power BI semantic models.

## Features

- **LookML Parsing**: Parse LookML view files, model files, and explores
- **Expression Conversion**: Convert Looker SQL and field references to DAX
- **Relationship Mapping**: Convert Looker explore joins to Power BI relationships
- **TMDL Generation**: Generate valid TMDL files using Jinja2 templates
- **Validation**: Validate DAX expressions, relationships, and TMDL output
- **Batch Processing**: Process multiple projects or views in batch
- **PBIT Compilation**: Optional compilation to .pbit format
- **Looker-Aligned Architecture Layer**: `looker_migrator.main` frontend facade with
  explicit LookML project/view orchestration and `output/pbit`, `output/extracted`,
  and `output/source` contracts.

## Looker-Aligned API Layer

This repository exposes a frontend-compatible facade directly from `looker_migrator.main`
while using Looker-native terminology and extracted artifacts.

Example:

```python
from looker_migrator.main import migrate_single_project

result = migrate_single_project(
    filename="./input/sample_project",
    output_dir="./output",
)

print(result["pbit_dir"])
print(result["validation"])
```

## Installation

```bash
# Clone the repository
git clone https://github.com/Codehive-Inc/Bi-Migrator-Looker.git
cd Bi-Migrator-Looker

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements-dev.txt
```

## Quick Start

### Command Line

```bash
# Migrate a LookML project
python -m looker_migrator.main /path/to/looker/project -o ./output

# Migrate a single view file
python -m looker_migrator.main /path/to/view.view.lkml -o ./output

# With custom model name and verbose output
python -m looker_migrator.main /path/to/project -o ./output -n MyModel -v
```

### Python API

```python
from looker_migrator import migrate_lookml_project, Settings

# Simple migration
result = migrate_lookml_project(
    project_path="./my_looker_project",
    output_dir="./output",
    model_name="MyPowerBIModel",
)

if result.success:
    print(f"Migration successful!")
    print(f"Tables: {result.tables_count}")
    print(f"Measures: {result.measures_count}")
    print(f"Output: {result.output_path}")
else:
    for error in result.errors:
        print(f"Error: {error.message}")
```

### With Custom Settings

```python
from looker_migrator import LookerMigrator, Settings

settings = Settings()
settings.parser.resolve_extends = True
settings.converter.convert_derived_tables = True
settings.generator.culture = "en-US"

migrator = LookerMigrator(settings=settings)
result = migrator.migrate_project(
    project_path="./my_project",
    output_dir="./output",
)
```

## Supported Conversions

### LookML Elements

| LookML Element | Power BI Element |
|---------------|------------------|
| View | Table |
| Dimension | Column |
| Measure | Measure |
| Explore | Model (relationships) |
| Join | Relationship |
| Derived Table | Power Query partition |

### Measure Types

| Looker Type | DAX Equivalent |
|-------------|----------------|
| count | COUNTROWS |
| count_distinct | DISTINCTCOUNT |
| sum | SUM |
| average | AVERAGE |
| min | MIN |
| max | MAX |
| number | Custom DAX |

### Data Types

| Looker Type | Power BI Type |
|------------|---------------|
| string | String |
| number | Double |
| int | Int64 |
| date | DateTime |
| datetime | DateTime |
| yesno | Boolean |

## Project Structure

```
Bi-Migrator-Looker/
├── looker_migrator/
│   ├── __init__.py
│   ├── main.py              # Entry point
│   ├── models.py            # Data models
│   ├── config/              # Configuration
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── parsers/             # LookML parsing
│   │   ├── __init__.py
│   │   ├── lookml_parser.py
│   │   └── project_parser.py
│   ├── converters/          # Expression conversion
│   │   ├── __init__.py
│   │   ├── expression_converter.py
│   │   ├── sql_to_dax_converter.py
│   │   ├── datatype_mapper.py
│   │   └── join_converter.py
│   ├── generators/          # TMDL generation
│   │   ├── __init__.py
│   │   ├── tmdl_generator.py
│   │   ├── model_generator.py
│   │   └── view_converter.py
│   ├── validators/          # Output validation
│   │   ├── __init__.py
│   │   ├── dax_validator.py
│   │   ├── relationship_validator.py
│   │   └── tmdl_validator.py
│   ├── templates/           # Jinja2 templates
│   │   ├── model.tmdl.j2
│   │   ├── database.tmdl.j2
│   │   ├── table.tmdl.j2
│   │   └── relationships.tmdl.j2
│   └── common/              # Utilities
│       ├── __init__.py
│       ├── log_utils.py
│       └── logging_service.py
├── tests/                   # Test suites
│   ├── __init__.py
│   ├── test_looker_migration.py
│   ├── test_validation.py
│   └── test_api.py
├── scripts/                 # Utility scripts
│   ├── batch_migration_test.py
│   ├── run_migration_and_compile.py
│   └── compile/
│       └── compile_pbit_online.py
├── input/                   # Sample input files
├── test_output/             # Test output directory
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
└── README.md
```

## Running Tests

```bash
# Run all tests
python run_tests.py

# Run specific test suite
python run_tests.py --migration
python run_tests.py --validation
python run_tests.py --api

# Run with verbose output
python run_tests.py -v 2

# Run specific test
python run_tests.py -t test_looker_migration.TestExpressionConverter
```

## Batch Processing

```bash
# Process multiple projects
python scripts/batch_migration_test.py ./looker_projects -o ./batch_output

# With validation and verbose output
python scripts/batch_migration_test.py ./projects -o ./output -v

# Save summary to JSON
python scripts/batch_migration_test.py ./projects -o ./output --summary-file results.json
```

## PBIT Compilation

```bash
# Simplest command: convert migration output folder to .pbit
python scripts/compile/convert_to_pbit.py ./output --name MyModel

# Compile generated output (expects output/pbit/* layout)
python scripts/compile/compile_pbit_online.py --project ./output --name MyModel

# Or provide explicit output path for the .pbit file
python scripts/compile/compile_pbit_online.py --project ./output --output ./output/MyModel.pbit

# Health check for compiler service
python scripts/compile/compile_pbit_online.py --health
```

## Configuration

### YAML Configuration

Create a `config.yaml` file:

```yaml
parser:
  max_file_size_mb: 100
  resolve_extends: true
  skip_invalid_views: true

converter:
  default_connection_type: sql_server
  convert_derived_tables: true

generator:
  tmdl_version: "1567"
  culture: en-US
  sanitize_names: true

output:
  output_encoding: utf-8
  log_level: INFO
```

Load with:

```python
settings = Settings.from_yaml("config.yaml")
```

### Environment Variables

- `PBIT_COMPILE_API_URL`: URL for online compilation service
- `PBIT_COMPILE_API_KEY`: API key for compilation service

## Limitations

- Liquid templating in LookML is not supported
- Some complex derived table SQL may require manual adjustment
- Custom SQL blocks may need review for DAX compatibility
- Looker refinements are currently not processed

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `python run_tests.py`
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Related Projects

- [Bi-Migrator-SAP-BO](https://github.com/Codehive-Inc/Bi-Migrator-SAP-BO) - SAP BusinessObjects migration
- [Bi-Migrator-Tableau](https://github.com/Codehive-Inc/Bi-Migrator-Tableau) - Tableau migration
- [Bi-Migrator-Cognos](https://github.com/Codehive-Inc/Bi-Migrator-Cognos) - IBM Cognos migration
- [Bi-Migrator-MicroStrategy](https://github.com/Codehive-Inc/Bi-Migrator-MicroStrategy) - MicroStrategy migration
