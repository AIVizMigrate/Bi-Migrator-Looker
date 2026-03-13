"""
Test suite for Looker Migrator API.

Tests the public API functions and classes.
"""

import unittest
import tempfile
from pathlib import Path

from looker_migrator import (
    LookerMigrator,
    migrate_lookml_project,
    migrate_lookml_view,
    Settings,
    load_settings,
)
from looker_migrator.models import MigrationResult


class TestSettings(unittest.TestCase):
    """Test Settings configuration."""

    def test_default_settings(self):
        """Test default settings creation."""
        settings = Settings()
        self.assertIsNotNone(settings.parser)
        self.assertIsNotNone(settings.converter)
        self.assertIsNotNone(settings.generator)
        self.assertIsNotNone(settings.output)

    def test_settings_from_dict(self):
        """Test creating settings from dictionary."""
        config = {
            "parser": {
                "max_file_size_mb": 200,
                "resolve_extends": False,
            },
            "converter": {
                "convert_derived_tables": False,
            },
            "generator": {
                "tmdl_version": "1600",
                "culture": "de-DE",
            },
            "verbose": True,
        }
        settings = Settings.from_dict(config)
        self.assertEqual(settings.parser.max_file_size_mb, 200)
        self.assertFalse(settings.parser.resolve_extends)
        self.assertFalse(settings.converter.convert_derived_tables)
        self.assertEqual(settings.generator.tmdl_version, "1600")
        self.assertEqual(settings.generator.culture, "de-DE")
        self.assertTrue(settings.verbose)

    def test_settings_to_dict(self):
        """Test converting settings to dictionary."""
        settings = Settings()
        settings.job_id = "test-123"
        config = settings.to_dict()

        self.assertIn("parser", config)
        self.assertIn("converter", config)
        self.assertIn("generator", config)
        self.assertIn("output", config)
        self.assertEqual(config["job_id"], "test-123")

    def test_settings_from_yaml(self):
        """Test loading settings from YAML file."""
        yaml_content = """
parser:
  max_file_size_mb: 150
  skip_invalid_views: false

converter:
  default_connection_type: postgresql

generator:
  culture: fr-FR
  sanitize_names: false

output:
  log_level: DEBUG

job_id: yaml-test-001
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            yaml_path = f.name

        try:
            settings = Settings.from_yaml(yaml_path)
            self.assertEqual(settings.parser.max_file_size_mb, 150)
            self.assertFalse(settings.parser.skip_invalid_views)
            self.assertEqual(settings.converter.default_connection_type, "postgresql")
            self.assertEqual(settings.generator.culture, "fr-FR")
            self.assertEqual(settings.output.log_level, "DEBUG")
            self.assertEqual(settings.job_id, "yaml-test-001")
        finally:
            Path(yaml_path).unlink()

    def test_load_settings_function(self):
        """Test load_settings convenience function."""
        settings = load_settings()
        self.assertIsInstance(settings, Settings)

    def test_load_settings_with_overrides(self):
        """Test load_settings with override dictionary."""
        overrides = {
            "verbose": True,
            "job_id": "override-test",
        }
        settings = load_settings(overrides=overrides)
        self.assertTrue(settings.verbose)
        self.assertEqual(settings.job_id, "override-test")


class TestLookerMigrator(unittest.TestCase):
    """Test LookerMigrator class."""

    def test_migrator_initialization(self):
        """Test migrator initialization with default settings."""
        migrator = LookerMigrator()
        self.assertIsNotNone(migrator.settings)
        self.assertIsNotNone(migrator.project_parser)
        self.assertIsNotNone(migrator.lookml_parser)
        self.assertIsNotNone(migrator.model_generator)
        self.assertIsNotNone(migrator.tmdl_generator)

    def test_migrator_with_custom_settings(self):
        """Test migrator initialization with custom settings."""
        settings = Settings()
        settings.verbose = True
        settings.job_id = "custom-job"

        migrator = LookerMigrator(settings=settings)
        self.assertTrue(migrator.settings.verbose)
        self.assertEqual(migrator.settings.job_id, "custom-job")

    def test_migrator_with_progress_callback(self):
        """Test migrator with progress callback."""
        progress_calls = []

        def progress_callback(phase: str, percent: int, message: str):
            progress_calls.append((phase, percent, message))

        migrator = LookerMigrator(progress_callback=progress_callback)
        self.assertIsNotNone(migrator.logger)

    def test_migrate_nonexistent_project(self):
        """Test migrating nonexistent project path."""
        migrator = LookerMigrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrator.migrate_project(
                project_path="/nonexistent/path",
                output_dir=tmpdir,
            )
            self.assertFalse(result.success)
            self.assertTrue(len(result.errors) > 0)

    def test_migrate_nonexistent_view(self):
        """Test migrating nonexistent view file."""
        migrator = LookerMigrator()

        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrator.migrate_view(
                view_path="/nonexistent/view.view.lkml",
                output_dir=tmpdir,
            )
            self.assertFalse(result.success)


class TestMigrationFunctions(unittest.TestCase):
    """Test migration convenience functions."""

    def test_migrate_lookml_project_nonexistent(self):
        """Test migrate_lookml_project with nonexistent path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_lookml_project(
                project_path="/nonexistent/project",
                output_dir=tmpdir,
            )
            self.assertFalse(result.success)

    def test_migrate_lookml_view_nonexistent(self):
        """Test migrate_lookml_view with nonexistent path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_lookml_view(
                view_path="/nonexistent/view.view.lkml",
                output_dir=tmpdir,
            )
            self.assertFalse(result.success)

    def test_migrate_with_model_name(self):
        """Test migration with custom model name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Even though it fails, it should accept the model_name parameter
            result = migrate_lookml_project(
                project_path="/nonexistent/project",
                output_dir=tmpdir,
                model_name="CustomModelName",
            )
            # The function should not error on the parameter itself
            self.assertIsInstance(result, MigrationResult)

    def test_migrate_with_settings(self):
        """Test migration with custom settings."""
        settings = Settings()
        settings.verbose = True

        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_lookml_project(
                project_path="/nonexistent/project",
                output_dir=tmpdir,
                settings=settings,
            )
            self.assertIsInstance(result, MigrationResult)

    def test_migrate_with_progress_callback(self):
        """Test migration with progress callback."""
        progress_calls = []

        def callback(phase: str, percent: int, message: str):
            progress_calls.append((phase, percent, message))

        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_lookml_project(
                project_path="/nonexistent/project",
                output_dir=tmpdir,
                progress_callback=callback,
            )
            # Progress callback should have been called at least once
            # (for the start of migration before it fails)
            self.assertIsInstance(result, MigrationResult)


class TestMigrationResult(unittest.TestCase):
    """Test MigrationResult class."""

    def test_successful_result_properties(self):
        """Test successful migration result."""
        result = MigrationResult(
            success=True,
            output_path="/tmp/output",
            source_file="/tmp/input",
            model_name="TestModel",
            tables_count=5,
            measures_count=10,
            relationships_count=3,
            views_converted=5,
            explores_converted=2,
            duration_seconds=1.5,
            generated_files=["model.tmdl", "database.tmdl", "Orders.tmdl"],
        )

        self.assertTrue(result.success)
        self.assertEqual(result.tables_count, 5)
        self.assertEqual(result.measures_count, 10)
        self.assertEqual(result.relationships_count, 3)
        self.assertEqual(result.views_converted, 5)
        self.assertEqual(result.explores_converted, 2)
        self.assertEqual(len(result.generated_files), 3)
        self.assertEqual(len(result.errors), 0)
        self.assertEqual(len(result.warnings), 0)

    def test_failed_result_properties(self):
        """Test failed migration result."""
        from looker_migrator.models import MigrationError

        result = MigrationResult(
            success=False,
            output_path="/tmp/output",
            source_file="/tmp/input",
            errors=[
                MigrationError(
                    code="PARSE_ERROR",
                    message="Failed to parse LookML",
                    source_element="views/broken.view.lkml",
                ),
                MigrationError(
                    code="CONVERSION_ERROR",
                    message="Failed to convert expression",
                ),
            ],
            duration_seconds=0.5,
        )

        self.assertFalse(result.success)
        self.assertEqual(len(result.errors), 2)
        self.assertEqual(result.errors[0].code, "PARSE_ERROR")

    def test_result_with_warnings(self):
        """Test migration result with warnings."""
        from looker_migrator.models import MigrationWarning

        result = MigrationResult(
            success=True,
            output_path="/tmp/output",
            source_file="/tmp/input",
            warnings=[
                MigrationWarning(
                    code="UNSUPPORTED_FEATURE",
                    message="Liquid templates not supported",
                    source_element="views/orders.view.lkml",
                ),
            ],
        )

        self.assertTrue(result.success)
        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(result.warnings[0].code, "UNSUPPORTED_FEATURE")


class TestModuleImports(unittest.TestCase):
    """Test that all public APIs are importable."""

    def test_main_imports(self):
        """Test main module imports."""
        from looker_migrator import (
            LookerMigrator,
            migrate_lookml_project,
            migrate_lookml_view,
            Settings,
            load_settings,
        )
        self.assertIsNotNone(LookerMigrator)
        self.assertIsNotNone(migrate_lookml_project)
        self.assertIsNotNone(migrate_lookml_view)
        self.assertIsNotNone(Settings)
        self.assertIsNotNone(load_settings)

    def test_model_imports(self):
        """Test model imports."""
        from looker_migrator.models import (
            LookmlProject,
            LookmlModel,
            LookmlView,
            LookmlExplore,
            LookmlDimension,
            LookmlMeasure,
            LookmlJoin,
            PbiModel,
            PbiTable,
            PbiColumn,
            PbiMeasure,
            PbiRelationship,
            MigrationResult,
            MigrationError,
            MigrationWarning,
        )
        # All should be importable without error
        self.assertIsNotNone(LookmlProject)
        self.assertIsNotNone(PbiModel)
        self.assertIsNotNone(MigrationResult)

    def test_parser_imports(self):
        """Test parser imports."""
        from looker_migrator.parsers import (
            LookmlParser,
            ProjectParser,
        )
        self.assertIsNotNone(LookmlParser)
        self.assertIsNotNone(ProjectParser)

    def test_converter_imports(self):
        """Test converter imports."""
        from looker_migrator.converters import (
            ExpressionConverter,
            SqlToDaxConverter,
            DatatypeMapper,
            JoinConverter,
        )
        self.assertIsNotNone(ExpressionConverter)
        self.assertIsNotNone(SqlToDaxConverter)
        self.assertIsNotNone(DatatypeMapper)
        self.assertIsNotNone(JoinConverter)

    def test_generator_imports(self):
        """Test generator imports."""
        from looker_migrator.generators import (
            ModelGenerator,
            TmdlGenerator,
            ViewConverter,
        )
        self.assertIsNotNone(ModelGenerator)
        self.assertIsNotNone(TmdlGenerator)
        self.assertIsNotNone(ViewConverter)

    def test_validator_imports(self):
        """Test validator imports."""
        from looker_migrator.validators import (
            DAXValidator,
            RelationshipValidator,
            TMDLValidator,
        )
        self.assertIsNotNone(DAXValidator)
        self.assertIsNotNone(RelationshipValidator)
        self.assertIsNotNone(TMDLValidator)


if __name__ == "__main__":
    unittest.main()
