"""Regression tests for recently fixed migration issues."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from looker_migrator.main import migrate_single_project
from looker_migrator.common.logging_service import LookerLoggingService
from looker_migrator.generators.model_generator import ModelGenerator
from looker_migrator.models import (
    LookmlModel,
    LookmlProject,
    LookmlMeasure,
    LookmlView,
)
from looker_migrator.validators.tmdl_validator import TMDLValidator


class TestRegressionFixes(unittest.TestCase):
    """Validate key regressions stay fixed."""

    def test_metadata_extraction_does_not_reconvert_model(self):
        """Metadata extraction should reuse first-pass conversion snapshots."""
        sample_project = Path("input") / "sample_project"
        self.assertTrue(sample_project.exists(), "Sample input project missing")

        call_counter = {"count": 0}
        original_generate = ModelGenerator.generate_from_project

        def wrapped_generate(self, *args, **kwargs):
            call_counter["count"] += 1
            return original_generate(self, *args, **kwargs)

        with patch.object(ModelGenerator, "generate_from_project", new=wrapped_generate):
            with tempfile.TemporaryDirectory() as tmpdir:
                result = migrate_single_project(
                    filename=sample_project,
                    output_dir=tmpdir,
                    validate_output=False,
                )
                self.assertTrue(result["migration_result"]["success"])

        self.assertEqual(
            call_counter["count"],
            1,
            "Model conversion should run once (not once again during metadata extraction)",
        )

    def test_log_settings_info_logs_output_format(self):
        """Settings summary should log output format."""
        service = LookerLoggingService()
        captured = {}

        def fake_handle_message(**kwargs):
            captured.update(kwargs)

        with patch.object(service, "handle_message", side_effect=fake_handle_message):
            service.log_settings_info(
                {
                    "converter": {},
                    "output": {"format": "tmdl"},
                },
                task_id="regression-log-settings",
            )

        self.assertIn("settings_summary", captured)
        self.assertEqual(captured["settings_summary"]["output_format"], "tmdl")

    def test_validate_content_returns_issues_without_exception(self):
        """Inline validator helper should not raise attribute errors."""
        validator = TMDLValidator()
        issues = validator.validate_content(
            "model Model\n\tculture: en-US\n",
            filename="inline_model.tmdl",
        )
        self.assertIsInstance(issues, list)

    def test_duplicate_measure_rename_is_info_not_warning(self):
        """Expected measure normalization should not spam warning logs."""
        project = LookmlProject(
            name="dup_measures",
            views=[
                LookmlView(name="orders", measures=[LookmlMeasure(name="count", type="count")]),
                LookmlView(name="customers", measures=[LookmlMeasure(name="count", type="count")]),
            ],
            models=[LookmlModel(name="dup_measures", explores=[])],
        )
        generator = ModelGenerator()

        with self.assertLogs("looker_migrator", level="INFO") as captured:
            model = generator.generate_from_project(project)

        measure_names = [m.name for t in model.tables for m in t.measures]
        self.assertEqual(len(measure_names), 2)
        self.assertEqual(len(set(n.lower() for n in measure_names)), 2)

        duplicate_warnings = [
            line for line in captured.output
            if "WARNING" in line and "duplicate measure" in line.lower()
        ]
        self.assertEqual(duplicate_warnings, [])
        self.assertTrue(
            any("Normalized" in line for line in captured.output),
            "Expected summary info log for normalized duplicate measures",
        )


if __name__ == "__main__":
    unittest.main()
