"""Tests for Looker-aligned architecture facade."""

import tempfile
import unittest
from pathlib import Path

from looker_migrator import (
    Settings,
    load_settings,
    migrate_lookml_project_arch,
    migrate_single_project,
    migrate_single_workbook,
)


class TestTableauAlignedSettings(unittest.TestCase):
    """Validate settings behavior for Looker-aligned facade."""

    def test_load_settings_from_dict(self):
        settings = load_settings(
            overrides={
                "converter": {"dax_api_url": "https://daxapidemo.azurewebsites.net"},
                "generator": {"culture": "en-US"},
            }
        )
        self.assertIsInstance(settings, Settings)
        self.assertEqual(settings.converter.dax_api_url, "https://daxapidemo.azurewebsites.net")
        self.assertEqual(settings.generator.culture, "en-US")

    def test_convert_to_looker_settings(self):
        settings = Settings.from_dict(
            {
                "job_id": "tableau-arch-test",
                "generator": {"culture": "en-US"},
            }
        )
        self.assertEqual(settings.job_id, "tableau-arch-test")
        self.assertEqual(settings.generator.culture, "en-US")


class TestTableauAlignedMigration(unittest.TestCase):
    """Validate Looker-style orchestration output contract."""

    def test_migrate_single_project_contract(self):
        sample_project = Path("input") / "sample_project"
        self.assertTrue(sample_project.exists(), "Sample input project missing")

        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_single_project(
                filename=sample_project,
                output_dir=tmpdir,
                validate_output=True,
            )

            self.assertIn("pbit_dir", result)
            self.assertIn("extracted_dir", result)
            self.assertIn("source_dir", result)
            self.assertIn("files", result)
            self.assertIn("migration_result", result)
            self.assertIn("validation", result)
            self.assertIn("metadata_files", result)

            self.assertTrue(Path(result["pbit_dir"]).exists())
            self.assertTrue(Path(result["extracted_dir"]).exists())
            self.assertTrue(Path(result["source_dir"]).exists())
            self.assertTrue((Path(tmpdir) / "validation_summary.json").exists())
            self.assertTrue((Path(result["extracted_dir"]) / "model.json").exists())
            self.assertTrue((Path(result["extracted_dir"]) / "relationships.json").exists())
            self.assertTrue((Path(result["extracted_dir"]) / "calculations.json").exists())
            self.assertTrue(len(result["files"]) > 0)
            self.assertTrue(len(result["metadata_files"]) > 0)
            self.assertTrue(result["migration_result"]["success"])
            self.assertTrue(result["validation"]["is_valid"])

    def test_migrate_lookml_project_arch_wrapper(self):
        sample_project = Path("input") / "sample_project"
        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_lookml_project_arch(
                project_path=sample_project,
                output_dir=tmpdir,
                validate_output=False,
            )
            self.assertTrue(result["migration_result"]["success"])
            self.assertTrue((Path(tmpdir) / "extracted" / "model.json").exists())

    def test_migrate_single_workbook_compatibility_kwargs(self):
        sample_project = Path("input") / "sample_project"
        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_single_workbook(
                workbook_path=sample_project,
                output_dir=tmpdir,
                validate_output=False,
                skip_license_check=True,
                task_id="tableau-contract-task",
            )
            self.assertTrue(result["migration_result"]["success"])
            self.assertEqual(result["task_id"], "tableau-contract-task")

    def test_migrate_single_project_generates_task_id(self):
        sample_project = Path("input") / "sample_project"
        with tempfile.TemporaryDirectory() as tmpdir:
            result = migrate_single_project(
                filename=sample_project,
                output_dir=tmpdir,
                validate_output=False,
            )
            self.assertIn("task_id", result)
            self.assertTrue(result["task_id"].startswith("migration_"))


if __name__ == "__main__":
    unittest.main()
