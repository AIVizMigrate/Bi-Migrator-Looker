"""Tests for PBIT compilation helpers."""

import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from scripts.compile.compile_pbit_online import PbitCompiler


class TestPbitCompiler(unittest.TestCase):
    """Validate layout resolution and packaging behavior."""

    def _write_file(self, path: Path, content: str = "x") -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def test_validate_prefers_pbit_layout_over_definition(self):
        """When both layouts exist, compiler should validate using pbit/Model."""
        compiler = PbitCompiler()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_file(root / "definition" / "database.tmdl", "db")
            self._write_file(root / "pbit" / "Model" / "model.tmdl", "model")
            self._write_file(root / "pbit" / "Model" / "tables" / "Orders.tmdl", "table")
            self._write_file(root / "pbit" / "Report" / "report.json", "{}")

            is_valid, issues = compiler.validate_project(root)
            self.assertTrue(is_valid, f"Expected valid project, got issues: {issues}")

    def test_validate_detects_missing_tables(self):
        """Validation should fail if tables folder is missing under model root."""
        compiler = PbitCompiler()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_file(root / "pbit" / "Model" / "model.tmdl", "model")
            self._write_file(root / "pbit" / "Report" / "report.json", "{}")

            is_valid, issues = compiler.validate_project(root)
            self.assertFalse(is_valid)
            self.assertTrue(any("tables" in issue.lower() for issue in issues))

    def test_package_uses_pbit_root_when_available(self):
        """Packaging should include pbit-root paths instead of legacy definition paths."""
        compiler = PbitCompiler()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_file(root / "definition" / "database.tmdl", "legacy")
            self._write_file(root / "pbit" / ".pbixproj.json", "{}")
            self._write_file(root / "pbit" / "Model" / "model.tmdl", "model")
            self._write_file(root / "pbit" / "Model" / "tables" / "Orders.tmdl", "table")
            self._write_file(root / "pbit" / "Report" / "report.json", "{}")

            payload = compiler.package_project(root)
            with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
                names = set(zf.namelist())

            self.assertIn("Model/model.tmdl", names)
            self.assertIn("Model/tables/Orders.tmdl", names)
            self.assertIn("Report/report.json", names)
            self.assertNotIn("definition/database.tmdl", names)

    def test_compile_validate_only(self):
        """Validate-only compile mode should not attempt upload."""
        compiler = PbitCompiler()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_file(root / "pbit" / "Model" / "model.tmdl", "model")
            self._write_file(root / "pbit" / "Model" / "tables" / "Orders.tmdl", "table")
            self._write_file(root / "pbit" / "Report" / "report.json", "{}")

            result = compiler.compile(root, validate_only=True)
            self.assertTrue(result.get("success"))
            self.assertEqual(result.get("message"), "Validation passed")


if __name__ == "__main__":
    unittest.main()
