"""Standalone frontend contract smoke test for the looker_migrator facade."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Any

# Allow direct execution from repository root with: python scripts/frontend_contract_test.py
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from looker_migrator.main import migrate_single_workbook


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _run_contract_test(
    input_path: Path,
    output_dir: Path,
    task_id: str,
    validate_output: bool,
) -> dict[str, Any]:
    result = migrate_single_workbook(
        workbook_path=input_path,
        output_dir=str(output_dir),
        skip_license_check=True,  # accepted for Tableau API compatibility
        task_id=task_id,
        validate_output=validate_output,
    )

    required_keys = {
        "task_id",
        "pbit_dir",
        "extracted_dir",
        "source_dir",
        "files",
        "migration_result",
        "validation",
        "metadata_files",
    }
    missing = sorted(required_keys - set(result.keys()))
    _assert(not missing, f"Missing response keys: {missing}")

    _assert(result["task_id"] == task_id, "Returned task_id does not match input task_id")
    _assert(Path(result["pbit_dir"]).exists(), "pbit_dir does not exist")
    _assert(Path(result["extracted_dir"]).exists(), "extracted_dir does not exist")
    _assert(Path(result["source_dir"]).exists(), "source_dir does not exist")

    migration_result = result.get("migration_result", {})
    _assert(bool(migration_result.get("success")), "migration_result.success is not True")
    _assert(len(result.get("files", [])) > 0, "No files were reported in result.files")
    _assert(len(result.get("metadata_files", [])) > 0, "No metadata files were reported")

    if validate_output:
        validation = result.get("validation")
        _assert(isinstance(validation, dict), "Validation summary is missing")
        _assert(bool(validation.get("is_valid")), "Validation failed (is_valid=False)")

    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run frontend contract smoke test against looker_migrator.main",
    )
    parser.add_argument(
        "--input",
        default="input/sample_project",
        help="Path to LookML project directory or single .view.lkml file",
    )
    parser.add_argument(
        "--output",
        default="test_output_frontend_contract",
        help="Output directory for smoke test artifacts",
    )
    parser.add_argument(
        "--task-id",
        default="frontend_test_001",
        help="Task ID to use (verifies frontend contract passthrough)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Disable validation stage for faster runs",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Delete output directory before running test",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output)
    validate_output = not args.no_validate

    try:
        _assert(input_path.exists(), f"Input path does not exist: {input_path}")
        if args.clean_output and output_dir.exists():
            shutil.rmtree(output_dir)

        result = _run_contract_test(
            input_path=input_path,
            output_dir=output_dir,
            task_id=args.task_id,
            validate_output=validate_output,
        )

        migration_result = result["migration_result"]
        validation = result.get("validation") or {}

        print("Frontend contract test passed")
        print(f"task_id={result['task_id']}")
        print(f"success={migration_result.get('success')}")
        print(f"tables={migration_result.get('tables_count')}")
        print(f"measures={migration_result.get('measures_count')}")
        print(f"relationships={migration_result.get('relationships_count')}")
        print(f"files={len(result.get('files', []))}")
        print(f"metadata_files={len(result.get('metadata_files', []))}")
        if validate_output:
            print(f"validation_is_valid={validation.get('is_valid')}")
            print(f"validation_errors={validation.get('error_count')}")
            print(f"validation_warnings={validation.get('warning_count')}")
        print(f"output_dir={output_dir}")
        return 0
    except Exception as error:  # pragma: no cover - smoke test command
        print(f"Frontend contract test failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
