#!/usr/bin/env python3
"""
Combined migration and compilation script for Looker Migrator.

Runs the full pipeline: LookML -> TMDL -> PBIT
"""

import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from looker_migrator import (
    LookerMigrator,
    migrate_lookml_project,
    migrate_lookml_view,
    Settings,
)
from looker_migrator.validators import (
    DAXValidator,
    TMDLValidator,
)


@dataclass
class PipelineResult:
    """Result of full migration pipeline."""
    success: bool
    source_path: str
    tmdl_output_path: Optional[str] = None
    pbit_output_path: Optional[str] = None
    model_name: Optional[str] = None
    tables_count: int = 0
    measures_count: int = 0
    relationships_count: int = 0
    views_converted: int = 0
    migration_time_seconds: float = 0.0
    validation_time_seconds: float = 0.0
    compile_time_seconds: float = 0.0
    total_time_seconds: float = 0.0
    errors: list[str] = None
    warnings: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


def run_pipeline(
    input_path: str,
    output_dir: str,
    model_name: Optional[str] = None,
    compile_pbit: bool = False,
    validate: bool = True,
    verbose: bool = False,
    use_local_compile: bool = False,
) -> PipelineResult:
    """
    Run the complete migration pipeline.

    Args:
        input_path: Path to LookML project or view file
        output_dir: Output directory for TMDL files
        model_name: Optional model name
        compile_pbit: Whether to compile to PBIT
        validate: Whether to validate output
        verbose: Enable verbose output
        use_local_compile: Deprecated flag; online compiler is always used

    Returns:
        PipelineResult with pipeline status
    """
    start_time = time.time()
    result = PipelineResult(
        success=False,
        source_path=input_path,
    )

    input_path_obj = Path(input_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Progress callback
    def progress(phase: str, percent: int, message: str):
        if verbose:
            print(f"[{phase}] {percent}% - {message}")

    # Phase 1: Migration
    if verbose:
        print("\n=== PHASE 1: MIGRATION ===")

    migration_start = time.time()

    if input_path_obj.is_dir():
        migration_result = migrate_lookml_project(
            project_path=input_path_obj,
            output_dir=output_path,
            model_name=model_name,
            progress_callback=progress,
        )
    else:
        migration_result = migrate_lookml_view(
            view_path=input_path_obj,
            output_dir=output_path,
            model_name=model_name,
            progress_callback=progress,
        )

    result.migration_time_seconds = time.time() - migration_start

    if not migration_result.success:
        result.errors = [e.message for e in migration_result.errors]
        result.total_time_seconds = time.time() - start_time
        return result

    result.tmdl_output_path = str(output_path)
    result.model_name = migration_result.model_name
    result.tables_count = migration_result.tables_count
    result.measures_count = migration_result.measures_count
    result.relationships_count = migration_result.relationships_count
    result.views_converted = migration_result.views_converted
    result.warnings = [w.message for w in migration_result.warnings]

    if verbose:
        print(f"Migration completed in {result.migration_time_seconds:.2f}s")
        print(f"  Tables: {result.tables_count}")
        print(f"  Measures: {result.measures_count}")
        print(f"  Relationships: {result.relationships_count}")

    # Phase 2: Validation
    if validate:
        if verbose:
            print("\n=== PHASE 2: VALIDATION ===")

        validation_start = time.time()

        tmdl_validator = TMDLValidator()
        validation_result = tmdl_validator.validate_directory(str(output_path))

        result.validation_time_seconds = time.time() - validation_start

        if not validation_result.is_valid:
            result.errors.extend([
                f"Validation: {issue.message}"
                for issue in validation_result.issues
                if issue.severity.value == "error"
            ])
            result.warnings.extend([
                f"Validation: {issue.message}"
                for issue in validation_result.issues
                if issue.severity.value == "warning"
            ])

            if verbose:
                print(f"Validation failed with {validation_result.error_count} errors")
                for issue in validation_result.issues:
                    print(f"  [{issue.severity.value}] {issue.message}")

            # Continue even with validation errors for compile step
        else:
            if verbose:
                print(f"Validation passed in {result.validation_time_seconds:.2f}s")

    # Phase 3: Compilation (optional)
    if compile_pbit:
        if verbose:
            print("\n=== PHASE 3: COMPILATION ===")

        compile_start = time.time()

        try:
            # Use the curl-based compile script (aligned with Tableau)
            compile_script = Path(__file__).parent / "compile" / "compile_pbit_online.py"
            if not compile_script.exists():
                raise FileNotFoundError(f"Compile script not found: {compile_script}")

            pbit_name = result.model_name or output_path.name
            pbit_name = pbit_name.replace(" ", "_").replace("-", "_")
            pbit_name = "".join(c for c in pbit_name if c.isalnum() or c == "_") or "model"

            endpoint = os.getenv("PBIT_COMPILE_API_URL", "https://pbi-tools-for-agents-production.up.railway.app")

            cmd = [
                sys.executable,
                str(compile_script),
                "--project", str(output_path),
                "--name", pbit_name,
                "--endpoint", endpoint,
                "--log", str(output_path / "compile_online.log"),
            ]

            if verbose:
                print(f"Running online compilation...")
                print(f"  Command: {' '.join(cmd)}")
                print("-" * 60)

            compile_proc = subprocess.run(cmd, text=True, timeout=360)

            if verbose:
                print("-" * 60)

            result.compile_time_seconds = time.time() - compile_start

            if compile_proc.returncode == 0:
                expected_pbit = output_path / f"{pbit_name}.pbit"
                if expected_pbit.exists():
                    result.pbit_output_path = str(expected_pbit)
                    if verbose:
                        print(f"Compilation completed in {result.compile_time_seconds:.2f}s")
                        print(f"  Output: {result.pbit_output_path}")
                else:
                    if verbose:
                        print(f"Compilation completed (validation only)")
            else:
                result.errors.append("Compilation: Online compile failed")
                if verbose:
                    print(f"Compilation failed with exit code {compile_proc.returncode}")

        except subprocess.TimeoutExpired:
            result.errors.append("Compilation: Timed out after 360 seconds")
            if verbose:
                print("Compilation timed out")
        except FileNotFoundError as e:
            result.errors.append(f"Compilation: {str(e)}")
            if verbose:
                print(f"Compilation skipped: {e}")
        except Exception as e:
            result.errors.append(f"Compilation: {str(e)}")
            if verbose:
                print(f"Compilation error: {e}")

    result.total_time_seconds = time.time() - start_time

    # Determine overall success
    # Success if migration worked and no critical errors
    result.success = migration_result.success and not any(
        "error" in e.lower() for e in result.errors
        if not e.startswith("Validation:")  # Allow validation warnings
    )

    return result


def print_result(result: PipelineResult) -> None:
    """Print pipeline result summary."""
    print("\n" + "=" * 60)
    print("MIGRATION PIPELINE RESULT")
    print("=" * 60)
    print(f"Status:        {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Source:        {result.source_path}")
    print(f"TMDL Output:   {result.tmdl_output_path or 'N/A'}")
    print(f"PBIT Output:   {result.pbit_output_path or 'N/A'}")
    print("-" * 60)
    print("METRICS:")
    print(f"  Model Name:     {result.model_name or 'N/A'}")
    print(f"  Tables:         {result.tables_count}")
    print(f"  Measures:       {result.measures_count}")
    print(f"  Relationships:  {result.relationships_count}")
    print(f"  Views:          {result.views_converted}")
    print("-" * 60)
    print("TIMING:")
    print(f"  Migration:   {result.migration_time_seconds:.2f}s")
    print(f"  Validation:  {result.validation_time_seconds:.2f}s")
    print(f"  Compilation: {result.compile_time_seconds:.2f}s")
    print(f"  Total:       {result.total_time_seconds:.2f}s")

    if result.warnings:
        print("-" * 60)
        print(f"WARNINGS ({len(result.warnings)}):")
        for w in result.warnings[:10]:  # Show first 10
            print(f"  - {w}")
        if len(result.warnings) > 10:
            print(f"  ... and {len(result.warnings) - 10} more")

    if result.errors:
        print("-" * 60)
        print(f"ERRORS ({len(result.errors)}):")
        for e in result.errors:
            print(f"  - {e}")

    print("=" * 60)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run complete Looker to Power BI migration pipeline"
    )
    parser.add_argument(
        "input",
        help="Path to LookML project directory or view file"
    )
    parser.add_argument(
        "-o", "--output",
        default="./output",
        help="Output directory for TMDL files"
    )
    parser.add_argument(
        "-n", "--name",
        help="Model name for the output"
    )
    parser.add_argument(
        "--compile",
        action="store_true",
        help="Compile TMDL to PBIT file"
    )
    parser.add_argument(
        "--local-compile",
        action="store_true",
        help="Use local pbi-tools for compilation (requires pbi-tools)"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip TMDL validation"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()

    result = run_pipeline(
        input_path=args.input,
        output_dir=args.output,
        model_name=args.name,
        compile_pbit=args.compile,
        validate=not args.no_validate,
        verbose=args.verbose,
        use_local_compile=args.local_compile,
    )

    print_result(result)

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
