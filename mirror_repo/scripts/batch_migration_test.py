#!/usr/bin/env python3
"""
Batch migration testing script for Looker Migrator.

Processes multiple LookML projects/views and generates metrics.
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from looker_migrator import (
    LookerMigrator,
    migrate_lookml_project,
    migrate_lookml_view,
    Settings,
)
from looker_migrator.validators import (
    DAXValidator,
    RelationshipValidator,
    TMDLValidator,
)


@dataclass
class BatchTestResult:
    """Result of a single batch test item."""
    source_path: str
    output_path: str
    success: bool
    model_name: Optional[str] = None
    tables_count: int = 0
    measures_count: int = 0
    relationships_count: int = 0
    views_converted: int = 0
    explores_converted: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    warnings_count: int = 0
    validation_passed: bool = False
    validation_errors: int = 0


@dataclass
class BatchTestSummary:
    """Summary of batch test execution."""
    total_items: int = 0
    successful: int = 0
    failed: int = 0
    total_tables: int = 0
    total_measures: int = 0
    total_relationships: int = 0
    total_views: int = 0
    total_explores: int = 0
    total_duration_seconds: float = 0.0
    validation_passed: int = 0
    validation_failed: int = 0
    results: list[BatchTestResult] = field(default_factory=list)
    start_time: str = ""
    end_time: str = ""


class BatchMigrationTester:
    """Handles batch migration testing."""

    def __init__(
        self,
        output_dir: str,
        settings: Optional[Settings] = None,
        validate: bool = True,
        verbose: bool = False,
    ):
        """
        Initialize batch tester.

        Args:
            output_dir: Base output directory for migrations
            settings: Optional settings for migration
            validate: Whether to validate output
            verbose: Enable verbose output
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.settings = settings or Settings()
        self.validate = validate
        self.verbose = verbose

        self.migrator = LookerMigrator(settings=self.settings)
        self.dax_validator = DAXValidator()
        self.rel_validator = RelationshipValidator()
        self.tmdl_validator = TMDLValidator()

    def run_batch(
        self,
        input_paths: list[str],
        model_prefix: str = "",
    ) -> BatchTestSummary:
        """
        Run batch migration tests.

        Args:
            input_paths: List of paths to LookML projects or view files
            model_prefix: Optional prefix for model names

        Returns:
            BatchTestSummary with all results
        """
        summary = BatchTestSummary(
            total_items=len(input_paths),
            start_time=datetime.now().isoformat(),
        )

        for i, input_path in enumerate(input_paths, 1):
            if self.verbose:
                print(f"\n[{i}/{len(input_paths)}] Processing: {input_path}")

            result = self._process_item(input_path, model_prefix, i)
            summary.results.append(result)

            if result.success:
                summary.successful += 1
                summary.total_tables += result.tables_count
                summary.total_measures += result.measures_count
                summary.total_relationships += result.relationships_count
                summary.total_views += result.views_converted
                summary.total_explores += result.explores_converted

                if result.validation_passed:
                    summary.validation_passed += 1
                else:
                    summary.validation_failed += 1
            else:
                summary.failed += 1

            summary.total_duration_seconds += result.duration_seconds

        summary.end_time = datetime.now().isoformat()
        return summary

    def _process_item(
        self,
        input_path: str,
        model_prefix: str,
        index: int,
    ) -> BatchTestResult:
        """Process a single migration item."""
        path = Path(input_path)
        item_output = self.output_dir / f"{model_prefix}item_{index:04d}"
        item_output.mkdir(parents=True, exist_ok=True)

        # Determine model name
        model_name = f"{model_prefix}{path.stem}".replace("-", "_").replace(" ", "_")

        start_time = time.time()

        try:
            # Run migration
            if path.is_dir():
                migration_result = self.migrator.migrate_project(
                    project_path=path,
                    output_dir=item_output,
                    model_name=model_name,
                )
            else:
                migration_result = self.migrator.migrate_view(
                    view_path=path,
                    output_dir=item_output,
                    model_name=model_name,
                )

            duration = time.time() - start_time

            if not migration_result.success:
                error_msg = "; ".join(e.message for e in migration_result.errors[:3])
                return BatchTestResult(
                    source_path=str(path),
                    output_path=str(item_output),
                    success=False,
                    error_message=error_msg,
                    duration_seconds=duration,
                )

            # Run validation if enabled
            validation_passed = True
            validation_errors = 0

            if self.validate:
                tmdl_result = self.tmdl_validator.validate_directory(str(item_output))
                validation_passed = tmdl_result.is_valid
                validation_errors = tmdl_result.error_count

            return BatchTestResult(
                source_path=str(path),
                output_path=str(item_output),
                success=True,
                model_name=migration_result.model_name,
                tables_count=migration_result.tables_count,
                measures_count=migration_result.measures_count,
                relationships_count=migration_result.relationships_count,
                views_converted=migration_result.views_converted,
                explores_converted=migration_result.explores_converted,
                duration_seconds=duration,
                warnings_count=len(migration_result.warnings),
                validation_passed=validation_passed,
                validation_errors=validation_errors,
            )

        except Exception as e:
            return BatchTestResult(
                source_path=str(path),
                output_path=str(item_output),
                success=False,
                error_message=str(e),
                duration_seconds=time.time() - start_time,
            )

    def run_directory(
        self,
        source_dir: str,
        pattern: str = "**/*.lkml",
        model_prefix: str = "",
    ) -> BatchTestSummary:
        """
        Run batch migration on all matching files in a directory.

        Args:
            source_dir: Source directory to scan
            pattern: Glob pattern for finding files
            model_prefix: Prefix for model names

        Returns:
            BatchTestSummary
        """
        source_path = Path(source_dir)
        if not source_path.exists():
            raise ValueError(f"Source directory not found: {source_dir}")

        # Find all matching files
        input_paths = [str(p) for p in source_path.glob(pattern)]

        if self.verbose:
            print(f"Found {len(input_paths)} files matching pattern: {pattern}")

        return self.run_batch(input_paths, model_prefix)


def save_summary(summary: BatchTestSummary, output_path: str) -> None:
    """Save batch test summary to JSON file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(asdict(summary), f, indent=2)


def print_summary(summary: BatchTestSummary) -> None:
    """Print batch test summary to console."""
    print("\n" + "=" * 60)
    print("BATCH MIGRATION TEST SUMMARY")
    print("=" * 60)
    print(f"Start Time:    {summary.start_time}")
    print(f"End Time:      {summary.end_time}")
    print(f"Total Items:   {summary.total_items}")
    print(f"Successful:    {summary.successful}")
    print(f"Failed:        {summary.failed}")
    print(f"Success Rate:  {(summary.successful / max(1, summary.total_items)) * 100:.1f}%")
    print("-" * 60)
    print("MIGRATION METRICS:")
    print(f"  Total Tables:        {summary.total_tables}")
    print(f"  Total Measures:      {summary.total_measures}")
    print(f"  Total Relationships: {summary.total_relationships}")
    print(f"  Total Views:         {summary.total_views}")
    print(f"  Total Explores:      {summary.total_explores}")
    print("-" * 60)
    print("VALIDATION:")
    print(f"  Passed:  {summary.validation_passed}")
    print(f"  Failed:  {summary.validation_failed}")
    print("-" * 60)
    print(f"Total Duration: {summary.total_duration_seconds:.2f} seconds")
    print("=" * 60)

    if summary.failed > 0:
        print("\nFAILED ITEMS:")
        for result in summary.results:
            if not result.success:
                print(f"  - {result.source_path}")
                print(f"    Error: {result.error_message}")


def main():
    """Main entry point for batch testing."""
    parser = argparse.ArgumentParser(
        description="Batch migration testing for Looker Migrator"
    )
    parser.add_argument(
        "input",
        help="Input directory or comma-separated list of paths"
    )
    parser.add_argument(
        "-o", "--output",
        default="./batch_test_output",
        help="Output directory for migrations"
    )
    parser.add_argument(
        "-p", "--pattern",
        default="**/*.lkml",
        help="Glob pattern for finding files (default: **/*.lkml)"
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Prefix for model names"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation of output"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--summary-file",
        help="Path to save JSON summary"
    )

    args = parser.parse_args()

    tester = BatchMigrationTester(
        output_dir=args.output,
        validate=not args.no_validate,
        verbose=args.verbose,
    )

    # Determine if input is directory or list of paths
    if os.path.isdir(args.input):
        summary = tester.run_directory(
            source_dir=args.input,
            pattern=args.pattern,
            model_prefix=args.prefix,
        )
    else:
        input_paths = [p.strip() for p in args.input.split(",")]
        summary = tester.run_batch(
            input_paths=input_paths,
            model_prefix=args.prefix,
        )

    # Print summary
    print_summary(summary)

    # Save summary if requested
    if args.summary_file:
        save_summary(summary, args.summary_file)
        print(f"\nSummary saved to: {args.summary_file}")

    # Exit with error code if any failures
    sys.exit(0 if summary.failed == 0 else 1)


if __name__ == "__main__":
    main()
