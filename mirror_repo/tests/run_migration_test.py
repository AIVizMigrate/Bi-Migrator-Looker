"""
Migration test script for Looker to Power BI migration.

Tests the complete migration pipeline:
1. Parse LookML files (model, views)
2. Extract metadata in Tableau-aligned format
3. Generate Power BI model
4. Generate TMDL output
"""

import sys
import time
import re
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from looker_migrator.parsers.lookml_parser import LookmlParser
from looker_migrator.extractors.metadata_extractor import MetadataExtractor
from looker_migrator.generators.model_generator import ModelGenerator
from looker_migrator.generators.tmdl_generator import TmdlGenerator
from looker_migrator.models import (
    LookmlProject,
    LookmlModel,
    LookmlExplore,
    LookmlView,
    PbiModel,
)
from looker_migrator.common.log_utils import log_info, log_error, log_warning


@dataclass
class MigrationTestResult:
    """Result of a migration test."""
    success: bool
    project_name: str
    views_count: int = 0
    explores_count: int = 0
    tables_count: int = 0
    measures_count: int = 0
    relationships_count: int = 0
    calculations_count: int = 0
    files_generated: int = 0
    duration: float = 0.0
    error: Optional[str] = None


class LookerMigrationTestRunner:
    """Test runner for Looker migrations."""

    def __init__(self, input_dir: str, output_dir: str):
        """
        Initialize the test runner.

        Args:
            input_dir: Directory containing LookML files
            output_dir: Base output directory for test results
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)

    def find_lookml_projects(self) -> List[dict]:
        """Find LookML projects (model files with views)."""
        projects = []

        # Find model files
        model_files = list(self.input_dir.rglob("*.model.lkml"))

        for model_file in model_files:
            # Look for views in sibling 'views' folder or same folder
            views_dir = model_file.parent / "views"
            if not views_dir.exists():
                views_dir = model_file.parent

            view_files = list(views_dir.rglob("*.view.lkml"))

            project_name = model_file.stem.replace(".model", "")
            projects.append({
                "name": project_name,
                "model_file": str(model_file),
                "view_files": [str(f) for f in view_files],
                "project_dir": str(model_file.parent),
            })

        return projects

    def run_migration(self, project: dict) -> MigrationTestResult:
        """
        Run migration for a single project.

        Args:
            project: Project info dict

        Returns:
            MigrationTestResult
        """
        start_time = time.time()
        project_name = project["name"]

        try:
            # Setup output directories
            output_name = project_name.replace(" ", "_")
            output_path = self.output_dir / output_name
            extracted_dir = output_path / "extracted"
            pbit_dir = output_path / "pbit"

            print(f"\n{'='*60}")
            print(f"Migrating Looker Project: {project_name}")
            print(f"{'='*60}")
            print(f"  Output: {output_path}")
            print(f"  Extracted: {extracted_dir}")
            print(f"  PBIT: {pbit_dir}")
            print(f"  Model: {project['model_file']}")
            print(f"  Views: {len(project['view_files'])} files")

            # Initialize components
            extractor = MetadataExtractor(str(output_path))
            parser = LookmlParser()

            # Save source files
            source_files = [project["model_file"]] + project["view_files"]
            extractor.save_source_files(source_files)

            # Step 1: Parse model file
            print("  Parsing model file...")
            model_blocks = parser.parse_file(project["model_file"])

            # Extract model info and explores
            explores = []
            connection = None

            for block in model_blocks:
                if block.type == "explore":
                    explore = parser.parse_explore(block)
                    explores.append(explore)

            # Get connection from model file content
            model_content = Path(project["model_file"]).read_text()
            conn_match = re.search(r'^connection:\s*["\']?([^"\'\s]+)["\']?\s*$', model_content, re.MULTILINE)
            if conn_match:
                connection = conn_match.group(1).strip('"\'')

            # Step 2: Parse view files
            print("  Parsing view files...")
            views = []
            for view_file in project["view_files"]:
                try:
                    view_blocks = parser.parse_file(view_file)
                    for block in view_blocks:
                        if block.type == "view":
                            view = parser.parse_view(block)
                            views.append(view)
                except Exception as e:
                    log_warning(f"Failed to parse view {view_file}: {e}")

            print(f"  Parsed: {len(views)} views, {len(explores)} explores")

            # Create project object
            lookml_project = LookmlProject(
                name=project_name,
                models=[LookmlModel(
                    name=project_name,
                    connection=connection,
                    explores=explores,
                )],
                views=views,
                connection=connection,
            )

            # Step 3: Save extracted metadata (Tableau-aligned format)
            print("  Saving extracted metadata...")
            extractor.save_model_metadata(lookml_project)
            extractor.save_views_metadata(views, explores)
            extractor.save_explores_metadata(explores)
            extractor.save_relationships_metadata(explores, views)

            # Save config
            extractor.save_config({
                "source_type": "looker",
                "model_file": project["model_file"],
                "view_files": project["view_files"],
                "model_name": output_name,
                "connection": connection,
            })

            # Step 4: Generate Power BI model
            print("  Generating Power BI model...")
            model_generator = ModelGenerator()
            pbi_model = model_generator.generate_from_project(lookml_project, model_name=output_name)

            # Save PBI model metadata
            extractor.save_pbi_model_metadata(pbi_model)
            extractor.save_pbi_tables_metadata(pbi_model.tables)
            if pbi_model.relationships:
                extractor.save_pbi_relationships_metadata(pbi_model.relationships)

            # Save conversion mapping (calculations.json)
            converted_measures = []
            for table in pbi_model.tables:
                for measure in table.measures:
                    converted_measures.append({
                        "name": measure.name,
                        "table_name": table.name,
                        "expression": measure.expression,
                    })
            extractor.save_conversion_mapping(views, converted_measures)

            # Count calculations
            calculations_count = 0
            for view in views:
                for dim in view.dimensions:
                    if extractor._is_actual_calculation(dim.sql, "dimension"):
                        calculations_count += 1
                for measure in view.measures:
                    if extractor._is_actual_calculation(measure.sql, measure.type):
                        calculations_count += 1

            # Step 5: Generate TMDL files
            print("  Generating TMDL files...")
            tmdl_generator = TmdlGenerator()
            generated_files = tmdl_generator.generate(pbi_model, str(output_path))

            # Count measures
            total_measures = sum(len(t.measures) for t in pbi_model.tables)

            duration = time.time() - start_time

            print(f"  SUCCESS: {len(pbi_model.tables)} tables, {total_measures} measures, {calculations_count} calculations, {len(generated_files)} files")

            return MigrationTestResult(
                success=True,
                project_name=project_name,
                views_count=len(views),
                explores_count=len(explores),
                tables_count=len(pbi_model.tables),
                measures_count=total_measures,
                relationships_count=len(pbi_model.relationships),
                calculations_count=calculations_count,
                files_generated=len(generated_files),
                duration=duration,
            )

        except Exception as e:
            import traceback
            duration = time.time() - start_time
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            print(f"  FAILED: {e}")
            log_error(f"Migration failed for {project_name}: {error_msg}")

            return MigrationTestResult(
                success=False,
                project_name=project_name,
                duration=duration,
                error=str(e),
            )

    def run_all(self) -> List[MigrationTestResult]:
        """Run migrations for all found projects."""
        projects = self.find_lookml_projects()
        results = []

        print(f"Found {len(projects)} LookML project(s)\n")

        for project in projects:
            result = self.run_migration(project)
            results.append(result)

        return results

    def generate_report(self, results: List[MigrationTestResult]) -> str:
        """Generate test report."""
        report_lines = [
            "=" * 60,
            "Looker Migration Test Report",
            f"Generated: {datetime.now().isoformat()}",
            "=" * 60,
            "",
        ]

        success_count = sum(1 for r in results if r.success)
        fail_count = len(results) - success_count

        report_lines.append(f"Total: {len(results)}, Success: {success_count}, Failed: {fail_count}")
        report_lines.append("")

        for result in results:
            status = "[SUCCESS]" if result.success else "[FAILED]"
            report_lines.append(f"{status} {result.project_name}")

            if result.success:
                report_lines.append(f"  Duration: {result.duration:.2f}s")
                report_lines.append(f"  Views: {result.views_count}")
                report_lines.append(f"  Explores: {result.explores_count}")
                report_lines.append(f"  Tables: {result.tables_count}")
                report_lines.append(f"  Measures: {result.measures_count}")
                report_lines.append(f"  Calculations: {result.calculations_count}")
                report_lines.append(f"  Relationships: {result.relationships_count}")
            else:
                report_lines.append(f"  Error: {result.error}")

            report_lines.append("")

        return "\n".join(report_lines)


def main():
    """Run the Looker migration tests."""
    # Setup paths
    base_dir = Path(__file__).parent.parent
    input_dir = base_dir / "input"
    output_dir = base_dir / "test_output"

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run tests
    runner = LookerMigrationTestRunner(str(input_dir), str(output_dir))
    results = runner.run_all()

    # Generate and save report
    report = runner.generate_report(results)
    print(f"\n{report}")

    report_file = output_dir / "TEST_REPORT.txt"
    report_file.write_text(report)
    print(f"Report saved to: {report_file}")

    # Return exit code
    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
