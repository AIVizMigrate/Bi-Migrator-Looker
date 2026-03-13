"""
Main entry point for Looker to Power BI migration.

Provides the primary API for migrating Looker LookML projects
to Power BI TMDL format.
"""

import json
import time
import uuid
from pathlib import Path
from typing import Optional, Callable, Union, Any

from .models import (
    LookmlProject,
    LookmlModel,
    LookmlView,
    PbiModel,
    MigrationResult,
    MigrationError,
    MigrationWarning,
)
from .parsers import ProjectParser, LookmlParser
from .generators import ModelGenerator, TmdlGenerator
from .converters import DaxApiConfig
from .config import Settings, load_settings
from .extractors.metadata_extractor import MetadataExtractor
from .validators.tmdl_validator import TMDLValidator
from .common.log_utils import log_info, log_warning, log_error
from .common.logging_service import LoggingService


class LookerMigrator:
    """
    Main orchestrator for Looker to Power BI migration.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        progress_callback: Optional[Callable[[str, int, str], None]] = None,
    ):
        """
        Initialize the migrator.

        Args:
            settings: Configuration settings
            progress_callback: Callback function for progress updates
        """
        self.settings = settings or Settings()

        self.logger = LoggingService(
            job_id=self.settings.job_id,
            callback=progress_callback,
        )

        self._init_components()

    def _init_components(self) -> None:
        """Initialize migration components."""
        config_dict = self.settings.to_dict()

        # Initialize DAX API config (AI conversion by default, falls back to rule-based)
        log_info("Initializing DAX API client for AI-powered conversion")
        dax_api_config = DaxApiConfig(
            base_url=self.settings.converter.dax_api_url,
            timeout=self.settings.converter.dax_api_timeout,
            use_rag=self.settings.converter.use_rag,
        )

        # Get task_id from logger
        task_id = self.logger.job_id

        self.project_parser = ProjectParser()
        self.lookml_parser = LookmlParser()
        self.model_generator = ModelGenerator(
            config=config_dict,
            dax_api_config=dax_api_config,
            task_id=task_id,
        )
        self.tmdl_generator = TmdlGenerator(
            output_encoding=self.settings.output.output_encoding,
        )

    def migrate_project(
        self,
        project_path: Union[str, Path],
        output_dir: Union[str, Path],
        model_name: Optional[str] = None,
    ) -> MigrationResult:
        """
        Migrate a complete LookML project.

        Args:
            project_path: Path to LookML project directory
            output_dir: Output directory for TMDL files
            model_name: Optional name for the Power BI model

        Returns:
            MigrationResult with status and details
        """
        start_time = time.time()
        errors: list[MigrationError] = []
        warnings: list[MigrationWarning] = []

        try:
            # Phase 1: Parse project
            self.logger.log_phase("parsing", 0, "Starting migration")
            project = self.project_parser.parse(project_path)
            self.logger.log_phase("parsing", 25, f"Parsed project: {project.name}")

            # Phase 2: Analyze
            self.logger.log_phase("extraction", 25, "Analyzing project structure")
            self._analyze_project(project)
            self.logger.log_phase("extraction", 50, "Analysis complete")

            # Phase 3: Convert
            self.logger.log_phase("conversion", 50, "Converting to Power BI model")
            pbi_model = self.model_generator.generate_from_project(
                project,
                model_name=model_name,
            )
            warnings.extend(self.model_generator.get_warnings())
            self.logger.log_phase("conversion", 75, f"Created model: {pbi_model.name}")

            # Phase 4: Generate TMDL (also generates extracted JSON files)
            self.logger.log_phase("generation", 75, "Generating TMDL files")
            output_path = Path(output_dir)
            generated_files = self.tmdl_generator.generate(pbi_model, str(output_path))

            # Phase 5: Export calculations.json with conversion tracking metadata
            extracted_path = output_path / "extracted"
            calc_file = self.model_generator.export_calculations(extracted_path)
            generated_files.append(str(calc_file))
            calc_summary = self.model_generator.get_calculation_summary()
            log_info(f"Calculation summary: {calc_summary}")

            self.logger.log_phase("generation", 100, f"Generated {len(generated_files)} files")

            duration = time.time() - start_time

            return MigrationResult(
                success=True,
                output_path=str(output_path),
                source_file=str(project_path),
                model_name=pbi_model.name,
                tables_count=len(pbi_model.tables),
                measures_count=sum(len(t.measures) for t in pbi_model.tables),
                relationships_count=len(pbi_model.relationships),
                views_converted=len(project.views),
                explores_converted=sum(len(m.explores) for m in project.models),
                warnings=warnings,
                errors=[],
                duration_seconds=duration,
                generated_files=generated_files,
                calculation_summary=calc_summary,
                conversion_stats=self.model_generator.get_conversion_stats(),
                project_snapshot=project,
                pbi_model_snapshot=pbi_model,
            )

        except Exception as e:
            log_error(f"Migration failed: {e}")
            errors.append(
                MigrationError(
                    code="MIGRATION_FAILED",
                    message=str(e),
                    source_element="migration",
                )
            )

            return MigrationResult(
                success=False,
                output_path=str(output_dir),
                source_file=str(project_path),
                errors=errors,
                warnings=warnings,
                duration_seconds=time.time() - start_time,
                conversion_stats=self.model_generator.get_conversion_stats(),
                project_snapshot=None,
                pbi_model_snapshot=None,
            )

    def migrate_view(
        self,
        view_path: Union[str, Path],
        output_dir: Union[str, Path],
        model_name: Optional[str] = None,
    ) -> MigrationResult:
        """
        Migrate a single LookML view file.

        Args:
            view_path: Path to .view.lkml file
            output_dir: Output directory
            model_name: Optional model name

        Returns:
            MigrationResult
        """
        start_time = time.time()

        try:
            # Parse view
            view = self.project_parser.parse_single_view(view_path)
            if not view:
                raise ValueError(f"Could not parse view: {view_path}")

            # Generate model
            pbi_model = self.model_generator.generate_from_view(view, model_name)

            # Generate TMDL (also generates extracted JSON files)
            output_path = Path(output_dir)
            generated_files = self.tmdl_generator.generate(pbi_model, str(output_path))
            view_input_path = Path(view_path)
            project_name = view_input_path.stem or view.name

            # Export calculations.json with conversion tracking metadata
            extracted_path = output_path / "extracted"
            calc_file = self.model_generator.export_calculations(extracted_path)
            generated_files.append(str(calc_file))
            calc_summary = self.model_generator.get_calculation_summary()

            return MigrationResult(
                success=True,
                output_path=str(output_path),
                source_file=str(view_path),
                model_name=pbi_model.name,
                tables_count=1,
                measures_count=len(pbi_model.tables[0].measures) if pbi_model.tables else 0,
                views_converted=1,
                duration_seconds=time.time() - start_time,
                generated_files=generated_files,
                calculation_summary=calc_summary,
                conversion_stats=self.model_generator.get_conversion_stats(),
                project_snapshot=LookmlProject(
                    name=project_name,
                    views=[view],
                    models=[LookmlModel(name=view.name, explores=[])],
                    project_path=str(view_input_path.parent),
                ),
                pbi_model_snapshot=pbi_model,
            )

        except Exception as e:
            return MigrationResult(
                success=False,
                output_path=str(output_dir),
                source_file=str(view_path),
                errors=[MigrationError(code="VIEW_MIGRATION_FAILED", message=str(e))],
                duration_seconds=time.time() - start_time,
                conversion_stats=self.model_generator.get_conversion_stats(),
                project_snapshot=None,
                pbi_model_snapshot=None,
            )

    def _analyze_project(self, project: LookmlProject) -> None:
        """Analyze and log project statistics."""
        log_info(f"Project: {project.name}")
        log_info(f"  Views: {len(project.views)}")
        log_info(f"  Models: {len(project.models)}")

        total_dims = sum(len(v.dimensions) for v in project.views)
        total_measures = sum(len(v.measures) for v in project.views)
        total_explores = sum(len(m.explores) for m in project.models)

        log_info(f"  Total dimensions: {total_dims}")
        log_info(f"  Total measures: {total_measures}")
        log_info(f"  Total explores: {total_explores}")


def _normalize_settings(settings: Optional[Union[Settings, dict]]) -> Settings:
    """Normalize incoming settings payload to Settings object."""
    if isinstance(settings, Settings):
        return settings
    if isinstance(settings, dict):
        return Settings.from_dict(settings)
    return Settings()


def migrate_lookml_project(
    project_path: Union[str, Path],
    output_dir: Union[str, Path],
    model_name: Optional[str] = None,
    settings: Optional[Union[Settings, dict]] = None,
    progress_callback: Optional[Callable[[str, int, str], None]] = None,
    task_id: Optional[str] = None,
) -> MigrationResult:
    """
    Convenience function to migrate a LookML project.

    Args:
        project_path: Path to LookML project directory
        output_dir: Output directory for TMDL files
        model_name: Optional model name
        settings: Optional configuration (Settings object or dict)
        progress_callback: Optional progress callback
        task_id: Optional task ID for progress tracking (matches Tableau pattern)

    Returns:
        MigrationResult

    Example:
        result = migrate_lookml_project(
            project_path="./my_looker_project",
            output_dir="./output",
            task_id="migration_123",
        )
    """
    # Import WebSocket functions for task initialization
    from .common.websocket_client import set_task_info

    # Normalize settings to handle both dict and Settings object
    normalized_settings = _normalize_settings(settings)

    # Generate task_id if not provided (matches Tableau pattern)
    if task_id is None:
        task_id = f"migration_{uuid.uuid4().hex[:12]}"

    # Set job_id on settings for backward compatibility
    normalized_settings.job_id = task_id

    # Initialize WebSocket logging with task ID (matches Tableau pattern)
    set_task_info(task_id, total_steps=12)

    migrator = LookerMigrator(
        settings=normalized_settings,
        progress_callback=progress_callback,
    )

    return migrator.migrate_project(
        project_path=project_path,
        output_dir=output_dir,
        model_name=model_name,
    )


def migrate_lookml_view(
    view_path: Union[str, Path],
    output_dir: Union[str, Path],
    model_name: Optional[str] = None,
    settings: Optional[Union[Settings, dict]] = None,
    progress_callback: Optional[Callable[[str, int, str], None]] = None,
    task_id: Optional[str] = None,
) -> MigrationResult:
    """
    Convenience function to migrate a single view file.

    Args:
        view_path: Path to .view.lkml file
        output_dir: Output directory
        model_name: Optional model name
        settings: Optional configuration (Settings object or dict)
        progress_callback: Optional progress callback
        task_id: Optional task ID for progress tracking (matches Tableau pattern)

    Returns:
        MigrationResult
    """
    # Validate file extension - must be a .view.lkml file
    view_path_obj = Path(view_path)
    file_name = view_path_obj.name.lower()
    if file_name.endswith('.model.lkml'):
        return MigrationResult(
            success=False,
            source_file=str(view_path),
            errors=[
                MigrationError(
                    code="INVALID_FILE_TYPE",
                    message=f"Expected a .view.lkml file but received a .model.lkml file: {view_path_obj.name}",
                    source_element=view_path_obj.name,
                    details="For single view migration, upload a .view.lkml file. For project migration with model files, use the project/zip migration option.",
                )
            ],
        )
    if not file_name.endswith('.view.lkml') and not file_name.endswith('.lkml'):
        return MigrationResult(
            success=False,
            source_file=str(view_path),
            errors=[
                MigrationError(
                    code="INVALID_FILE_TYPE",
                    message=f"Expected a .view.lkml file but received: {view_path_obj.name}",
                    source_element=view_path_obj.name,
                    details="Single view migration requires a .view.lkml file.",
                )
            ],
        )

    # Import WebSocket functions for task initialization
    from .common.websocket_client import set_task_info

    # Normalize settings to handle both dict and Settings object
    normalized_settings = _normalize_settings(settings)

    # Generate task_id if not provided (matches Tableau pattern)
    if task_id is None:
        task_id = f"migration_{uuid.uuid4().hex[:12]}"

    # Set job_id on settings for backward compatibility
    normalized_settings.job_id = task_id

    # Initialize WebSocket logging with task ID (matches Tableau pattern)
    set_task_info(task_id, total_steps=12)

    migrator = LookerMigrator(
        settings=normalized_settings,
        progress_callback=progress_callback,
    )

    return migrator.migrate_view(
        view_path=view_path,
        output_dir=output_dir,
        model_name=model_name,
    )


def _serialize_migration_result(result: MigrationResult) -> dict[str, Any]:
    """Convert MigrationResult dataclass to frontend-friendly dictionary."""
    return {
        "success": bool(result.success),
        "output_path": result.output_path,
        "source_file": result.source_file,
        "model_name": result.model_name,
        "tables_count": result.tables_count,
        "measures_count": result.measures_count,
        "relationships_count": result.relationships_count,
        "views_converted": result.views_converted,
        "explores_converted": result.explores_converted,
        "duration_seconds": result.duration_seconds,
        "generated_files": list(result.generated_files or []),
        "conversion_stats": dict(result.conversion_stats or {}),
        "warnings": [
            {
                "code": warning.code,
                "message": warning.message,
                "source_element": warning.source_element,
                "suggestion": warning.suggestion,
            }
            for warning in (result.warnings or [])
        ],
        "errors": [
            {
                "code": error.code,
                "message": error.message,
                "source_element": error.source_element,
                "details": error.details,
            }
            for error in (result.errors or [])
        ],
    }


def _validate_output_dir(output_dir: Path) -> dict[str, Any]:
    """Validate generated TMDL output and persist summary."""
    validator = TMDLValidator(str(output_dir))
    validation_result = validator.validate_directory(str(output_dir))
    issues = [
        {
            "severity": issue.severity.value,
            "message": issue.message,
            "file_path": issue.file_path,
            "line_number": issue.line_number,
            "suggestion": issue.suggestion,
        }
        for issue in validation_result.issues
    ]
    summary = {
        "is_valid": validation_result.is_valid,
        "files_checked": validation_result.files_checked,
        "error_count": validation_result.error_count,
        "warning_count": validation_result.warning_count,
        "issues": issues,
    }
    summary_path = output_dir / "validation_summary.json"
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    return summary


def _gather_metadata(
    *,
    output_dir: Path,
    source_path: Path,
    settings_obj: Settings,
    migration_result: MigrationResult,
    copy_source: bool,
) -> list[str]:
    """Generate extracted metadata files without re-running conversion when snapshots are available."""
    project = migration_result.project_snapshot
    pbi_model = migration_result.pbi_model_snapshot

    # Fallback only when snapshots are unavailable.
    if project is None:
        parser = ProjectParser()
        if source_path.is_dir():
            project = parser.parse(source_path)
        else:
            view = parser.parse_single_view(source_path)
            if view:
                project = LookmlProject(
                    name=source_path.stem or view.name,
                    views=[view],
                    models=[LookmlModel(name=view.name, explores=[])],
                    project_path=str(source_path.parent),
                )

    if pbi_model is None and project is not None:
        generator = ModelGenerator(
            config=settings_obj.to_dict(),
            dax_api_config=DaxApiConfig(
                base_url=settings_obj.converter.dax_api_url,
                timeout=settings_obj.converter.dax_api_timeout,
                use_rag=settings_obj.converter.use_rag,
            ),
        )
        pbi_model = generator.generate_from_project(project)

    if project is None or pbi_model is None:
        return []

    extractor = MetadataExtractor(str(output_dir))
    metadata_files: list[Path] = []

    metadata_files.append(extractor.save_model_metadata(project))

    explores = [explore for model in project.models for explore in model.explores]
    metadata_files.append(extractor.save_explores_metadata(explores))
    metadata_files.extend(extractor.save_views_metadata(project.views, explores))
    metadata_files.append(extractor.save_relationships_metadata(explores, project.views))

    converted_measures = [
        {"name": measure.name, "expression": measure.expression}
        for table in pbi_model.tables
        for measure in table.measures
    ]
    metadata_files.append(extractor.save_conversion_mapping(project.views, converted_measures))
    metadata_files.append(extractor.save_config(settings_obj.to_dict()))
    metadata_files.append(extractor.save_pbi_model_metadata(pbi_model))
    metadata_files.extend(extractor.save_pbi_tables_metadata(pbi_model.tables))
    metadata_files.append(extractor.save_pbi_relationships_metadata(pbi_model.relationships))

    if copy_source:
        if source_path.is_dir():
            source_files = [str(p) for p in source_path.rglob("*.lkml")]
        elif source_path.is_file():
            source_files = [str(source_path)]
        else:
            source_files = []
        metadata_files.extend(extractor.save_source_files(source_files))

    return [str(path) for path in metadata_files]


def _collect_files(output_dir: Path) -> list[str]:
    """Collect generated files in stable order."""
    return sorted(
        str(path) for path in output_dir.rglob("*") if path.is_file()
    )


def migrate_single_project(
    filename: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    *,
    output_path: Optional[Union[str, Path]] = None,
    model_name: Optional[str] = None,
    settings: Optional[Union[Settings, dict]] = None,
    progress_callback: Optional[Callable[[str, int, str], None]] = None,
    task_id: Optional[str] = None,
    validate_output: bool = True,
    extract_metadata: bool = True,
    copy_source: bool = False,
    skip_license_check: bool = False,
    **_: Any,
) -> dict[str, Any]:
    """
    Frontend-friendly facade using looker_migrator directly.

    Maintains Tableau-style response contract while keeping implementation
    inside looker_migrator package.
    """
    source_path = Path(filename)
    target_output = Path(output_dir or output_path or "output")

    resolved_task_id = task_id or f"migration_{uuid.uuid4().hex[:12]}"
    settings_obj = _normalize_settings(settings)
    settings_obj.job_id = resolved_task_id

    if skip_license_check:
        log_info("skip_license_check requested; ignored for Looker migration")

    migration_result = migrate_lookml_project(
        project_path=source_path,
        output_dir=target_output,
        model_name=model_name,
        settings=settings_obj,
        progress_callback=progress_callback,
    )

    metadata_files: list[str] = []
    if extract_metadata and migration_result.success:
        metadata_files = _gather_metadata(
            output_dir=target_output,
            source_path=source_path,
            settings_obj=settings_obj,
            migration_result=migration_result,
            copy_source=copy_source,
        )

    if validate_output:
        validation = _validate_output_dir(target_output)
    else:
        validation = {
            "is_valid": True,
            "files_checked": 0,
            "error_count": 0,
            "warning_count": 0,
            "issues": [],
        }

    files = _collect_files(target_output)
    extracted_dir = target_output / "extracted"
    source_dir = extracted_dir / "source"
    source_dir.mkdir(parents=True, exist_ok=True)

    return {
        "task_id": resolved_task_id,
        "pbit_dir": str(target_output / "pbit"),
        "extracted_dir": str(extracted_dir),
        "source_dir": str(source_dir),
        "files": files,
        "metadata_files": metadata_files,
        "migration_result": _serialize_migration_result(migration_result),
        "validation": validation,
    }


def migrate_single_workbook(
    workbook_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Tableau-compatible alias for project migration."""
    return migrate_single_project(
        filename=workbook_path,
        output_dir=output_dir,
        **kwargs,
    )


def migrate_lookml_project_arch(
    project_path: Union[str, Path],
    output_dir: Optional[Union[str, Path]] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Architecture wrapper alias for frontend parity."""
    return migrate_single_project(
        filename=project_path,
        output_dir=output_dir,
        **kwargs,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Migrate Looker LookML to Power BI TMDL"
    )
    parser.add_argument(
        "input_path",
        help="Path to LookML project directory or view file"
    )
    parser.add_argument(
        "-o", "--output",
        default="./output",
        help="Output directory"
    )
    parser.add_argument(
        "-n", "--name",
        help="Model name"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    args = parser.parse_args()

    def cli_progress(phase: str, percent: int, message: str):
        if args.verbose or percent in (0, 25, 50, 75, 100):
            print(f"[{percent:3d}%] {phase}: {message}")

    input_path = Path(args.input_path)

    if input_path.is_dir():
        result = migrate_lookml_project(
            project_path=input_path,
            output_dir=args.output,
            model_name=args.name,
            progress_callback=cli_progress,
        )
    else:
        result = migrate_lookml_view(
            view_path=input_path,
            output_dir=args.output,
            model_name=args.name,
            progress_callback=cli_progress,
        )

    if result.success:
        print(f"\nMigration successful!")
        print(f"  Output: {result.output_path}")
        print(f"  Tables: {result.tables_count}")
        print(f"  Measures: {result.measures_count}")
    else:
        print(f"\nMigration failed!")
        for error in result.errors:
            print(f"  Error: {error.message}")
        exit(1)
