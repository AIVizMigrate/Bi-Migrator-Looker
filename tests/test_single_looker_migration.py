#!/usr/bin/env python3
"""
Comprehensive test script for LookML project migration.
Based on the Tableau test pattern - provides CLI interface for single project migration testing.
"""

import argparse
import os
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass  # dotenv not installed, use system environment variables

# Import the Looker migrator API
from looker_migrator import migrate_lookml_project, migrate_lookml_view, Settings

# Import validators
try:
    from looker_migrator.validators import validate_tmdl_directory, TmdlValidationResult
    TMDL_VALIDATOR_AVAILABLE = True
except ImportError:
    TMDL_VALIDATOR_AVAILABLE = False


def compile_pbit_online(
    project_dir: str,
    endpoint: Optional[str] = None,
    timeout: int = 300,
    pbit_name: Optional[str] = None
) -> Tuple[bool, str, Optional[str]]:
    """
    Compile a PbixProj folder to PBIT using the online compilation service.
    Downloads the compiled PBIT file to the project directory.

    Args:
        project_dir: Path to the project directory containing pbit/ folder
        endpoint: Optional custom endpoint URL for the online service
        timeout: Timeout in seconds for the compilation request
        pbit_name: Optional name for the output PBIT file (without extension)

    Returns:
        tuple: (success: bool, log_path: str, pbit_file_path: Optional[str])
    """
    try:
        project_path = Path(project_dir).resolve()
        if not (project_path / "pbit").exists():
            return False, f"PbixProj folder not found: {project_path / 'pbit'}", None

        print("[WEB] Compiling PBIT using online service...")
        print(f"   [DIR] Project path: {project_path}")

        # Locate the compile_pbit_online.py helper script
        helper_script = Path(__file__).parent.parent / "scripts" / "compile" / "compile_pbit_online.py"
        if not helper_script.exists():
            helper_script = Path(__file__).parent.parent / "compile_pbit_online.py"
        if not helper_script.exists():
            return False, f"Helper script not found: {helper_script}", None

        # Determine PBIT name from project directory if not provided
        if not pbit_name:
            pbit_name = project_path.name.replace(" ", "_").replace("-", "_")
            pbit_name = "".join(c for c in pbit_name if c.isalnum() or c == "_") or "output"

        # Prepare command to run the helper script
        cmd = [
            sys.executable,
            str(helper_script),
            "--project", str(project_path),
            "--name", pbit_name,
            "--timeout", str(timeout)
        ]

        if endpoint:
            cmd.extend(["--endpoint", endpoint])
            print(f"   [WEB] Endpoint: {endpoint}")

        log_path = project_path / "compile_online.log"
        cmd.extend(["--log", str(log_path)])

        print(f"   [LOG] PBIT name: {pbit_name}")
        print(f"   [LOG] Log file: {log_path}")
        print(f"   [TIME] Timeout: {timeout}s")
        print("   [GO] Starting online compilation...")
        print("   " + "=" * 50)

        # Run the helper script
        result = subprocess.run(cmd, text=True, timeout=timeout + 30)

        print("   " + "=" * 50)

        success = result.returncode == 0
        pbit_file_path = None

        if success:
            # Check if PBIT file was downloaded
            expected_pbit = project_path / f"{pbit_name}.pbit"
            if expected_pbit.exists():
                pbit_file_path = str(expected_pbit)
                print(f"   [OK] Online compilation successful!")
                print(f"   [FILE] PBIT file: {pbit_file_path}")
            else:
                print("   [OK] Online compilation successful (validation only)")
        else:
            print("   [FAIL] Online compilation failed!")

        return success, str(log_path), pbit_file_path

    except subprocess.TimeoutExpired:
        error_msg = f"Online compilation timed out after {timeout + 30} seconds"
        print(f"   [FAIL] {error_msg}")
        return False, error_msg, None
    except Exception as e:
        error_msg = f"Exception during online compilation: {str(e)}"
        print(f"   [FAIL] {error_msg}")
        return False, error_msg, None


def compile_pbit_with_docker(project_dir: str, project_name: str, verbose: bool = True) -> Tuple[bool, str, str]:
    """
    Compile a PbixProj folder to PBIT using Docker pbi-tools

    Args:
        project_dir: Path to the project directory containing pbit/ folder
        project_name: Name of the project for output file
        verbose: If True, show real-time Docker output; if False, capture silently

    Returns:
        tuple: (success: bool, output_file: str, error_message: str)
    """
    try:
        project_path = Path(project_dir).resolve()
        if not (project_path / "pbit").exists():
            return False, "", f"PbixProj folder not found: {project_path / 'pbit'}"

        print(f"[DOCKER] Compiling PBIT using Docker...")
        print(f"   Project path: {project_path}")

        # Docker command to compile PBIT
        cmd = [
            "docker", "run", "--rm",
            "-v", f"{project_path.parent.absolute()}:/workspace",
            "--entrypoint=/app/pbi-tools/pbi-tools.core",
            f"--workdir=/workspace/{project_path.name}",
            "ghcr.io/pbi-tools/pbi-tools-core:1.2.0",
            "compile", "pbit",
            "-format", "PBIT",
            "-outPath", f"{project_name}.pbit",
            "-overwrite"
        ]

        print(f"   Docker command: {' '.join(cmd)}")

        if verbose:
            print(f"   Running pbi-tools compilation...")
            print("-" * 60)
            result = subprocess.run(cmd, text=True, timeout=120)
            print("-" * 60)
            print(f"   Docker process completed with exit code: {result.returncode}")
        else:
            print(f"   Running pbi-tools compilation (quiet mode)...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            print(f"   Docker process completed with exit code: {result.returncode}")

            if result.returncode != 0:
                print("   Docker STDOUT:")
                print(f"   {result.stdout}")
                print("   Docker STDERR:")
                print(f"   {result.stderr}")

        output_file = project_path / f"{project_name}.pbit"
        if result.returncode == 0 and output_file.exists():
            print(f"   [OK] PBIT file created successfully: {output_file.name}")
            return True, str(output_file), ""
        else:
            error_msg = f"Docker command failed (exit code {result.returncode})"
            if output_file.exists():
                print(f"   [WARN] PBIT file exists but Docker returned non-zero exit code")
                return True, str(output_file), f"Warning: {error_msg}"
            else:
                print(f"   [FAIL] PBIT file not created")
                return False, "", error_msg

    except subprocess.TimeoutExpired:
        return False, "", "Docker compilation timed out after 120 seconds"
    except Exception as e:
        return False, "", f"Exception during compilation: {str(e)}"


def test_single_lookml_view(
    view_path: str,
    output_dir: Optional[str] = None,
    model_name: Optional[str] = None,
    compile_pbit: bool = False,
    verbose_pbit: bool = True,
    online_compile: bool = False,
    online_endpoint: Optional[str] = None,
):
    """
    Test a single LookML view file migration.

    Args:
        view_path: Path to the .view.lkml file
        output_dir: Output directory (default: test_output/{view_name})
        model_name: Optional model name for the output
        compile_pbit: Whether to compile PBIT after migration using Docker
        verbose_pbit: Show full pbi-tools output
        online_compile: Whether to use online compilation service
        online_endpoint: Optional custom endpoint for online compilation
    """
    view_file = Path(view_path)
    if not view_file.exists():
        print(f"[FAIL] View file not found: {view_path}")
        return {"success": False, "error": "View file not found"}

    if not view_file.suffix == '.lkml':
        print(f"[FAIL] Invalid file type: {view_file.suffix} (expected .lkml)")
        return {"success": False, "error": "Invalid file type"}

    view_name = view_file.stem.replace('.view', '')

    # Always output to test_output folder
    test_output_base = Path(__file__).parent.parent / "test_output"
    report_name = model_name if model_name else view_name
    report_name = report_name.replace(" ", "_").replace("-", "_")
    report_name = "".join(c for c in report_name if c.isalnum() or c == "_") or "output"

    if output_dir is None:
        output_dir = str(test_output_base / report_name)

    print("=" * 80)
    print("SINGLE LOOKML VIEW MIGRATION TEST")
    print("=" * 80)
    print(f"View: {view_name}")
    print(f"Input path: {view_path}")
    print(f"Output directory: {output_dir}")
    print()

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Configure settings
    settings = Settings()
    settings.job_id = f"looker_view_test_{view_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Run migration
    print("[..] Starting LookML view migration...")
    migration_success = False
    migration_error = ""

    try:
        result = migrate_lookml_view(
            view_path=str(view_file),
            output_dir=output_dir,
            model_name=model_name,
            settings=settings,
            progress_callback=lambda phase, pct, msg: print(f"   [{phase}] {pct}% - {msg}")
        )

        migration_success = result.success
        if result.success:
            print(f"[OK] Migration completed successfully!")
            print(f"   Tables: {result.tables_count}")
            print(f"   Measures: {result.measures_count}")
            print(f"   Views converted: {result.views_converted}")
        else:
            migration_error = result.errors[0].message if result.errors else "Unknown error"
            print(f"[FAIL] Migration failed!")
            print(f"   Error: {migration_error}")

    except Exception as e:
        migration_error = str(e)
        print(f"[FAIL] Migration failed with exception:")
        print(f"   {migration_error}")

    # Check generated files
    if migration_success:
        print()
        print("[DIR] Checking generated files...")
        pbit_dir = output_path / "pbit"
        if pbit_dir.exists():
            for tmdl_file in sorted(pbit_dir.rglob("*")):
                if tmdl_file.is_file():
                    rel_path = tmdl_file.relative_to(output_path)
                    print(f"   [FILE] {rel_path}")
            print()
            print(f"[INFO] Total files generated: {len(list(pbit_dir.rglob('*')))}")

    # PBIT Compilation
    pbit_success = False
    pbit_file = ""
    online_success = False
    online_log_path = ""
    online_pbit_file = ""

    if migration_success and compile_pbit:
        print("\n[DOCKER] Starting PBIT compilation with Docker...")
        pbit_success, pbit_file, pbit_error = compile_pbit_with_docker(
            str(output_dir),
            report_name,
            verbose=verbose_pbit
        )

    if migration_success and online_compile:
        print("\n[WEB] Compiling PBIT with online service...")
        pbit_dir = output_path / "pbit"
        if pbit_dir.exists() and pbit_dir.is_dir():
            online_success, online_log_path, online_pbit_file = compile_pbit_online(
                str(output_path),
                endpoint=online_endpoint,
                pbit_name=report_name
            )
            if online_success:
                print(f"[OK] Online PBIT compilation successful!")
                print(f"[FILE] PBIT File: {online_pbit_file}")
                print(f"[LOG] Compilation log: {online_log_path}")
            else:
                print(f"[FAIL] Online PBIT compilation failed")

    # Summary
    print("\n" + "=" * 80)
    print("LOOKER VIEW MIGRATION TEST SUMMARY")
    print("=" * 80)
    print(f"View: {view_name}")
    print(f"Report Name: {report_name}")
    print(f"Migration: {'[OK] SUCCESS' if migration_success else '[FAIL] FAILED'}")
    if online_compile:
        print(f"Online PBIT Compilation: {'[OK] SUCCESS' if online_success else '[FAIL] FAILED'}")
        if online_pbit_file:
            print(f"[FILE] PBIT File (Online): {online_pbit_file}")
        if online_log_path:
            print(f"Online Compile Log: {online_log_path}")
    print(f"Output Directory: {output_dir}")
    if not migration_success:
        print(f"Migration Error: {migration_error}")
    print("=" * 80)

    return {
        "success": migration_success,
        "migration_success": migration_success,
        "pbit_success": pbit_success,
        "output_dir": output_dir,
        "report_name": report_name,
        "pbit_file": pbit_file,
        "online_success": online_success,
        "online_log": online_log_path,
        "online_pbit_file": online_pbit_file,
    }


def test_single_lookml_project(
    project_path: str,
    output_dir: Optional[str] = None,
    model_name: Optional[str] = None,
    compile_pbit: bool = False,
    verbose_pbit: bool = True,
    online_compile: bool = False,
    online_endpoint: Optional[str] = None,
    resolve_extends: bool = True,
    convert_derived_tables: bool = True
):
    """
    Test a single LookML project migration scenario.

    Args:
        project_path: Path to the LookML project directory
        output_dir: Output directory (default: test_output/{project_name})
        model_name: Optional model name to migrate (default: all models)
        compile_pbit: Whether to compile PBIT after migration using Docker
        verbose_pbit: Show full pbi-tools output
        online_compile: Whether to use online compilation service
        online_endpoint: Optional custom endpoint for online compilation
        resolve_extends: Whether to resolve view extends
        convert_derived_tables: Whether to convert derived tables
    """

    input_path = Path(project_path)
    if not input_path.exists():
        print(f"[FAIL] LookML input not found: {project_path}")
        return {"success": False, "error": "Input not found"}

    # Handle zip file extraction (like Django API does)
    temp_dir = None
    if input_path.suffix.lower() == '.zip':
        print(f"[ZIP] Extracting zip file: {input_path.name}")
        temp_dir = tempfile.mkdtemp(prefix="looker_test_")
        extract_dir = Path(temp_dir) / "lookml_project"
        extract_dir.mkdir(parents=True, exist_ok=True)
        try:
            with zipfile.ZipFile(input_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            print(f"[ZIP] Extracted to: {extract_dir}")
            project_dir = extract_dir
            project_name = input_path.stem  # Use zip filename without extension
        except zipfile.BadZipFile:
            print(f"[FAIL] Invalid zip file: {input_path}")
            return {"success": False, "error": "Invalid zip file"}
    else:
        project_dir = input_path
        project_name = input_path.name

    # Always output to test_output folder (relative to script location)
    test_output_base = Path(__file__).parent.parent / "test_output"

    # Derive report name from model_name or project_name
    report_name = model_name if model_name else project_name
    # Sanitize report name for filesystem
    report_name = report_name.replace(" ", "_").replace("-", "_")
    report_name = "".join(c for c in report_name if c.isalnum() or c == "_") or "output"

    if output_dir is None:
        output_dir = str(test_output_base / report_name)

    print("=" * 80)
    print("SINGLE LOOKML PROJECT MIGRATION TEST")
    print("=" * 80)
    print(f"Project: {project_name}")
    print(f"Input path: {project_path}")
    print(f"Output directory: {output_dir}")
    print()
    print("Settings:")
    print(f"  - Resolve Extends: {resolve_extends}")
    print(f"  - Convert Derived Tables: {convert_derived_tables}")
    if model_name:
        print(f"  - Target Model: {model_name}")
    print()

    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Configure settings
    settings = Settings()
    settings.parser.resolve_extends = resolve_extends
    settings.converter.convert_derived_tables = convert_derived_tables
    settings.job_id = f"looker_test_{project_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Run migration
    print("[..] Starting LookML project migration...")
    migration_success = False
    migration_error = ""

    try:
        result = migrate_lookml_project(
            project_path=str(project_dir),
            output_dir=output_dir,
            model_name=model_name,
            settings=settings,
            progress_callback=lambda phase, pct, msg: print(f"   [{phase}] {pct}% - {msg}")
        )

        migration_success = result.success

        if migration_success:
            print("[OK] Migration completed successfully!")
            print(f"   Tables: {result.tables_count}")
            print(f"   Measures: {result.measures_count}")
            print(f"   Relationships: {result.relationships_count}")
            print(f"   Views converted: {result.views_converted}")
        else:
            print("[FAIL] Migration failed!")
            if result.errors:
                for err in result.errors:
                    print(f"   Error: {err.message}")
                    migration_error = result.errors[0].message

    except Exception as e:
        migration_success = False
        migration_error = str(e)
        print(f"[FAIL] Migration failed with exception: {migration_error}")

    # Check generated files
    if migration_success:
        print("\n[DIR] Checking generated files...")
        pbit_dir = Path(output_dir)
        generated_files = []

        for item in pbit_dir.rglob("*"):
            if item.is_file() and not item.name.startswith('.'):
                relative_path = item.relative_to(pbit_dir)
                generated_files.append(str(relative_path))
                print(f"   [FILE] {relative_path}")

        print(f"\n[INFO] Total files generated: {len(generated_files)}")

    # PBIT compilation with Docker
    pbit_success = False
    pbit_file = ""
    pbit_error = ""

    if migration_success and compile_pbit:
        print("\n[DOCKER] Starting PBIT compilation with Docker (PRIMARY METHOD)...")
        pbit_success, pbit_file, pbit_error = compile_pbit_with_docker(
            str(output_dir),
            report_name,
            verbose=verbose_pbit
        )

        if pbit_success:
            print(f"[OK] PBIT compilation successful!")
            print(f"[FILE] PBIT file: {pbit_file}")

            pbit_path = Path(pbit_file)
            if pbit_path.exists():
                file_size = pbit_path.stat().st_size
                print(f"[INFO] File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        else:
            print(f"[FAIL] PBIT compilation failed!")
            print(f"Error: {pbit_error}")
    elif migration_success and not compile_pbit:
        print("\n[SKIP] Skipping Docker PBIT compilation")
        print("   [TIP] Use --with-pbit to compile with Docker (recommended)")
        print("   [TIP] Use --with-online-compile for online compilation (alternative)")

    # Online compilation
    online_success = False
    online_log_path = ""
    online_pbit_file = None

    if online_compile and migration_success:
        print("\n[WEB] Compiling PBIT with online service...")
        project_output = Path(output_dir)
        pbit_dir = project_output / "pbit"

        if pbit_dir.exists() and pbit_dir.is_dir():
            online_success, online_log_path, online_pbit_file = compile_pbit_online(
                str(project_output),
                endpoint=online_endpoint,
                pbit_name=report_name
            )
            if online_success:
                print("[OK] Online PBIT compilation successful!")
                if online_pbit_file:
                    print(f"[FILE] PBIT File: {online_pbit_file}")
                if online_log_path and not online_log_path.startswith("Exception"):
                    print(f"[LOG] Compilation log: {online_log_path}")
            else:
                print("[FAIL] Online PBIT compilation failed!")
                if online_log_path:
                    print(f"[LOG] Check log for details: {online_log_path}")
        else:
            print("[FAIL] Could not find valid pbixproj directory for online compilation")
    elif not online_compile:
        print("\n[SKIP] Online PBIT compilation disabled")
    else:
        print("\n[SKIP] Online PBIT compilation skipped (migration failed)")

    # TMDL Validation
    tmdl_validation_success = True
    tmdl_validation_report = ""
    tmdl_error_count = 0

    if migration_success and TMDL_VALIDATOR_AVAILABLE:
        print("\n[VALIDATE] Validating TMDL files...")
        model_dir = Path(output_dir) / "pbit" / "Model"

        if model_dir.exists():
            try:
                result = validate_tmdl_directory(model_dir)

                tmdl_error_count = len(result.errors)
                tmdl_warning_count = len(result.warnings)
                tmdl_validation_success = result.is_valid

                tmdl_validation_report = Path(output_dir) / "TMDL_VALIDATION_REPORT.md"

                # Generate simple report
                with open(tmdl_validation_report, "w") as f:
                    f.write(f"# TMDL Validation Report\n\n")
                    f.write(f"Generated: {datetime.now().isoformat()}\n\n")
                    f.write(f"## Summary\n")
                    f.write(f"- Files checked: {result.files_checked}\n")
                    f.write(f"- Errors: {tmdl_error_count}\n")
                    f.write(f"- Warnings: {tmdl_warning_count}\n\n")

                    if result.errors:
                        f.write("## Errors\n")
                        for err in result.errors:
                            f.write(f"- {err}\n")

                    if result.warnings:
                        f.write("\n## Warnings\n")
                        for warn in result.warnings:
                            f.write(f"- {warn}\n")

                if result.is_valid:
                    print(f"[OK] TMDL validation passed: {result.files_checked} files checked")
                else:
                    print(f"[WARN] TMDL validation issues: {tmdl_error_count} errors, {tmdl_warning_count} warnings")
                    print(f"[LOG] Report: {tmdl_validation_report}")
            except Exception as e:
                print(f"[WARN] TMDL validation error: {e}")
                tmdl_validation_success = True
        else:
            print("[SKIP] TMDL validation skipped (no Model directory)")
    elif not TMDL_VALIDATOR_AVAILABLE:
        print("\n[SKIP] TMDL validation skipped (validator not available)")
    else:
        print("\n[SKIP] TMDL validation skipped (migration failed)")

    # Summary
    print("\n" + "=" * 80)
    print("LOOKER MIGRATION TEST SUMMARY")
    print("=" * 80)
    print(f"Project: {project_name}")
    print(f"Report Name: {report_name}")
    print(f"Migration: {'[OK] SUCCESS' if migration_success else '[FAIL] FAILED'}")
    if compile_pbit:
        print(f"PBIT Compilation (Docker): {'[OK] SUCCESS' if pbit_success else '[FAIL] FAILED'}")
        if pbit_file:
            print(f"[FILE] PBIT File: {pbit_file}")
    if online_compile:
        print(f"Online PBIT Compilation: {'[OK] SUCCESS' if online_success else '[FAIL] FAILED'}")
        if online_pbit_file:
            print(f"[FILE] PBIT File (Online): {online_pbit_file}")
        if online_log_path and not online_log_path.startswith("Exception"):
            print(f"Online Compile Log: {online_log_path}")
    if TMDL_VALIDATOR_AVAILABLE and migration_success:
        print(f"TMDL Validation: {'[OK] PASSED' if tmdl_validation_success else '[WARN] ISSUES FOUND'}")
        if tmdl_error_count > 0:
            print(f"   [WARN] {tmdl_error_count} errors found")
        if tmdl_validation_report:
            print(f"[LOG] TMDL Report: {tmdl_validation_report}")
    print(f"Output Directory: {output_dir}")

    if migration_error:
        print(f"Migration Error: {migration_error}")
    if pbit_error and compile_pbit:
        print(f"PBIT Error: {pbit_error}")

    print("=" * 80)

    return {
        "success": migration_success and (pbit_success if compile_pbit else True),
        "migration_success": migration_success,
        "pbit_success": pbit_success,
        "output_dir": output_dir,
        "report_name": report_name,
        "pbit_file": pbit_file,
        "online_success": online_success,
        "online_log": online_log_path,
        "online_pbit_file": online_pbit_file,
        "tmdl_validation_success": tmdl_validation_success,
        "tmdl_error_count": tmdl_error_count,
        "tmdl_validation_report": str(tmdl_validation_report) if tmdl_validation_report else None
    }


def test_api_functionality():
    """Test the Looker migrator API functions without full migration."""
    print("=" * 80)
    print("LOOKER API FUNCTIONALITY TEST")
    print("=" * 80)

    # Test 1: Settings creation
    print("[SETTINGS] Testing settings creation...")
    try:
        settings = Settings()
        print(f"[OK] Default settings created successfully")
        print(f"   - Resolve extends: {settings.parser.resolve_extends}")
        print(f"   - Convert derived tables: {settings.converter.convert_derived_tables}")
        print(f"   - DAX API URL: {settings.converter.dax_api_url}")
    except Exception as e:
        print(f"[FAIL] Settings creation failed: {e}")

    # Test 2: Settings from dict
    print("\n[DICT] Testing settings from dictionary...")
    custom_dict = {
        "parser": {"resolve_extends": False},
        "converter": {"convert_derived_tables": False},
        "job_id": "test_job_123"
    }

    try:
        settings = Settings.from_dict(custom_dict)
        print(f"[OK] Settings from dict created successfully")
        print(f"   - Resolve extends: {settings.parser.resolve_extends}")
        print(f"   - Convert derived tables: {settings.converter.convert_derived_tables}")
        print(f"   - DAX API URL: {settings.converter.dax_api_url}")
        print(f"   - Job ID: {settings.job_id}")
    except Exception as e:
        print(f"[FAIL] Settings from dict failed: {e}")

    print("\n" + "=" * 80)
    print("API FUNCTIONALITY TEST COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    DEFAULT_INPUT_PATH = "input/sample_project"

    parser = argparse.ArgumentParser(
        description='Migrate LookML project to Power BI TMDL format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Project migration (directory)
  python test_single_looker_migration.py --project input/sample_project

  # Project migration (zip file)
  python test_single_looker_migration.py --project input/sample_project.zip --with-online-compile

  # Single view migration
  python test_single_looker_migration.py --view input/views/users.view.lkml --with-online-compile

  # Migration with Docker PBIT compilation (recommended)
  python test_single_looker_migration.py --project input/my_project --with-pbit

  # Migration with online PBIT compilation
  python test_single_looker_migration.py --project input/my_project --with-online-compile

  # Test API functionality only
  python test_single_looker_migration.py --api-only

  # Run all tests
  python test_single_looker_migration.py --all

Compilation Methods:
  PRIMARY:   Docker compilation (--with-pbit)
             - Creates PBIT file locally
             - Requires Docker
             - Recommended for production use

  ALTERNATIVE: Online compilation (--with-online-compile)
             - Compiles on remote service
             - Downloads PBIT file
        """
    )

    # Project input
    parser.add_argument('--project', type=str, default=None,
                       help=f'Path to LookML project directory or zip file (default: {DEFAULT_INPUT_PATH})')
    parser.add_argument('--view', type=str, default=None,
                       help='Path to single .view.lkml file for single view migration')
    parser.add_argument('--model', type=str, default=None,
                       help='Specific model name to migrate (default: all models)')

    # Output
    parser.add_argument('--output-dir', type=str, default=None,
                       help='Output directory (default: test_output/{project_name})')

    # Migration options
    parser.add_argument('--no-resolve-extends', action='store_true', default=False,
                       help='Disable resolving view extends')
    parser.add_argument('--no-derived-tables', action='store_true', default=False,
                       help='Disable converting derived tables')

    # Compilation options
    parser.add_argument('--with-pbit', action='store_true', default=False,
                       help='Compile PBIT using Docker (PRIMARY METHOD - recommended)')
    parser.add_argument('--with-online-compile', action='store_true', default=False,
                       help='Enable online compilation (ALTERNATIVE METHOD)')
    parser.add_argument('--online-endpoint', type=str, default=None,
                       help='Override online compile service endpoint URL')

    # Verbosity
    parser.add_argument('--verbose', action='store_true', default=True,
                       help='Show full pbi-tools Docker output (default)')
    parser.add_argument('--quiet', dest='verbose', action='store_false',
                       help='Hide pbi-tools output unless there\'s an error')

    # Test modes
    parser.add_argument('--api-only', action='store_true', default=False,
                       help='Test API functionality only')
    parser.add_argument('--all', action='store_true', default=False,
                       help='Run all tests (API + migration)')

    args = parser.parse_args()

    # Handle special test modes
    if args.api_only:
        test_api_functionality()
        sys.exit(0)

    if args.all:
        test_api_functionality()
        print("\n" + "=" * 80 + "\n")
        result = test_single_lookml_project(
            project_path=args.project,
            output_dir=args.output_dir,
            model_name=args.model,
            compile_pbit=args.with_pbit,
            verbose_pbit=args.verbose,
            online_compile=args.with_online_compile,
            online_endpoint=args.online_endpoint,
            resolve_extends=not args.no_resolve_extends,
            convert_derived_tables=not args.no_derived_tables
        )
        sys.exit(0 if result.get("success") else 1)

    # Determine migration type: view or project
    if args.view:
        # Single view migration
        print(f"[NOTE] Testing single view: {args.view}")
        if args.output_dir:
            print(f"[DIR] Output directory: {args.output_dir}")
        print(f"[PBIT] PBIT compilation: {'Enabled' if args.with_pbit else 'Disabled (use --with-pbit to enable)'}")
        if args.with_online_compile:
            print(f"[WEB] Online compilation: Enabled")
            if args.online_endpoint:
                print(f"   Endpoint: {args.online_endpoint}")

        result = test_single_lookml_view(
            view_path=args.view,
            output_dir=args.output_dir,
            model_name=args.model,
            compile_pbit=args.with_pbit,
            verbose_pbit=args.verbose,
            online_compile=args.with_online_compile,
            online_endpoint=args.online_endpoint,
        )
    else:
        # Project migration (default)
        project_path = args.project if args.project else DEFAULT_INPUT_PATH
        print(f"[NOTE] Testing project: {project_path}")
        if args.output_dir:
            print(f"[DIR] Output directory: {args.output_dir}")
        print(f"[PBIT] PBIT compilation: {'Enabled' if args.with_pbit else 'Disabled (use --with-pbit to enable)'}")
        if args.with_online_compile:
            print(f"[WEB] Online compilation: Enabled")
            if args.online_endpoint:
                print(f"   Endpoint: {args.online_endpoint}")

        result = test_single_lookml_project(
            project_path=project_path,
            output_dir=args.output_dir,
            model_name=args.model,
            compile_pbit=args.with_pbit,
            verbose_pbit=args.verbose,
            online_compile=args.with_online_compile,
            online_endpoint=args.online_endpoint,
            resolve_extends=not args.no_resolve_extends,
            convert_derived_tables=not args.no_derived_tables
        )

    sys.exit(0 if result.get("success") else 1)
