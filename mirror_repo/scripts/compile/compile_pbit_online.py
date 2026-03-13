#!/usr/bin/env python3
"""
Online PBIT Compilation Script

Packages a pbixproj (with a pbit/ folder) into a ZIP and uploads it
to the online compile service:
  - Health:  https://pbi-tools-for-agents-production.up.railway.app/health
  - Compile (logs only): https://pbi-tools-for-agents-production.up.railway.app/compile
  - Compile (download PBIT): https://pbi-tools-for-agents-production.up.railway.app/compile/pbit

Usage examples

1) Health check:
   ./compile_pbit_online.py --health

2) Package, upload, and download PBIT file (DEFAULT):
   ./compile_pbit_online.py --project ./test_output/My_Project
   # Downloads: ./test_output/My_Project/My_Project.pbit

3) Specify custom output name:
   ./compile_pbit_online.py --project ./test_output/My_Project --name "Sales_Dashboard"
   # Downloads: ./test_output/My_Project/Sales_Dashboard.pbit

4) Validate only (no PBIT download, just logs):
   ./compile_pbit_online.py --project ./test_output/My_Project --validate-only

5) Upload a pre-built ZIP:
   ./compile_pbit_online.py --zip /tmp/myproj.zip --name "My_Report"

6) Provide a public URL to a ZIP:
   ./compile_pbit_online.py --zip-url https://your-bucket/pbi/myproj.zip --name "My_Report"

The script downloads the compiled PBIT file and saves it to the project directory.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Optional, Tuple
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED


DEFAULT_ENDPOINT = "https://pbi-tools-for-agents-production.up.railway.app"


def run_cmd(cmd: list, cwd: Optional[str] = None, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=check)


def is_tool(name: str) -> bool:
    return shutil.which(name) is not None


def safe_name(name: str) -> str:
    """Convert a name to a safe filename."""
    s = name.strip().replace(" ", "_").replace("-", "_")
    return "".join(c for c in s if c.isalnum() or c == "_") or "project"


def validate_pbit_structure(project_dir: Path) -> Tuple[bool, str]:
    pbit_dir = project_dir / "pbit"
    if not pbit_dir.is_dir():
        return False, f"Expected directory not found: {pbit_dir}"

    required = [
        pbit_dir / ".pbixproj.json",
        pbit_dir / "Model",
        pbit_dir / "Report",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        return False, f"Missing required files/folders in pbit/: {', '.join(missing)}"

    return True, ""


def make_zip_from_project(project_dir: Path, out_zip: Path) -> Path:
    pbit_dir = project_dir / "pbit"
    if not pbit_dir.exists():
        raise FileNotFoundError(f"pbit folder not found in: {project_dir}")

    out_zip.parent.mkdir(parents=True, exist_ok=True)

    # Ensure 'pbit/' is the root inside the ZIP
    with ZipFile(out_zip, "w", compression=ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(pbit_dir):
            for f in files:
                abs_path = Path(root) / f
                # Arcname includes the 'pbit/' prefix
                arcname = Path("pbit") / abs_path.relative_to(pbit_dir)
                zf.write(abs_path, arcname)

    return out_zip


def curl_health(endpoint: str) -> Tuple[bool, str]:
    if not is_tool("curl"):
        return False, "curl not found in PATH"
    url = f"{endpoint.rstrip('/')}/health"
    res = run_cmd(["curl", "-sS", url])
    ok = res.returncode == 0
    return ok, res.stdout if res.stdout else res.stderr


def curl_compile_validate(endpoint: str, zip_path: Path, log_path: Path, timeout: int = 300) -> Tuple[bool, str]:
    """
    Compile and return logs only (validation mode).
    Uses /compile endpoint.
    """
    if not is_tool("curl"):
        return False, "curl not found in PATH"

    url = f"{endpoint.rstrip('/')}/compile"

    cmd = [
        "curl", "-fSL", "-X", "POST", url,
        "-F", f"file=@{zip_path}"
    ]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        output_lines: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            output_lines.append(line)
        proc.wait(timeout=timeout)
        combined = "".join(output_lines)
    except subprocess.TimeoutExpired:
        proc.kill()
        return False, "Request timed out"

    log_path.write_text(combined, encoding="utf-8")

    # Service indicates success when this phrase appears in logs
    success = bool(re.search(r"PBIT file written to:", combined)) and proc.returncode == 0
    return success, combined


def curl_compile_pbit(
    endpoint: str,
    zip_path: Path,
    output_pbit_path: Path,
    pbit_name: str,
    log_path: Path,
    timeout: int = 300
) -> Tuple[bool, str, Optional[Path]]:
    """
    Compile and download the PBIT file.

    Two-step process:
    1. Call /compile to get detailed logs (streamed to stdout)
    2. Call /compile/pbit to download the PBIT file

    Returns:
        Tuple of (success, message, pbit_file_path)
    """
    if not is_tool("curl"):
        return False, "curl not found in PATH", None

    # Ensure output directory exists
    output_pbit_path.parent.mkdir(parents=True, exist_ok=True)

    # STEP 1: Call /compile to get detailed logs
    print(f"\n   [NOTE] STEP 1: Getting compile logs...")
    compile_url = f"{endpoint.rstrip('/')}/compile"

    compile_cmd = [
        "curl", "-fSL", "-X", "POST", compile_url,
        "-F", f"file=@{zip_path}"
    ]

    print(f"   [UPLOAD] Uploading to: {compile_url}")

    try:
        # Stream output to both stdout and capture it
        proc = subprocess.Popen(compile_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        output_lines: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)  # Stream to terminal
            sys.stdout.flush()
            output_lines.append(line)
        proc.wait(timeout=timeout)
        compile_output = "".join(output_lines)

        # Save compile logs
        log_path.write_text(compile_output, encoding="utf-8")
        print(f"\n   [LOG] Compile logs saved to: {log_path}")

        # Check if compilation was successful
        compile_success = bool(re.search(r"PBIT file written to:", compile_output)) and proc.returncode == 0

        if not compile_success:
            return False, f"Compilation failed. See logs: {log_path}", None

    except subprocess.TimeoutExpired:
        proc.kill()
        return False, "Compile request timed out", None
    except Exception as e:
        return False, f"Compile error: {str(e)}", None

    # STEP 2: Call /compile/pbit to download PBIT
    print(f"\n   [DOWNLOAD] STEP 2: Downloading PBIT file...")
    pbit_url = f"{endpoint.rstrip('/')}/compile/pbit"

    pbit_cmd = [
        "curl", "-fSL", "-X", "POST", pbit_url,
        "-F", f"file=@{zip_path}",
        "-F", f"name={pbit_name}",
        "-o", str(output_pbit_path),
        "-w", "%{http_code}",
        "-D", str(log_path.with_suffix('.headers'))
    ]

    print(f"   [UPLOAD] Uploading to: {pbit_url}")
    print(f"   [LOG] PBIT name: {pbit_name}")
    print(f"   [SAVE] Output: {output_pbit_path}")

    try:
        result = subprocess.run(
            pbit_cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        http_code = result.stdout.strip()

        if result.returncode == 0 and http_code == "200":
            # Check if we got a valid PBIT file (should be > 1KB typically)
            if output_pbit_path.exists():
                file_size = output_pbit_path.stat().st_size
                if file_size > 1000:  # Minimum reasonable PBIT size
                    # Append download success to log
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(
                            f"\n\n=== PBIT DOWNLOAD ===\n"
                            f"HTTP Status: {http_code}\n"
                            f"Output file: {output_pbit_path}\n"
                            f"File size: {file_size} bytes\n"
                        )
                    return True, f"PBIT downloaded: {output_pbit_path} ({file_size} bytes)", output_pbit_path
                else:
                    # Small file likely means error response as text
                    error_content = output_pbit_path.read_text(encoding="utf-8", errors="replace")
                    with open(log_path, "a", encoding="utf-8") as f:
                        f.write(
                            f"\n\n=== PBIT DOWNLOAD ERROR ===\n"
                            f"HTTP Status: {http_code}\n"
                            f"Response:\n{error_content}\n"
                        )
                    output_pbit_path.unlink()  # Remove invalid file
                    return False, f"PBIT download failed: {error_content[:500]}", None
            else:
                return False, "Output file not created", None
        else:
            # HTTP error - read the error response
            error_msg = f"HTTP {http_code}"
            if output_pbit_path.exists():
                try:
                    error_content = output_pbit_path.read_text(encoding="utf-8", errors="replace")
                    error_msg = error_content[:1000]
                    output_pbit_path.unlink()  # Remove error file
                except:
                    pass

            with open(log_path, "a", encoding="utf-8") as f:
                f.write(
                    f"\n\n=== PBIT DOWNLOAD HTTP ERROR ===\n"
                    f"HTTP Status: {http_code}\n"
                    f"Error: {error_msg}\n"
                    f"Stderr: {result.stderr}\n"
                )
            return False, error_msg, None

    except subprocess.TimeoutExpired:
        error_msg = f"PBIT download timed out after {timeout} seconds"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n\n=== TIMEOUT ===\n{error_msg}\n")
        return False, error_msg, None
    except Exception as e:
        error_msg = f"PBIT download exception: {str(e)}"
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"\n\n=== EXCEPTION ===\n{error_msg}\n")
        return False, error_msg, None


def curl_compile_pbit_url(
    endpoint: str,
    zip_url: str,
    output_pbit_path: Path,
    pbit_name: str,
    log_path: Path,
    timeout: int = 300
) -> Tuple[bool, str, Optional[Path]]:
    """
    Compile from URL and download the PBIT file.
    Uses /compile/pbit endpoint with url parameter.
    """
    if not is_tool("curl"):
        return False, "curl not found in PATH", None

    url = f"{endpoint.rstrip('/')}/compile/pbit"

    output_pbit_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "curl", "-fSL", "-X", "POST", url,
        "-F", f"url={zip_url}",
        "-F", f"name={pbit_name}",
        "-o", str(output_pbit_path),
        "-w", "%{http_code}"
    ]

    print(f"   [UPLOAD] Uploading URL to: {url}")
    print(f"   [LINK] ZIP URL: {zip_url}")
    print(f"   [LOG] PBIT name: {pbit_name}")
    print(f"   [SAVE] Output: {output_pbit_path}")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        http_code = result.stdout.strip()

        if result.returncode == 0 and http_code == "200":
            if output_pbit_path.exists():
                file_size = output_pbit_path.stat().st_size
                if file_size > 1000:
                    log_path.write_text(
                        f"PBIT compilation successful!\n"
                        f"HTTP Status: {http_code}\n"
                        f"Output file: {output_pbit_path}\n"
                        f"File size: {file_size} bytes\n",
                        encoding="utf-8"
                    )
                    return True, f"PBIT downloaded: {output_pbit_path} ({file_size} bytes)", output_pbit_path
                else:
                    error_content = output_pbit_path.read_text(encoding="utf-8", errors="replace")
                    log_path.write_text(f"Compilation failed:\n{error_content}\n", encoding="utf-8")
                    output_pbit_path.unlink()
                    return False, f"Compilation failed: {error_content[:500]}", None

        error_msg = f"HTTP {http_code}"
        if output_pbit_path.exists():
            try:
                error_msg = output_pbit_path.read_text(encoding="utf-8", errors="replace")[:1000]
                output_pbit_path.unlink()
            except:
                pass
        log_path.write_text(f"Compilation failed: {error_msg}\n", encoding="utf-8")
        return False, error_msg, None

    except subprocess.TimeoutExpired:
        return False, "Request timed out", None
    except Exception as e:
        return False, str(e), None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Online PBIT compiler - compiles and downloads PBIT files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compile and download PBIT (default behavior)
  %(prog)s --project ./test_output/My_Project

  # Compile with custom output name
  %(prog)s --project ./test_output/My_Project --name "Sales_Report"

  # Validate only (no download)
  %(prog)s --project ./test_output/My_Project --validate-only

  # Health check
  %(prog)s --health
"""
    )
    parser.add_argument("--project", type=str, help="Path to project dir containing pbit/", default=None)
    parser.add_argument("--zip", dest="zip_path", type=str, help="Path to pre-built ZIP containing pbit/ at root", default=None)
    parser.add_argument("--zip-url", dest="zip_url", type=str, help="Public URL to a ZIP containing pbit/ at root", default=None)
    parser.add_argument("--name", type=str, help="Output PBIT filename (without .pbit extension)", default=None)
    parser.add_argument("--output-dir", dest="output_dir", type=str, help="Directory to save PBIT file (default: project dir)", default=None)
    parser.add_argument("--endpoint", type=str, default=DEFAULT_ENDPOINT, help="Base URL of compile service")
    parser.add_argument("--log", type=str, default=None, help="Path to save compile log")
    parser.add_argument("--timeout", type=int, default=300, help="Timeout in seconds for compile request")
    parser.add_argument("--health", action="store_true", help="Check service health and exit")
    parser.add_argument("--validate-only", dest="validate_only", action="store_true",
                       help="Only validate (return logs), don't download PBIT file")

    args = parser.parse_args()

    if args.health:
        ok, msg = curl_health(args.endpoint)
        print(msg.strip())
        return 0 if ok else 1

    if args.zip_url and (args.project or args.zip_path):
        print("--zip-url is mutually exclusive with --project/--zip", file=sys.stderr)
        return 2

    # Determine project name and paths
    zip_path: Optional[Path] = None
    project_dir: Optional[Path] = None
    pbit_name: str = args.name or "output"

    if args.zip_path:
        zip_path = Path(args.zip_path).expanduser().resolve()
        if not zip_path.exists():
            print(f"ZIP not found: {zip_path}", file=sys.stderr)
            return 2
        if not args.name:
            pbit_name = safe_name(zip_path.stem)

    elif args.project:
        project_dir = Path(args.project).expanduser().resolve()
        if not project_dir.exists():
            print(f"Project directory not found: {project_dir}", file=sys.stderr)
            return 2

        ok, msg = validate_pbit_structure(project_dir)
        if not ok:
            print(f"Invalid pbit structure: {msg}", file=sys.stderr)
            return 2

        if not args.name:
            pbit_name = safe_name(project_dir.name)

        tmp_dir = Path(tempfile.gettempdir()) / pbit_name
        tmp_dir.mkdir(parents=True, exist_ok=True)
        zip_path = tmp_dir / f"{pbit_name}.zip"

        print(f"Packaging pbixproj into ZIP: {zip_path}")
        make_zip_from_project(project_dir, zip_path)

    elif args.zip_url:
        if not args.name:
            # Try to extract name from URL
            url_path = args.zip_url.split('/')[-1].split('?')[0]
            pbit_name = safe_name(Path(url_path).stem) if url_path else "output"
    else:
        print("Provide either --project, --zip, or --zip-url", file=sys.stderr)
        return 2

    # Determine output paths
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
    elif project_dir:
        output_dir = project_dir
    else:
        output_dir = Path.cwd()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_pbit_path = output_dir / f"{pbit_name}.pbit"
    log_path = Path(args.log) if args.log else (output_dir / "compile_online.log")

    print(f"\n[WEB] Online PBIT Compilation")
    print(f"   [DIR] Output directory: {output_dir}")
    print(f"   [FILE] PBIT filename: {pbit_name}.pbit")
    print(f"   [LOG] Log file: {log_path}")
    print(f"   [TIME]  Timeout: {args.timeout}s")
    print()

    # Validate-only mode (logs only, no PBIT download)
    if args.validate_only:
        print("Validate-only mode (no PBIT download)")
        if args.zip_url:
            print("   Note: --validate-only with --zip-url uses /compile endpoint")
            print("   [FAIL] --validate-only with --zip-url not fully implemented")
            return 2
        else:
            assert zip_path is not None
            print(f"   [UPLOAD] Uploading to: {args.endpoint}/compile")
            success, msg = curl_compile_validate(args.endpoint, zip_path, log_path, timeout=args.timeout)
            print()
            if success:
                print("[OK] Validation successful! TMDL compiles correctly.")
            else:
                print("[FAIL] Validation failed!")
            print(f"[LOG] Log: {log_path}")
            return 0 if success else 1

    # Download PBIT mode (default)
    print("[DOWNLOAD] Compile and download PBIT file")

    if args.zip_url:
        success, msg, pbit_path = curl_compile_pbit_url(
            args.endpoint, args.zip_url, output_pbit_path, pbit_name, log_path, timeout=args.timeout
        )
    else:
        assert zip_path is not None
        success, msg, pbit_path = curl_compile_pbit(
            args.endpoint, zip_path, output_pbit_path, pbit_name, log_path, timeout=args.timeout
        )

    print()
    if success and pbit_path:
        print("=" * 60)
        print("[OK] PBIT COMPILATION SUCCESSFUL!")
        print("=" * 60)
        print(f"   [FILE] PBIT File: {pbit_path}")
        print(f"   [INFO] File size: {pbit_path.stat().st_size:,} bytes")
        print(f"   [LOG] Log: {log_path}")
        print()
        print("   [TIP] Open in Power BI Desktop to use the report")
        print("=" * 60)
    else:
        print("=" * 60)
        print("[FAIL] PBIT COMPILATION FAILED!")
        print("=" * 60)
        print(f"   Error: {msg}")
        print(f"   [LOG] Log: {log_path}")
        print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
