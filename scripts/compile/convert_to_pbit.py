#!/usr/bin/env python3
"""
Convert migrator output to a Power BI template (.pbit).

Example:
    python scripts/compile/convert_to_pbit.py ./output --name my_model
"""

from __future__ import annotations

import argparse
from pathlib import Path

from compile_pbit_online import DEFAULT_ENDPOINT, PbitCompiler


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Convert generated output to .pbit")
    parser.add_argument(
        "project",
        help="Path to migrator output directory (contains pbit/)",
    )
    parser.add_argument(
        "--name",
        "-n",
        help="Output PBIT name (without extension) when --output is not set",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Explicit output .pbit file path",
    )
    parser.add_argument(
        "--endpoint",
        "-e",
        default=DEFAULT_ENDPOINT,
        help="PBIT compiler service endpoint",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate project only, do not compile",
    )
    args = parser.parse_args()

    project_dir = Path(args.project)
    if not project_dir.exists():
        print(f"Error: Project directory not found: {project_dir}")
        raise SystemExit(1)

    output_path = Path(args.output) if args.output else None
    compiler = PbitCompiler(endpoint=args.endpoint)

    result = compiler.compile(
        project_dir=project_dir,
        output_name=args.name,
        output_path=output_path,
        validate_only=args.validate_only,
    )

    if result.get("success"):
        if result.get("pbit_path"):
            print(f"PBIT created: {result['pbit_path']}")
        else:
            print("Validation successful")
        raise SystemExit(0)

    print(f"Compilation failed: {result.get('error', 'Unknown error')}")
    for issue in result.get("issues", []):
        print(f"  - {issue}")
    if result.get("details"):
        print(f"Details: {result['details']}")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
