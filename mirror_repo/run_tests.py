#!/usr/bin/env python3
"""
Test runner for Looker Migrator.

Runs all test suites and generates reports.
"""

import os
import sys
import unittest
import argparse
from pathlib import Path


def discover_tests(test_dir: str = "tests", pattern: str = "test_*.py"):
    """Discover and load tests from directory."""
    loader = unittest.TestLoader()
    suite = loader.discover(test_dir, pattern=pattern)
    return suite


def run_tests(
    verbosity: int = 2,
    test_dir: str = "tests",
    pattern: str = "test_*.py",
    failfast: bool = False,
):
    """
    Run test suite.

    Args:
        verbosity: Test output verbosity (0-2)
        test_dir: Directory containing tests
        pattern: Pattern for test file discovery
        failfast: Stop on first failure

    Returns:
        True if all tests passed, False otherwise
    """
    # Ensure project root is in path
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))

    # Discover tests
    suite = discover_tests(test_dir, pattern)

    # Run tests
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        failfast=failfast,
    )
    result = runner.run(suite)

    return result.wasSuccessful()


def run_specific_test(test_name: str, verbosity: int = 2):
    """
    Run a specific test module or test case.

    Args:
        test_name: Name of test module or test case
        verbosity: Output verbosity

    Returns:
        True if test passed
    """
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))

    loader = unittest.TestLoader()

    try:
        # Try to load as a module
        if "." in test_name:
            suite = loader.loadTestsFromName(test_name)
        else:
            # Try to load as a test file
            suite = loader.loadTestsFromName(f"tests.{test_name}")
    except Exception as e:
        print(f"Error loading test: {e}")
        return False

    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)

    return result.wasSuccessful()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Looker Migrator tests"
    )
    parser.add_argument(
        "-v", "--verbosity",
        type=int,
        default=2,
        choices=[0, 1, 2],
        help="Test output verbosity"
    )
    parser.add_argument(
        "-t", "--test",
        help="Run specific test module or test case"
    )
    parser.add_argument(
        "-p", "--pattern",
        default="test_*.py",
        help="Pattern for test discovery"
    )
    parser.add_argument(
        "--failfast",
        action="store_true",
        help="Stop on first failure"
    )
    parser.add_argument(
        "--migration",
        action="store_true",
        help="Run only migration tests"
    )
    parser.add_argument(
        "--validation",
        action="store_true",
        help="Run only validation tests"
    )
    parser.add_argument(
        "--api",
        action="store_true",
        help="Run only API tests"
    )

    args = parser.parse_args()

    # Determine which tests to run
    if args.test:
        success = run_specific_test(args.test, args.verbosity)
    elif args.migration:
        success = run_specific_test("test_looker_migration", args.verbosity)
    elif args.validation:
        success = run_specific_test("test_validation", args.verbosity)
    elif args.api:
        success = run_specific_test("test_api", args.verbosity)
    else:
        success = run_tests(
            verbosity=args.verbosity,
            pattern=args.pattern,
            failfast=args.failfast,
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
