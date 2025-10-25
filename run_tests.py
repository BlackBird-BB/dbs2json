#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test runner script for dbs2json project.
"""

import sys
import subprocess
from pathlib import Path


def main():
    """Run the test suite with appropriate options."""

    # Check if pytest is available
    try:
        import pytest
    except ImportError:
        print("Error: pytest is not installed. Please install it with:")
        print("pip install pytest")
        sys.exit(1)

    # Define pytest arguments
    pytest_args = [
        "tests/",
        "-v",                    # Verbose output
        "--tb=short",           # Short traceback format
        "--strict-markers",     # Strict marker enforcement
        "--disable-warnings",   # Disable warnings
    ]

    # Add coverage if requested
    if "--cov" in sys.argv:
        try:
            import pytest_cov
        except ImportError:
            print("Warning: pytest-cov not installed. Install with: pip install pytest-cov")
            sys.argv.remove("--cov")

    # Add any additional arguments from command line
    user_args = [arg for arg in sys.argv[1:] if not arg.startswith("--test-")]
    pytest_args.extend(user_args)

    # Run tests
    print("Running dbs2json test suite...")
    print("=" * 50)

    try:
        result = pytest.main(pytest_args)
        sys.exit(result)
    except Exception as e:
        print(f"Error running tests: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()