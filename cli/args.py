#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command line argument parsing module.

This module provides functionality to parse and validate command line arguments
for the database conversion tool.
"""

import argparse
import sys
from pathlib import Path


def parse_arguments():
    """
    Parse command line arguments.

    Returns:
        Parsed arguments object
    """
    parser = argparse.ArgumentParser(
        description='Convert database files (SQLite & plist) to JSON or CSV format for forensic analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Process current directory to JSON
  %(prog)s -i /path/to/evidence              # Process specific directory to JSON
  %(prog)s -i file.db                        # Process single database file to JSON
  %(prog)s -i evidence -o output -v          # Verbose output to specific directory
  %(prog)s -s size -st                       # Sort by size with strict error handling
  %(prog)s -f csv                            # Output to CSV format
  %(prog)s -i file.db -f csv -v              # Process single file to CSV with verbose output
  %(prog)s -t 4                              # Use 4 threads for parallel processing
  %(prog)s -t 0                              # Auto-detect optimal thread count
        """
    )

    parser.add_argument(
        '-i', '--input',
        default=Path(".").resolve(),
        type=str,
        help='Input file or directory path (default: current directory)'
    )

    parser.add_argument(
        '-o', '--output',
        default=Path(".").resolve(),
        type=str,
        help='Output directory path (default: current directory)'
    )

    parser.add_argument(
        '-s', '--sorted',
        default="mtime",
        type=str,
        choices=['mtime', 'ctime', 'atime', 'size'],
        help='Sorting criteria: mtime (modification time), ctime (creation time), atime (access time), size (default: mtime)'
    )

    parser.add_argument(
        '-f', '--format',
        default="json",
        type=str,
        choices=['json', 'csv'],
        help='Output format: json (default) or csv'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging output'
    )

    parser.add_argument(
        '-st', '--strict',
        action='store_true',
        help='Exit on errors instead of continuing processing'
    )

    parser.add_argument(
        '-t', '--threads',
        default=1,
        type=int,
        help='Number of threads for parallel processing (default: 1, use 0 for auto-detection)'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    return parser.parse_args()