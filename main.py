#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DBS2JSON - Database to JSON Converter for Forensic Analysis

This script converts SQLite and plist database files to JSON format for digital forensic analysis.
It processes directories of database files, extracts their contents, and outputs structured JSON data.

Author: Forensic Tools
License: MIT
Version: 1.0.0
"""

import sys
from pathlib import Path
from loguru import logger

from core.processor import DatabaseProcessor
from exporters.json_exporter import JSONExporter
from exporters.csv_exporter import CSVExporter
from cli.args import parse_arguments


def main() -> None:
    """
    Main function to orchestrate the database to JSON conversion process.

    This function handles command line argument parsing, input validation,
    and coordinates the file discovery, processing, and output stages.
    """
    # Parse command line arguments
    args = parse_arguments()

    # Set up paths
    input_path = Path(args.input)
    output_path = Path(args.output).resolve()

    # Configure logging based on verbosity
    if args.verbose:
        logger.remove()
        logger.add(
            sys.stderr,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level="DEBUG"
        )

    # Validate input path
    if not input_path.exists():
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)

    if not (input_path.is_dir() or input_path.is_file()):
        logger.error(f"Input path is not a file or directory: {input_path}")
        sys.exit(1)

    # Validate output path
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as e:
        logger.error(f"Cannot create output directory {output_path}: {e}")
        sys.exit(1)

    logger.info(f"Starting analysis of: {input_path}")
    logger.info(f"Output directory: {output_path}")

    # Initialize database processor
    processor = DatabaseProcessor()
    processor.set_paths(input_path, output_path)

    if input_path.is_file():
        # Process single file
        logger.info("Processing single file...")
        processor.process_single_file(input_path, verbose=args.verbose)
        key_files = processor.get_key_files()
        if not key_files:
            logger.error("File is not a supported SQLite or plist file")
            sys.exit(1)
        logger.info(f"Processed 1 database file")
    else:
        # Process directory (original logic)
        logger.info("Stage 1: Discovering database files...")
        processor.discover_database_files(input_path, base_path=input_path, verbose=args.verbose)

        key_files = processor.get_key_files()
        if not key_files:
            logger.warning("No SQLite or plist files found")
            sys.exit(0)

        logger.info(f"Found {len(key_files)} database files")

        # Stage 2: Process discovered files
        logger.info("Stage 2: Processing database files...")
        processor.process_database_files(args.sorted, args.verbose, args.strict)

    # Stage 3: Save results
    logger.info("Stage 3: Saving results...")

    if args.format == 'json':
        exporter = JSONExporter(output_path)
        success = exporter.export_results(key_files, input_path)
    elif args.format == 'csv':
        exporter = CSVExporter(output_path)
        success = exporter.export_results_to_csv(key_files, input_path)
    else:
        logger.warning(f"Unsupported format: {args.format}")
        success = False

    if not success:
        logger.error("Failed to export results")
        sys.exit(1)

    # Report encrypted files
    encrypted_files = processor.get_encrypted_files()
    if len(encrypted_files) > 0:
        logger.warning("Found potentially encrypted SQLite files:")
        for encrypted_file in encrypted_files:
            logger.warning(f"  - {encrypted_file}")

    logger.success("Analysis completed successfully")


if __name__ == "__main__":
    main()