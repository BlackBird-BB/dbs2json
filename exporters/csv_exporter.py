#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV export module.

This module provides functionality to export processed database data to CSV format.
"""

import csv
import os
from loguru import logger
from pathlib import Path
from typing import Dict, List, Any

from utils.helpers import sanitize_path_for_filename, flatten_dict_for_csv


class CSVExporter:
    """
    CSV exporter class for handling CSV output operations.
    """

    def __init__(self, output_path: Path):
        """
        Initialize the CSV exporter.

        Args:
            output_path: Directory path for output files
        """
        self.output_path = output_path

    def export_sqlite_tables_to_csv(self, sqlite_data: Dict[str, List[List[Any]]], csv_file_path: Path) -> bool:
        """
        Write SQLite data to CSV files.

        For each table in the SQLite database, creates a separate CSV file.

        Args:
            sqlite_data: Dictionary containing table data
            csv_file_path: Base path for CSV files (without extension)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory for CSV files
            csv_dir = csv_file_path.parent
            csv_dir.mkdir(parents=True, exist_ok=True)

            for table_name, table_data in sqlite_data.items():
                if not table_data or len(table_data) < 1:
                    logger.warning(f"No data found for table: {table_name}")
                    continue

                # First row contains column names
                if len(table_data) < 1:
                    continue

                headers = table_data[0] if isinstance(table_data[0], list) else []
                rows = table_data[1:] if len(table_data) > 1 else []

                # Create CSV file for this table
                table_csv_path = csv_file_path.parent / f"{csv_file_path.stem}_{table_name}.csv"

                with open(table_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

                    # Write headers
                    if headers:
                        writer.writerow(headers)

                    # Write data rows
                    for row in rows:
                        # Convert all items to strings and handle special characters
                        csv_row = []
                        for item in row:
                            if item is None:
                                csv_row.append('')
                            else:
                                str_item = str(item)
                                # Replace newlines and tabs to avoid CSV formatting issues
                                str_item = str_item.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                csv_row.append(str_item)
                        writer.writerow(csv_row)

                logger.success(f"Created CSV for table '{table_name}': {table_csv_path}")

            return True

        except Exception as e:
            logger.error(f"Error writing SQLite data to CSV: {e}")
            return False

    def export_plist_to_csv(self, plist_data: Dict[str, Any], csv_file_path: Path) -> bool:
        """
        Write plist data to CSV file.

        Flattens nested plist structure and writes to CSV format.

        Args:
            plist_data: Dictionary containing plist data
            csv_file_path: Path for output CSV file

        Returns:
            True if successful, False otherwise
        """
        try:
            # Flatten the plist data
            flattened_data = flatten_dict_for_csv(plist_data)

            if not flattened_data:
                logger.warning("No data to write to CSV")
                return False

            # Create directory for CSV file
            csv_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Get all unique keys from all flattened records
            all_keys = set()
            for record in flattened_data:
                all_keys.update(record.keys())

            # Sort keys for consistent column order
            sorted_keys = sorted(all_keys)

            with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=sorted_keys, quoting=csv.QUOTE_ALL)
                writer.writeheader()

                # Write each record
                for record in flattened_data:
                    # Ensure all keys are present in each record (empty string if missing)
                    complete_record = {key: record.get(key, '') for key in sorted_keys}
                    writer.writerow(complete_record)

            logger.success(f"Created CSV: {csv_file_path}")
            return True

        except Exception as e:
            logger.error(f"Error writing plist data to CSV: {e}")
            return False

    def export_results_to_csv(self, key_files: Dict[str, Any], input_path: Path) -> bool:
        """
        Save processed data to CSV format files.

        For SQLite files: Creates separate CSV files for each table
        For plist files: Creates flattened CSV representation

        Args:
            key_files: Dictionary containing processed file data
            input_path: Input path for directory naming

        Returns:
            True if successful, False otherwise
        """
        try:
            csv_dir = self.output_path / f"{sanitize_path_for_filename(input_path)}_csv"
            csv_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created CSV output directory: {csv_dir}")

            for db_name, db_con in key_files.items():
                if 'content' not in db_con or not db_con['content']:
                    logger.warning(f"No content found for {db_name}")
                    continue

                base_filename = sanitize_path_for_filename(db_name)
                base_path = csv_dir / base_filename

                try:
                    if db_con["info"]["type"] == "sqlite":
                        # SQLite: create separate CSV for each table
                        self.export_sqlite_tables_to_csv(db_con['content'], base_path)
                    elif db_con["info"]["type"] == "plist":
                        # Plist: create single flattened CSV
                        csv_path = csv_dir / f"{base_filename}.csv"
                        self.export_plist_to_csv(db_con['content'], csv_path)
                    else:
                        logger.warning(f"Unsupported file type for CSV export: {db_con['info']['type']}")

                except Exception as e:
                    logger.error(f"Error exporting {db_name} to CSV: {e}")
                    return False

            logger.success(f"CSV export completed. Files saved to: {csv_dir}")
            return True

        except Exception as e:
            logger.error(f"Error during CSV export: {e}")
            return False