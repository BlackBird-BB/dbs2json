#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON export module.

This module provides functionality to export processed database data to JSON format.
"""

import json
import os
from loguru import logger
from pathlib import Path
from typing import Dict, Any

from utils.helpers import sanitize_path_for_filename


class JSONExporter:
    """
    JSON exporter class for handling JSON output operations.
    """

    def __init__(self, output_path: Path):
        """
        Initialize the JSON exporter.

        Args:
            output_path: Directory path for output files
        """
        self.output_path = output_path

    def export_results(self, key_files: Dict[str, Any], input_path: Path) -> bool:
        """
        Export processed data to JSON format files.

        For small datasets (<20 files): Creates a single JSON file
        For large datasets: Creates a directory structure with separate JSON files

        Args:
            key_files: Dictionary containing processed file data
            input_path: Input path for filename generation

        Returns:
            True if successful, False otherwise
        """
        try:
            if len(key_files) > 20:
                # Large dataset: create directory with separate files
                output_dir = self.output_path / sanitize_path_for_filename(input_path)
                if not output_dir.exists():
                    os.mkdir(output_dir)
                    logger.info(f"Created output directory: {output_dir}")

                for db_name, db_con in key_files.items():
                    file_path = output_dir / (
                        sanitize_path_for_filename(db_name) + ".json"
                    )
                    try:
                        with open(file_path, "w", encoding="utf-8") as json_file:
                            json.dump(db_con, json_file, ensure_ascii=False, indent=2)
                        logger.success(f"Created: {file_path}")
                    except (TypeError, ValueError) as e:
                        logger.error(f"JSON serialization error for {db_name}: {e}")
                        # Fallback: write as string representation
                        try:
                            with open(file_path, "w", encoding="utf-8") as f:
                                f.write(str(db_con))
                            logger.warning(f"Fallback write completed for: {file_path}")
                        except Exception as fallback_error:
                            logger.error(
                                f"Fallback write failed for {file_path}: {fallback_error}"
                            )
                            return False

            else:
                # Small dataset: create single JSON file
                file_path = self.output_path / (
                    sanitize_path_for_filename(input_path) + ".json"
                )

                if file_path.exists():
                    try:
                        choice = (
                            input(
                                "There is already an analysis result. Do you want to overwrite it? (Y/n): "
                            )
                            .lower()
                            .strip()
                        )
                        if choice in ["n", "no"]:
                            logger.info("Operation cancelled by user")
                            return False
                    except (KeyboardInterrupt, EOFError):
                        logger.info("Operation cancelled by user")
                        return False

                try:
                    with open(file_path, "w", encoding="utf-8") as json_file:
                        json.dump(key_files, json_file, ensure_ascii=False, indent=2)
                    logger.success(f"Created output file: {file_path}")
                except (TypeError, ValueError) as e:
                    logger.error(f"JSON serialization error: {e}")
                    logger.error("Failed to create output file")
                    return False

            return True

        except Exception as e:
            logger.error(f"Error during JSON export: {e}")
            return False
