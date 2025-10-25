#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database processing module.

This module provides the main processing logic for discovering and analyzing database files.
"""

import os
import uuid
import datetime
import plistlib
from loguru import logger
from pathlib import Path
from shutil import copyfile
from tempfile import mkstemp
from typing import Dict, List, Any

from core.database import extract_sqlite_to_dict, extract_plist_to_dict
from utils.file_detector import (
    detect_sqlite_file,
    detect_plist_file,
    detect_encrypted_sqlite_file,
)
from utils.helpers import format_timestamp_to_date


class DatabaseProcessor:
    """
    Main database processor class that handles file discovery and content extraction.
    """

    def __init__(self):
        """Initialize the database processor."""
        self.key_files = (
            {}
        )  # Dictionary to store processed file information and contents
        self.encrypt_files = []  # List of potentially encrypted SQLite files
        self.path_n = None  # Path for temporary files
        self.inp = None  # Input path
        self.opt = None  # Output path

    def discover_database_files(
        self, folder_path: Path, base_path: Path, verbose: bool = False
    ) -> None:
        """
        Recursively scan directory for SQLite and plist files.

        This function identifies and collects metadata for SQLite and plist files,
        and tracks potentially encrypted SQLite databases.

        Args:
            folder_path: Directory path to scan
            base_path: Base path for relative path calculations
            verbose: Enable detailed logging output
        """
        try:
            for file_path in folder_path.iterdir():
                if file_path.is_file():
                    if detect_sqlite_file(file_path):
                        stat = file_path.stat()
                        self.key_files[str(file_path.relative_to(base_path))] = {
                            "info": {
                                "path": str(file_path.resolve()),
                                "type": "sqlite",
                                "size": stat.st_size,
                                "st_mtime": stat.st_mtime,
                                "mtime": format_timestamp_to_date(stat.st_mtime),
                                "st_atime": stat.st_atime,
                                "atime": format_timestamp_to_date(stat.st_atime),
                                "st_ctime": stat.st_ctime,
                                "ctime": format_timestamp_to_date(stat.st_ctime),
                            }
                        }
                        if verbose:
                            logger.success(
                                f"{file_path} is sqlite file.\n{self.key_files[str(file_path.relative_to(base_path))]['info']}"
                            )

                    elif detect_plist_file(file_path):
                        stat = file_path.stat()
                        self.key_files[str(file_path.relative_to(base_path))] = {
                            "info": {
                                "path": str(file_path.resolve()),
                                "type": "plist",
                                "size": stat.st_size,
                                "st_mtime": stat.st_mtime,
                                "mtime": format_timestamp_to_date(stat.st_mtime),
                                "st_atime": stat.st_atime,
                                "atime": format_timestamp_to_date(stat.st_atime),
                                "st_ctime": stat.st_ctime,
                                "ctime": format_timestamp_to_date(stat.st_ctime),
                            }
                        }
                        if verbose:
                            logger.success(
                                f"{file_path} is plist file.\n{self.key_files[str(file_path.relative_to(base_path))]['info']}"
                            )

                    elif detect_encrypted_sqlite_file(file_path):
                        if verbose:
                            logger.info(f"{file_path} is encrypted sqlite file.")
                        self.encrypt_files.append(str(file_path.relative_to(base_path)))

                elif file_path.is_dir():
                    # Recursively process subdirectories
                    self.discover_database_files(file_path, base_path, verbose)
                elif verbose:
                    logger.info(f"{file_path} is not KEY file.")

        except (PermissionError, OSError) as e:
            logger.error(f"Error accessing directory {folder_path}: {e}")

    def process_binary_data_in_dict(self, dic: Dict[str, Any]) -> None:
        """
        Recursively process dictionary to decode binary data and extract nested plists.

        This function traverses through dictionary values and:
        - Converts binary plist data to dictionaries
        - Decodes UTF-8 text data
        - Saves large binary blobs to temporary files and references them by path

        Args:
            dic: Dictionary to process (modified in-place)
        """
        for k, v in dic.items():
            if isinstance(v, bytes):
                # Check if binary data is a plist
                if v[:6] == b"bplist":
                    dic[k] = plistlib.loads(v)
                else:
                    try:
                        # Try to decode as UTF-8 text
                        dic[k] = v.decode("utf-8")
                    except (UnicodeDecodeError, AttributeError):
                        # Save binary blob to temporary file
                        if not Path(self.path_n).exists():
                            os.mkdir(self.path_n)
                        tmp_file_name = self.path_n / str(uuid.uuid1())
                        with open(tmp_file_name, "wb") as f:
                            f.write(v)
                        dic[k] = str(tmp_file_name.resolve())

            # Recursively process nested dictionaries
            if isinstance(v, dict):
                self.process_binary_data_in_dict(v)

            if isinstance(v, datetime.datetime):
                # stringify datetime objects
                dic[k] = str(v)

    def process_database_files(
        self, sorted_flag: str = "mtime", verbose: bool = False, strict: bool = False
    ) -> None:
        """
        Process identified database files and extract their contents.

        This function sorts the discovered files, converts them to JSON format,
        and enhances the data with post-processing.

        Args:
            sorted_flag: Sorting criteria (mtime, ctime, atime, size)
            verbose: Enable detailed logging output
            strict: Exit on errors instead of continuing
        """
        # Define sorting function based on criteria
        if sorted_flag == "mtime":
            func = lambda key: self.key_files[key]["info"]["st_mtime"]
        elif sorted_flag == "ctime":
            func = lambda key: self.key_files[key]["info"]["st_ctime"]
        elif sorted_flag == "atime":
            func = lambda key: self.key_files[key]["info"]["st_atime"]
        elif sorted_flag == "size":
            func = lambda key: self.key_files[key]["info"]["size"]
        else:
            logger.warning(f"Unknown sorting flag: {sorted_flag}, using mtime")
            func = lambda key: self.key_files[key]["info"]["st_mtime"]

        # Create temporary file for processing
        _, tmp_file = mkstemp()

        try:
            # Sort files by specified criteria (largest first)
            key_files_name = sorted(self.key_files, key=func, reverse=True)

            for db_name in key_files_name:
                # Copy file to temporary location for processing
                copyfile(self.key_files[db_name]["info"]["path"], tmp_file)

                try:
                    if verbose:
                        logger.info(f"Start to analyze {db_name}")

                    # Process based on file type
                    if self.key_files[db_name]["info"]["type"] == "sqlite":
                        db_json = extract_sqlite_to_dict(db_name, tmp_file, strict)
                    elif self.key_files[db_name]["info"]["type"] == "plist":
                        db_json = extract_plist_to_dict(tmp_file)
                    else:
                        logger.warning(
                            f"Unknown file type: {self.key_files[db_name]['info']['type']}"
                        )
                        continue

                    if verbose:
                        logger.info(f"{db_name} analyze done.")

                except Exception as e:
                    db_json = {}
                    logger.error(f"{db_name} analyze error.")
                    logger.error(f"File info: {self.key_files[db_name]}")
                    logger.error(f"Error: {e}")

                    if strict:
                        logger.error(
                            "Strict mode enabled, exiting due to processing error"
                        )
                        exit(-1)

                # Store processed content
                self.key_files[db_name]["content"] = db_json

            # Post-process all data to decode binary content and extract nested plists
            self.process_binary_data_in_dict(self.key_files)

        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file)
            except OSError:
                pass

    def set_paths(self, input_path: Path, output_path: Path) -> None:
        """
        Set input and output paths for processing.

        Args:
            input_path: Input directory path
            output_path: Output directory path
        """
        self.inp = input_path
        self.opt = output_path
        self.path_n = output_path

    def get_key_files(self) -> Dict[str, Any]:
        """Get the discovered and processed key files."""
        return self.key_files

    def get_encrypted_files(self) -> List[str]:
        """Get the list of potentially encrypted files."""
        return self.encrypt_files

    def process_single_file(self, file_path: Path, verbose: bool = False) -> None:
        """
        Process a single file and extract its contents.

        This function determines the file type, processes it accordingly,
        and stores the results in the key_files dictionary.

        Args:
            file_path: Path to the file to process
            verbose: Enable detailed logging output
        """
        # Check if it's a SQLite file
        if detect_sqlite_file(file_path):
            stat = file_path.stat()
            file_name = file_path.name
            self.key_files[file_name] = {
                "info": {
                    "path": str(file_path.resolve()),
                    "type": "sqlite",
                    "size": stat.st_size,
                    "st_mtime": stat.st_mtime,
                    "mtime": format_timestamp_to_date(stat.st_mtime),
                    "st_atime": stat.st_atime,
                    "atime": format_timestamp_to_date(stat.st_atime),
                    "st_ctime": stat.st_ctime,
                    "ctime": format_timestamp_to_date(stat.st_ctime),
                }
            }

            if verbose:
                logger.success(
                    f"{file_path} is SQLite file.\n{self.key_files[file_name]['info']}"
                )

            # Extract content
            try:
                if verbose:
                    logger.info(f"Start to analyze {file_name}")

                db_json = extract_sqlite_to_dict(
                    file_name, str(file_path), strict=False
                )

                if verbose:
                    logger.info(f"{file_name} analyze done.")

                self.key_files[file_name]["content"] = db_json

                # Post-process to decode binary content
                self.process_binary_data_in_dict(self.key_files[file_name]["content"])

            except Exception as e:
                db_json = {}
                logger.error(f"{file_name} analyze error.")
                logger.error(f"File info: {self.key_files[file_name]}")
                logger.error(f"Error: {e}")
                self.key_files[file_name]["content"] = db_json

        # Check if it's a plist file
        elif detect_plist_file(file_path):
            stat = file_path.stat()
            file_name = file_path.name
            self.key_files[file_name] = {
                "info": {
                    "path": str(file_path.resolve()),
                    "type": "plist",
                    "size": stat.st_size,
                    "st_mtime": stat.st_mtime,
                    "mtime": format_timestamp_to_date(stat.st_mtime),
                    "st_atime": stat.st_atime,
                    "atime": format_timestamp_to_date(stat.st_atime),
                    "st_ctime": stat.st_ctime,
                    "ctime": format_timestamp_to_date(stat.st_ctime),
                }
            }

            if verbose:
                logger.success(
                    f"{file_path} is plist file.\n{self.key_files[file_name]['info']}"
                )

            # Extract content
            try:
                if verbose:
                    logger.info(f"Start to analyze {file_name}")

                db_json = extract_plist_to_dict(str(file_path))

                if verbose:
                    logger.info(f"{file_name} analyze done.")

                self.key_files[file_name]["content"] = db_json

                # Post-process to decode binary content
                self.process_binary_data_in_dict(self.key_files[file_name]["content"])

            except Exception as e:
                db_json = {}
                logger.error(f"{file_name} analyze error.")
                logger.error(f"File info: {self.key_files[file_name]}")
                logger.error(f"Error: {e}")
                self.key_files[file_name]["content"] = db_json

        # Check if it's an encrypted SQLite file
        elif detect_encrypted_sqlite_file(file_path):
            if verbose:
                logger.info(f"{file_path} is encrypted SQLite file.")
            self.encrypt_files.append(str(file_path.name))
            # Don't add to key_files since it can't be processed
        else:
            if verbose:
                logger.info(f"{file_path} is not a supported database file.")
