#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database extraction module.

This module provides functions to extract data from SQLite and plist database files.
"""

import sqlite3
import plistlib
from loguru import logger
from typing import Dict, List, Any


def extract_sqlite_to_dict(
    db_name: str, sqlite_file: str, strict: bool = False
) -> Dict[str, List[List[Any]]]:
    """
    Extract SQLite database content to dictionary format.

    Args:
        db_name: Name of the database for logging purposes
        sqlite_file: Path to the SQLite file
        strict: If True, exit on errors instead of continuing

    Returns:
        Dictionary with table names as keys and data as values
    """
    db_content = {}
    connection = None

    try:
        connection = sqlite3.connect(sqlite_file)
        cur = connection.cursor()

        # Get all table names
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cur.fetchall()
        tables_name = [i[0] for i in tables]

        # Process each table
        for table_name in tables_name:
            try:
                # Get column information
                cur.execute(f"pragma table_info({table_name})")
                columns_name = [i[1] for i in cur.fetchall()]

                # Get table data
                cur.execute(f"SELECT * FROM {table_name}")
                rows = cur.fetchall()

                # Process rows and decode bytes
                rows_n = []
                for row in rows:
                    row_n = []
                    for item in row:
                        if isinstance(item, bytes):
                            row_n.append(item.decode("utf-8", errors="ignore"))
                        else:
                            row_n.append(item)
                    rows_n.append(row_n)

                # Insert column names as first row
                rows_n.insert(0, columns_name)
                db_content[table_name] = rows_n

            except sqlite3.OperationalError as e:
                if "no such module" in str(e):
                    logger.warning(
                        f"Skipping table {table_name} from {db_name} due to missing module"
                    )
                    continue
                logger.warning(
                    f"Select table {table_name} from {db_name} error with: {e}"
                )
                if strict:
                    logger.error("Strict mode enabled, exiting due to database error")
                    exit(-1)
                else:
                    continue

    except Exception as e:
        logger.error(f"Database connection error for {db_name}: {e}")
        if strict:
            exit(-1)
    finally:
        if connection:
            connection.close()

    return db_content


def extract_plist_to_dict(plist_file: str) -> Dict[str, Any]:
    """
    Extract plist file content to dictionary format.

    Args:
        plist_file: Path to the plist file

    Returns:
        Dictionary containing plist data
    """
    with open(plist_file, "rb") as f:
        return plistlib.load(f)
