#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database extraction and SQLite snapshot helpers.

This module provides functions to create consistent read-only SQLite snapshots
and extract data from SQLite and plist database files.
"""

from pathlib import Path
import plistlib
import sqlite3
from typing import Any, Dict, List, Union

from loguru import logger


SQLiteContent = Dict[str, List[List[Any]]]
SnapshotMetadata = Dict[str, Any]


def _quote_sqlite_identifier(identifier: str) -> str:
    """Return a safely quoted SQLite identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def _sqlite_read_only_uri(sqlite_file: Union[str, Path]) -> str:
    """Build a SQLite URI that opens a database without modifying evidence."""
    return Path(sqlite_file).resolve().as_uri() + "?mode=ro"


def create_sqlite_snapshot(
    source_file: Union[str, Path], destination_file: Union[str, Path]
) -> SnapshotMetadata:
    """Create a transactionally consistent SQLite snapshot with Backup API.

    Opening the source database normally through SQLite allows committed pages in
    ``-wal`` to be read.  The Backup API then writes a standalone database to the
    destination, avoiding the data loss caused by copying only the main database
    file.

    Args:
        source_file: Source SQLite database path.
        destination_file: Destination path for the standalone snapshot.

    Returns:
        Metadata describing the snapshot method and discovered sidecar files.

    Raises:
        FileNotFoundError: If the source database does not exist.
        sqlite3.Error: If SQLite cannot open or back up the source database.
    """
    source_path = Path(source_file).resolve()
    destination_path = Path(destination_file).resolve()

    if not source_path.is_file():
        raise FileNotFoundError(f"SQLite source does not exist: {source_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)

    sidecar_paths = {
        "wal": Path(f"{source_path}-wal"),
        "shm": Path(f"{source_path}-shm"),
        "journal": Path(f"{source_path}-journal"),
    }
    sidecars = {
        name: {
            "present": path.exists(),
            "path": str(path),
            "size": path.stat().st_size if path.exists() else 0,
        }
        for name, path in sidecar_paths.items()
    }

    source_connection = None
    destination_connection = None
    try:
        source_connection = sqlite3.connect(
            _sqlite_read_only_uri(source_path), uri=True, timeout=30.0
        )
        source_connection.execute("PRAGMA query_only = ON")

        journal_mode_row = source_connection.execute("PRAGMA journal_mode").fetchone()
        journal_mode = journal_mode_row[0] if journal_mode_row else "unknown"

        destination_connection = sqlite3.connect(str(destination_path))
        source_connection.backup(destination_connection)
        destination_connection.commit()

        page_count_row = destination_connection.execute("PRAGMA page_count").fetchone()
        page_count = page_count_row[0] if page_count_row else 0

        metadata: SnapshotMetadata = {
            "method": "sqlite_backup_api",
            "journal_mode": journal_mode,
            "sidecars": sidecars,
            "page_count": page_count,
        }
        logger.debug(
            "Created SQLite snapshot {} -> {} using Backup API; sidecars={}",
            source_path,
            destination_path,
            {name: data["present"] for name, data in sidecars.items()},
        )
        return metadata
    finally:
        if destination_connection is not None:
            destination_connection.close()
        if source_connection is not None:
            source_connection.close()


def extract_sqlite_to_dict(
    db_name: str, sqlite_file: str, strict: bool = False
) -> SQLiteContent:
    """Extract SQLite database content without altering BLOB bytes.

    SQLite ``TEXT`` values are returned by Python as ``str`` and SQLite ``BLOB``
    values as ``bytes``.  Keeping that distinction here is essential: decoding a
    BLOB with ``errors='ignore'`` silently deletes invalid bytes and makes exact
    reconstruction impossible.

    Args:
        db_name: Name of the database for logging purposes.
        sqlite_file: Path to the SQLite file.
        strict: If True, re-raise errors instead of continuing.

    Returns:
        Dictionary with table names as keys and table rows as values.  The first
        row for each table contains column names.
    """
    db_content: SQLiteContent = {}
    connection = None

    try:
        connection = sqlite3.connect(sqlite_file)
        cur = connection.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables_name = [row[0] for row in cur.fetchall()]

        for table_name in tables_name:
            try:
                quoted_table_name = _quote_sqlite_identifier(table_name)
                cur.execute(f"PRAGMA table_info({quoted_table_name})")
                columns_name = [row[1] for row in cur.fetchall()]

                cur.execute(f"SELECT * FROM {quoted_table_name}")
                rows = [list(row) for row in cur.fetchall()]

                rows.insert(0, columns_name)
                db_content[table_name] = rows

            except sqlite3.OperationalError as exc:
                if "no such module" in str(exc):
                    logger.warning(
                        f"Skipping table {table_name} from {db_name} due to missing module"
                    )
                    continue

                logger.warning(
                    f"Select table {table_name} from {db_name} error with: {exc}"
                )
                if strict:
                    logger.error("Strict mode enabled, exiting due to database error")
                    exit(-1)

    except Exception as exc:
        logger.error(f"Database connection error for {db_name}: {exc}")
        if strict:
            exit(-1)
    finally:
        if connection is not None:
            connection.close()

    return db_content


def extract_plist_to_dict(plist_file: str) -> Dict[str, Any]:
    """Extract plist file content to dictionary format."""
    with open(plist_file, "rb") as file_object:
        return plistlib.load(file_object)
