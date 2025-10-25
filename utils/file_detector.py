#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File detection module.

This module provides functions to detect different types of database files
by reading their magic bytes and checking associated files.
"""

from pathlib import Path
from typing import Any


def detect_sqlite_file(file_path: Path) -> bool:
    """
    Detect if file is SQLite database by reading magic bytes.

    Args:
        file_path: Path to the file to check

    Returns:
        True if file is SQLite database, False otherwise
    """
    try:
        with open(file_path, 'rb') as f:
            magic_number = f.read(6)
        return magic_number == b"SQLite"
    except (IOError, OSError):
        return False


def detect_plist_file(file_path: Path) -> bool:
    """
    Detect if file is binary plist by reading magic bytes.

    Args:
        file_path: Path to the file to check

    Returns:
        True if file is binary plist, False otherwise
    """
    try:
        with open(file_path, 'rb') as f:
            magic_number = f.read(6)
        return magic_number == b'bplist'
    except (IOError, OSError):
        return False


def detect_encrypted_sqlite_file(file_path: Path) -> bool:
    """
    Detect if SQLite database is encrypted by looking for associated files.

    Args:
        file_path: Path to the SQLite file

    Returns:
        True if database appears to be encrypted, False otherwise
    """
    shm_file = file_path.parent / (file_path.name + '-shm')
    wal_file = file_path.parent / (file_path.name + '-wal')
    return shm_file.exists() or wal_file.exists()