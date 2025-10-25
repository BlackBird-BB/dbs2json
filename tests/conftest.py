#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pytest configuration and fixtures for dbs2json tests.
"""

import pytest
import tempfile
import sqlite3
import plistlib
from pathlib import Path
from typing import Dict, Any, List


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_sqlite_db(temp_dir):
    """Create a sample SQLite database for testing."""
    db_path = temp_dir / "test.db"

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    # Create test tables
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            description TEXT
        )
    """)

    # Insert test data
    cursor.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                   ("Alice", "alice@example.com", 30))
    cursor.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                   ("Bob", "bob@example.com", 25))

    cursor.execute("INSERT INTO products (name, price, description) VALUES (?, ?, ?)",
                   ("Laptop", 999.99, "High-performance laptop"))
    cursor.execute("INSERT INTO products (name, price, description) VALUES (?, ?, ?)",
                   ("Mouse", 29.99, "Wireless mouse"))

    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def sample_plist_file(temp_dir):
    """Create a sample plist file for testing."""
    plist_path = temp_dir / "test.plist"

    sample_data = {
        "name": "Test Application",
        "version": "1.0.0",
        "settings": {
            "theme": "dark",
            "notifications": True,
            "max_connections": 10
        },
        "users": [
            {"id": 1, "name": "Alice", "role": "admin"},
            {"id": 2, "name": "Bob", "role": "user"}
        ],
        "metadata": {
            "created": "2023-01-01T00:00:00Z",
            "author": "Test Author"
        }
    }

    with open(plist_path, 'wb') as f:
        plistlib.dump(sample_data, f)

    return plist_path


@pytest.fixture
def encrypted_sqlite_file(temp_dir):
    """Create a file that looks like an encrypted SQLite database."""
    encrypted_path = temp_dir / "encrypted.db"

    # Write SQLite header followed by random data (simulates encryption)
    header = b"SQLite format 3\x00"
    encrypted_data = header + b'\x00' * 100  # Simplified encrypted data

    with open(encrypted_path, 'wb') as f:
        f.write(encrypted_data)

    return encrypted_path


@pytest.fixture
def sample_key_files_data():
    """Sample key_files data structure for testing exporters."""
    return {
        "test.db": {
            "info": {
                "path": "/path/to/test.db",
                "type": "sqlite",
                "size": 8192,
                "st_mtime": 1640995200.0,
                "mtime": "2022-01-01 00:00:00",
                "st_atime": 1640995200.0,
                "atime": "2022-01-01 00:00:00",
                "st_ctime": 1640995200.0,
                "ctime": "2022-01-01 00:00:00"
            },
            "content": {
                "users": [
                    ["id", "name", "email", "age"],
                    [1, "Alice", "alice@example.com", 30],
                    [2, "Bob", "bob@example.com", 25]
                ],
                "products": [
                    ["id", "name", "price", "description"],
                    [1, "Laptop", 999.99, "High-performance laptop"],
                    [2, "Mouse", 29.99, "Wireless mouse"]
                ]
            }
        },
        "test.plist": {
            "info": {
                "path": "/path/to/test.plist",
                "type": "plist",
                "size": 1024,
                "st_mtime": 1640995200.0,
                "mtime": "2022-01-01 00:00:00",
                "st_atime": 1640995200.0,
                "atime": "2022-01-01 00:00:00",
                "st_ctime": 1640995200.0,
                "ctime": "2022-01-01 00:00:00"
            },
            "content": {
                "name": "Test Application",
                "version": "1.0.0",
                "settings": {
                    "theme": "dark",
                    "notifications": True,
                    "max_connections": 10
                }
            }
        }
    }


@pytest.fixture
def complex_nested_dict():
    """Complex nested dictionary for testing flatten_dict_for_csv."""
    return {
        "level1": {
            "level2": {
                "string_field": "test_string",
                "number_field": 42,
                "boolean_field": True,
                "list_field": ["item1", "item2", "item3"]
            },
            "simple_field": "simple_value"
        },
        "root_field": "root_value",
        "empty_dict": {},
        "null_field": None
    }