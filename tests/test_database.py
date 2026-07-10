#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for core.database module.
"""

import pytest
import sqlite3
import plistlib
from pathlib import Path
from unittest.mock import patch, MagicMock

from core.database import extract_sqlite_to_dict, extract_plist_to_dict


class TestExtractSqliteToDict:
    """Test cases for extract_sqlite_to_dict function."""

    def test_extract_valid_sqlite_db(self, sample_sqlite_db):
        """Test extracting data from a valid SQLite database."""
        result = extract_sqlite_to_dict("test.db", str(sample_sqlite_db))

        # Check that both tables are extracted
        assert "users" in result
        assert "products" in result

        # Check users table structure
        users_data = result["users"]
        assert len(users_data) == 3  # Header + 2 rows
        assert users_data[0] == ["id", "name", "email", "age"]  # Column names
        assert len(users_data[1]) == 4  # First data row
        assert len(users_data[2]) == 4  # Second data row

        # Check specific data values
        assert users_data[1][1] == "Alice"
        assert users_data[1][2] == "alice@example.com"
        assert users_data[1][3] == 30

        # Check products table structure
        products_data = result["products"]
        assert len(products_data) == 3  # Header + 2 rows
        assert products_data[0] == ["id", "name", "price", "description"]

    def test_extract_empty_sqlite_db(self, temp_dir):
        """Test extracting from an empty SQLite database."""
        db_path = temp_dir / "empty.db"

        conn = sqlite3.connect(str(db_path))
        conn.close()

        result = extract_sqlite_to_dict("empty.db", str(db_path))
        assert result == {}

    def test_extract_sqlite_with_binary_data(self, temp_dir):
        """Test extracting SQLite database containing binary data."""
        db_path = temp_dir / "binary_test.db"

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE binary_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                data BLOB
            )
        """)

        binary_data = b"some binary data \x00\x01\x02\xff"
        cursor.execute("INSERT INTO binary_test (name, data) VALUES (?, ?)",
                       ("test", binary_data))

        conn.commit()
        conn.close()

        result = extract_sqlite_to_dict("binary_test.db", str(db_path))

        assert "binary_test" in result
        data = result["binary_test"]
        assert len(data) == 2  # Header + 1 row
        assert data[1][2] == binary_data

    def test_extract_sqlite_with_corrupted_table(self, temp_dir):
        """Test handling of corrupted or inaccessible tables."""
        db_path = temp_dir / "corrupt_test.db"

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Create a valid table
        cursor.execute("""
            CREATE TABLE valid_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        cursor.execute("INSERT INTO valid_table (name) VALUES (?)", ("test",))

        conn.commit()
        conn.close()

        result = extract_sqlite_to_dict("corrupt_test.db", str(db_path))
        assert "valid_table" in result

    def test_extract_nonexistent_sqlite_file(self):
        """Test handling of nonexistent SQLite file."""
        with pytest.raises(Exception):
            extract_sqlite_to_dict("nonexistent.db", "/nonexistent/path.db")

    def test_extract_sqlite_strict_mode_error(self, temp_dir):
        """Test strict mode with database error."""
        db_path = temp_dir / "strict_test.db"

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Create a table that will cause an error
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)

        conn.commit()
        # Corrupt the database by closing improperly
        conn.close()

        with patch('sys.exit') as mock_exit:
            extract_sqlite_to_dict("strict_test.db", str(db_path), strict=True)
            mock_exit.assert_called_with(-1)

    def test_extract_sqlite_non_strict_mode_continue(self, temp_dir):
        """Test non-strict mode continues on database errors."""
        db_path = temp_dir / "continue_test.db"

        # Create a valid database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE valid_table (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        cursor.execute("INSERT INTO valid_table (name) VALUES (?)", ("valid",))

        conn.commit()
        conn.close()

        # Should not raise exception in non-strict mode
        result = extract_sqlite_to_dict("continue_test.db", str(db_path), strict=False)
        assert "valid_table" in result


class TestExtractPlistToDict:
    """Test cases for extract_plist_to_dict function."""

    def test_extract_valid_plist(self, sample_plist_file):
        """Test extracting data from a valid plist file."""
        result = extract_plist_to_dict(str(sample_plist_file))

        assert isinstance(result, dict)
        assert "name" in result
        assert "version" in result
        assert "settings" in result
        assert "users" in result

        # Check specific values
        assert result["name"] == "Test Application"
        assert result["version"] == "1.0.0"
        assert result["settings"]["theme"] == "dark"
        assert len(result["users"]) == 2

    def test_extract_binary_plist(self, temp_dir):
        """Test extracting from a binary plist file."""
        plist_path = temp_dir / "binary_test.plist"

        sample_data = {
            "test_key": "test_value",
            "number": 42,
            "nested": {
                "inner_key": "inner_value"
            }
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(sample_data, f, fmt=plistlib.FMT_BINARY)

        result = extract_plist_to_dict(str(plist_path))

        assert result == sample_data

    def test_extract_empty_plist(self, temp_dir):
        """Test extracting from an empty plist file."""
        plist_path = temp_dir / "empty.plist"

        with open(plist_path, 'wb') as f:
            plistlib.dump({}, f)

        result = extract_plist_to_dict(str(plist_path))
        assert result == {}

    def test_extract_plist_with_complex_data_types(self, temp_dir):
        """Test extracting plist with various data types."""
        plist_path = temp_dir / "complex_test.plist"

        complex_data = {
            "string": "test_string",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "list": [1, 2, "three", {"four": 4}],
            "dict": {
                "nested_string": "nested",
                "nested_number": 99
            },
            "data": b"binary_data"
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(complex_data, f)

        result = extract_plist_to_dict(str(plist_path))

        assert result["string"] == "test_string"
        assert result["integer"] == 42
        assert result["float"] == 3.14
        assert result["boolean"] is True
        assert result["null"] is None
        assert len(result["list"]) == 4
        assert result["dict"]["nested_string"] == "nested"

    def test_extract_nonexistent_plist_file(self):
        """Test handling of nonexistent plist file."""
        with pytest.raises(FileNotFoundError):
            extract_plist_to_dict("/nonexistent/path.plist")

    def test_extract_corrupted_plist_file(self, temp_dir):
        """Test handling of corrupted plist file."""
        plist_path = temp_dir / "corrupted.plist"

        # Write invalid plist data
        with open(plist_path, 'wb') as f:
            f.write(b"invalid plist data")

        with pytest.raises(Exception):
            extract_plist_to_dict(str(plist_path))

    def test_extract_plist_with_unicode_content(self, temp_dir):
        """Test extracting plist with Unicode content."""
        plist_path = temp_dir / "unicode_test.plist"

        unicode_data = {
            "chinese": "测试中文",
            "emoji": "🚀🎯",
            "arabic": "اختبار العربية",
            "mixed": "Hello 世界 🌍"
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(unicode_data, f)

        result = extract_plist_to_dict(str(plist_path))

        assert result["chinese"] == "测试中文"
        assert result["emoji"] == "🚀🎯"
        assert result["arabic"] == "اختبار العربية"
        assert result["mixed"] == "Hello 世界 🌍"