#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for exporters.csv_exporter module.
"""

import csv
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

from exporters.csv_exporter import CSVExporter


class TestCSVExporter:
    """Test cases for CSVExporter class."""

    def test_init(self, temp_dir):
        """Test CSVExporter initialization."""
        exporter = CSVExporter(temp_dir)
        assert exporter.output_path == temp_dir

    def test_export_sqlite_tables_to_csv(self, temp_dir):
        """Test exporting SQLite tables to CSV files."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
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

        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        # Check that CSV files were created for each table
        users_csv = temp_dir / "output_users.csv"
        products_csv = temp_dir / "output_products.csv"

        assert users_csv.exists()
        assert products_csv.exists()

        # Verify content of users CSV
        with open(users_csv, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3  # Header + 2 data rows
            assert rows[0] == ["id", "name", "email", "age"]
            assert rows[1] == ["1", "Alice", "alice@example.com", "30"]
            assert rows[2] == ["2", "Bob", "bob@example.com", "25"]

    def test_export_sqlite_tables_to_csv_empty_data(self, temp_dir):
        """Test exporting empty SQLite data."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {}
        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True  # Should succeed with empty data

    def test_export_sqlite_tables_to_csv_empty_table(self, temp_dir):
        """Test exporting SQLite data with empty table."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
            "empty_table": [],
            "normal_table": [
                ["id", "name"],
                [1, "test"]
            ]
        }

        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        # Normal table should be created
        normal_csv = temp_dir / "output_normal_table.csv"
        assert normal_csv.exists()

    def test_export_sqlite_tables_to_csv_with_special_characters(self, temp_dir):
        """Test exporting SQLite data with special characters."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
            "test_table": [
                ["id", "text", "multiline"],
                [1, "Hello, world!", "Line 1\nLine 2\rLine 3\tTabbed"],
                [2, 'Quote "test"', "Normal text"]
            ]
        }

        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        csv_file = temp_dir / "output_test_table.csv"
        assert csv_file.exists()

        # Verify that special characters are properly escaped
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            content = f.read()
            assert '"Hello, world!"' in content
            assert '"Quote ""test"""' in content
            assert "Line 1\\nLine 2\\rLine 3\\tTabbed" in content

    def test_export_sqlite_tables_to_csv_with_none_values(self, temp_dir):
        """Test exporting SQLite data with None values."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
            "test_table": [
                ["id", "name", "email", "age"],
                [1, "Alice", None, 30],
                [2, None, "bob@example.com", None]
            ]
        }

        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        csv_file = temp_dir / "output_test_table.csv"
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 3
            assert rows[1][2] == ""  # None should become empty string
            assert rows[2][1] == ""  # None should become empty string

    def test_export_sqlite_tables_to_csv_directory_creation(self, temp_dir):
        """Test that directories are created when needed."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
            "test_table": [
                ["id", "name"],
                [1, "test"]
            ]
        }

        # Use nested path that doesn't exist
        csv_file_path = temp_dir / "nested" / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        nested_dir = temp_dir / "nested"
        assert nested_dir.exists()
        assert nested_dir.is_dir()

        csv_file = nested_dir / "output_test_table.csv"
        assert csv_file.exists()

    def test_export_plist_to_csv(self, temp_dir, complex_nested_dict):
        """Test exporting plist data to CSV."""
        exporter = CSVExporter(temp_dir)

        csv_file_path = temp_dir / "output.plist.csv"
        result = exporter.export_plist_to_csv(complex_nested_dict, csv_file_path)

        assert result is True

        # Check that CSV file was created
        assert csv_file_path.exists()

        # Verify CSV content
        with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1  # Should create one row from flattened dict

            # Check that flattened keys exist as columns
            row = rows[0]
            assert "level1.level2.string_field" in row
            assert "level1.level2.number_field" in row
            assert "level1.simple_field" in row
            assert "root_field" in row

            # Check values
            assert row["level1.level2.string_field"] == "test_string"
            assert row["level1.level2.number_field"] == "42"
            assert row["level1.simple_field"] == "simple_value"
            assert row["root_field"] == "root_value"

    def test_export_plist_to_csv_empty_data(self, temp_dir):
        """Test exporting empty plist data."""
        exporter = CSVExporter(temp_dir)

        csv_file_path = temp_dir / "empty.plist.csv"
        result = exporter.export_plist_to_csv({}, csv_file_path)

        assert result is False  # Should return False for empty data

    def test_export_plist_to_csv_simple_dict(self, temp_dir):
        """Test exporting simple plist dictionary."""
        exporter = CSVExporter(temp_dir)

        simple_data = {
            "name": "Test App",
            "version": "1.0.0",
            "enabled": True
        }

        csv_file_path = temp_dir / "simple.plist.csv"
        result = exporter.export_plist_to_csv(simple_data, csv_file_path)

        assert result is True

        with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1

            row = rows[0]
            assert row["name"] == "Test App"
            assert row["version"] == "1.0.0"
            assert row["enabled"] == "True"

    def test_export_plist_to_csv_with_none_values(self, temp_dir):
        """Test exporting plist data with None values."""
        exporter = CSVExporter(temp_dir)

        data_with_none = {
            "name": "Test",
            "description": None,
            "count": 0
        }

        csv_file_path = temp_dir / "none_test.plist.csv"
        result = exporter.export_plist_to_csv(data_with_none, csv_file_path)

        assert result is True

        with open(csv_file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["name"] == "Test"
            assert row["description"] == ""  # None becomes empty string
            assert row["count"] == "0"

    def test_export_plist_to_csv_directory_creation(self, temp_dir):
        """Test directory creation for plist CSV export."""
        exporter = CSVExporter(temp_dir)

        test_data = {"key": "value"}

        # Use nested path that doesn't exist
        csv_file_path = temp_dir / "nested" / "output.plist.csv"
        result = exporter.export_plist_to_csv(test_data, csv_file_path)

        assert result is True

        nested_dir = temp_dir / "nested"
        assert nested_dir.exists()
        assert csv_file_path.exists()

    def test_export_results_to_csv_mixed_data(self, temp_dir, sample_key_files_data):
        """Test exporting mixed SQLite and plist data to CSV."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        result = exporter.export_results_to_csv(sample_key_files_data, input_path)

        assert result is True

        # Check that CSV directory was created
        csv_dir = temp_dir / "test_input_csv"
        assert csv_dir.exists()

        # Check that files were created for both SQLite and plist
        sqlite_files = list(csv_dir.glob("test_db_*.csv"))
        plist_files = list(csv_dir.glob("test.plist.csv"))

        assert len(sqlite_files) == 2  # users and products tables
        assert len(plist_files) == 1   # plist file

        # Verify SQLite CSV files
        users_csv = csv_dir / "test_db_users.csv"
        products_csv = csv_dir / "test_db_products.csv"

        assert users_csv.exists()
        assert products_csv.exists()

        # Verify plist CSV file
        plist_csv = csv_dir / "test.plist.csv"
        assert plist_csv.exists()

    def test_export_results_to_csv_empty_dataset(self, temp_dir):
        """Test exporting empty dataset to CSV."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        result = exporter.export_results_to_csv({}, input_path)

        assert result is True

        # Check that CSV directory was created
        csv_dir = temp_dir / "test_input_csv"
        assert csv_dir.exists()

    def test_export_results_to_csv_no_content(self, temp_dir):
        """Test exporting dataset with no content."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        # Dataset with info but no content
        dataset_no_content = {
            "test.db": {
                "info": {"type": "sqlite", "path": "/test.db"},
                # No "content" key
            }
        }

        result = exporter.export_results_to_csv(dataset_no_content, input_path)

        assert result is True

        # CSV directory should still be created
        csv_dir = temp_dir / "test_input_csv"
        assert csv_dir.exists()

    def test_export_results_to_csv_unsupported_file_type(self, temp_dir):
        """Test handling unsupported file types."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        # Dataset with unsupported file type
        dataset_unsupported = {
            "test.xyz": {
                "info": {"type": "unsupported", "path": "/test.xyz"},
                "content": {"data": "test"}
            }
        }

        with patch('loguru.logger.warning') as mock_warning:
            result = exporter.export_results_to_csv(dataset_unsupported, input_path)

        assert result is True  # Should continue despite warning
        mock_warning.assert_called()

    def test_export_results_to_csv_individual_file_error(self, temp_dir):
        """Test handling errors when exporting individual files."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        dataset = {
            "valid.db": {
                "info": {"type": "sqlite"},
                "content": {
                    "table1": [["id", "name"], [1, "test"]]
                }
            },
            "error.plist": {
                "info": {"type": "plist"},
                "content": {"key": "value"}
            }
        }

        with patch.object(exporter, 'export_plist_to_csv', side_effect=Exception("Export error")):
            with patch('loguru.logger.error') as mock_error:
                result = exporter.export_results_to_csv(dataset, input_path)

        assert result is False  # Should return False due to error
        mock_error.assert_called()

    def test_export_results_to_csv_unicode_content(self, temp_dir):
        """Test exporting dataset with Unicode content."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        unicode_dataset = {
            "unicode.plist": {
                "info": {"type": "plist"},
                "content": {
                    "chinese": "测试中文",
                    "emoji": "🚀🎯",
                    "arabic": "اختبار العربية"
                }
            }
        }

        result = exporter.export_results_to_csv(unicode_dataset, input_path)

        assert result is True

        csv_file = temp_dir / "test_input_csv" / "unicode.plist.csv"
        assert csv_file.exists()

        # Verify Unicode content in CSV
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert row["chinese"] == "测试中文"
            assert row["emoji"] == "🚀🎯"
            assert row["arabic"] == "اختبار العربية"

    def test_export_results_to_csv_path_sanitization(self, temp_dir):
        """Test that paths are properly sanitized for filenames."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/path/with spaces and@special#chars")

        dataset = {
            "test file.db": {
                "info": {"type": "sqlite"},
                "content": {
                    "table1": [["id", "name"], [1, "test"]]
                }
            }
        }

        result = exporter.export_results_to_csv(dataset, input_path)

        assert result is True

        # Check that directory was created with sanitized name
        csv_dir = temp_dir / "path_with_spaces_and_special#chars_csv"
        assert csv_dir.exists()

        # Check that file has sanitized name
        csv_file = csv_dir / "test_file_table1.csv"
        assert csv_file.exists()

    def test_export_results_to_csv_permission_error(self, temp_dir):
        """Test handling permission errors during CSV export."""
        exporter = CSVExporter(temp_dir)
        input_path = Path("/test/input")

        dataset = {
            "test.db": {
                "info": {"type": "sqlite"},
                "content": {
                    "table1": [["id", "name"], [1, "test"]]
                }
            }
        }

        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
            with patch('loguru.logger.error') as mock_error:
                result = exporter.export_results_to_csv(dataset, input_path)

        assert result is False
        mock_error.assert_called()

    def test_export_sqlite_tables_large_data(self, temp_dir):
        """Test exporting large SQLite tables."""
        exporter = CSVExporter(temp_dir)

        # Create large table with many rows
        large_table = [["id", "name", "value"]]
        for i in range(1000):
            large_table.append([i, f"name_{i}", f"value_{i}"])

        sqlite_data = {
            "large_table": large_table
        }

        csv_file_path = temp_dir / "large_output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        csv_file = temp_dir / "large_output_large_table.csv"
        assert csv_file.exists()

        # Verify row count
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 1001  # Header + 1000 data rows

    def test_export_sqlite_tables_with_binary_data_references(self, temp_dir):
        """Test exporting SQLite data with binary file references."""
        exporter = CSVExporter(temp_dir)

        sqlite_data = {
            "test_table": [
                ["id", "data_file", "normal_field"],
                [1, str(temp_dir / "binary_file.bin"), "normal_value"]
            ]
        }

        csv_file_path = temp_dir / "output"
        result = exporter.export_sqlite_tables_to_csv(sqlite_data, csv_file_path)

        assert result is True

        csv_file = temp_dir / "output_test_table.csv"
        with open(csv_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 2
            assert str(temp_dir / "binary_file.bin") in rows[1][1]