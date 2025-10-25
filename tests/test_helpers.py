#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for utils.helpers module.
"""

import pytest
from datetime import datetime
from pathlib import Path

from utils.helpers import sanitize_path_for_filename, format_timestamp_to_date, flatten_dict_for_csv


class TestSanitizePathForFilename:
    """Test cases for sanitize_path_for_filename function."""

    def test_sanitize_simple_path(self):
        """Test sanitizing a simple path."""
        path = Path("/home/user/data")
        result = sanitize_path_for_filename(path)
        assert result == "home_user_data"

    def test_sanitize_path_with_special_characters(self):
        """Test sanitizing path with special characters."""
        path = Path("/path/with/special-chars_123@#$%^&*()")
        result = sanitize_path_for_filename(path)
        assert "@" not in result
        assert "#" not in result
        assert "$" not in result
        assert "%" not in result
        assert "^" not in result
        assert "&" not in result
        assert "*" not in result
        assert "(" not in result
        assert ")" not in result
        assert "_" in result  # Underscores should be preserved

    def test_sanitize_path_with_spaces(self):
        """Test sanitizing path with spaces."""
        path = Path("/path/with spaces/file name")
        result = sanitize_path_for_filename(path)
        assert " " not in result
        assert "_" in result

    def test_sanitize_path_with_dots(self):
        """Test sanitizing path with dots."""
        path = Path("/config/app.config/settings.json")
        result = sanitize_path_for_filename(path)
        assert result == "config_app.config_settings.json"

    def test_sanitize_path_with_slashes(self):
        """Test sanitizing path with multiple slashes."""
        path = Path("/multiple//slashes///in//path")
        result = sanitize_path_for_filename(path)
        assert "//" not in result
        assert "_" in result

    def test_sanitize_path_with_unicode_characters(self):
        """Test sanitizing path with Unicode characters."""
        path = Path("/测试/中文/路径/🚀/test")
        result = sanitize_path_for_filename(path)
        # Unicode characters should be preserved
        assert "测试" in result
        assert "中文" in result
        assert "路径" in result
        assert "🚀" in result

    def test_sanitize_empty_path(self):
        """Test sanitizing empty path."""
        path = Path("")
        result = sanitize_path_for_filename(path)
        assert result == ""

    def test_sanitize_root_path(self):
        """Test sanitizing root path."""
        path = Path("/")
        result = sanitize_path_for_filename(path)
        assert result == ""

    def test_sanitize_relative_path(self):
        """Test sanitizing relative path."""
        path = Path("../relative/path/to/file")
        result = sanitize_path_for_filename(path)
        assert ".." not in result
        assert result == "relative_path_to_file"

    def test_sanitize_current_directory_path(self):
        """Test sanitizing current directory path."""
        path = Path("./current/dir")
        result = sanitize_path_for_filename(path)
        assert "." not in result
        assert result == "current_dir"

    def test_sanitize_path_with_windows_separators(self):
        """Test sanitizing path with Windows separators."""
        path = Path(r"C:\Users\Test\Documents\File.txt")
        result = sanitize_path_for_filename(path)
        assert ":" not in result
        assert "\\" not in result
        assert result == "C_Users_Test_Documents_File.txt"

    def test_sanitize_path_as_string(self):
        """Test sanitizing when input is string."""
        path_str = "/test/string/path"
        result = sanitize_path_for_filename(path_str)
        assert result == "test_string_path"

    def test_sanitize_path_with_trailing_slash(self):
        """Test sanitizing path with trailing slash."""
        path = Path("/path/with/trailing/slash/")
        result = sanitize_path_for_filename(path)
        assert result == "path_with_trailing_slash"

    def test_sanitize_path_with_multiple_dots(self):
        """Test sanitizing path with multiple consecutive dots."""
        path = Path("/path/with...multiple....dots")
        result = sanitize_path_for_filename(path)
        assert result == "path_with...multiple....dots"


class TestFormatTimestampToDate:
    """Test cases for format_timestamp_to_date function."""

    def test_format_valid_timestamp(self):
        """Test formatting a valid Unix timestamp."""
        timestamp = 1640995200.0  # 2022-01-01 00:00:00 UTC
        result = format_timestamp_to_date(timestamp)
        assert isinstance(result, str)
        assert len(result) == 19  # YYYY-MM-DD HH:MM:SS format
        assert result.startswith("2022-01-01")

    def test_format_current_timestamp(self):
        """Test formatting current timestamp."""
        import time
        current_timestamp = time.time()
        result = format_timestamp_to_date(current_timestamp)
        assert isinstance(result, str)
        assert len(result) == 19

    def test_format_timestamp_as_integer(self):
        """Test formatting timestamp as integer."""
        timestamp = 1640995200  # Integer timestamp
        result = format_timestamp_to_date(timestamp)
        assert isinstance(result, str)
        assert len(result) == 19

    def test_format_zero_timestamp(self):
        """Test formatting zero timestamp (Unix epoch)."""
        timestamp = 0.0
        result = format_timestamp_to_date(timestamp)
        assert result == "1970-01-01 00:00:00"

    def test_format_negative_timestamp(self):
        """Test formatting negative timestamp (before Unix epoch)."""
        timestamp = -86400.0  # One day before Unix epoch
        result = format_timestamp_to_date(timestamp)
        assert result == "1969-12-31 00:00:00"

    def test_format_very_large_timestamp(self):
        """Test formatting very large timestamp."""
        # Year 2038 problem test
        timestamp = 2147483647.0
        result = format_timestamp_to_date(timestamp)
        assert isinstance(result, str)
        assert len(result) == 19

    def test_format_timestamp_with_microseconds(self):
        """Test formatting timestamp with microseconds."""
        timestamp = 1640995200.123456
        result = format_timestamp_to_date(timestamp)
        assert isinstance(result, str)
        assert len(result) == 19  # Microseconds should be rounded

    def test_format_invalid_timestamp_string(self):
        """Test handling invalid timestamp (string)."""
        with pytest.raises((TypeError, OSError)):
            format_timestamp_to_date("invalid_timestamp")

    def test_format_none_timestamp(self):
        """Test handling None timestamp."""
        with pytest.raises((TypeError, OSError)):
            format_timestamp_to_date(None)

    def test_format_negative_zero_timestamp(self):
        """Test formatting negative zero timestamp."""
        timestamp = -0.0
        result = format_timestamp_to_date(timestamp)
        assert result == "1970-01-01 00:00:00"

    def test_format_timestamp_timezone_consistency(self):
        """Test that timestamp formatting is consistent."""
        timestamp = 1640995200.0
        result1 = format_timestamp_to_date(timestamp)
        result2 = format_timestamp_to_date(timestamp)
        assert result1 == result2

    def test_format_timestamp_leap_year(self):
        """Test formatting timestamp from leap year."""
        # February 29, 2020
        timestamp = 1582934400.0
        result = format_timestamp_to_date(timestamp)
        assert result == "2020-02-29 00:00:00"

    def test_format_timestamp_dst_transition(self):
        """Test formatting timestamp during DST transition."""
        # This would be during DST change in many timezones
        timestamp = 1583692800.0  # March 8, 2020
        result = format_timestamp_to_date(timestamp)
        assert isinstance(result, str)
        assert len(result) == 19


class TestFlattenDictForCsv:
    """Test cases for flatten_dict_for_csv function."""

    def test_flatten_simple_dict(self):
        """Test flattening a simple dictionary."""
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": 42
        }
        result = flatten_dict_for_csv(data)
        assert len(result) == 1  # One record
        assert result[0] == data

    def test_flatten_nested_dict(self, complex_nested_dict):
        """Test flattening a nested dictionary."""
        result = flatten_dict_for_csv(complex_nested_dict)

        # Should create one flattened record
        assert len(result) == 1

        # Check that nested keys are flattened
        flattened_record = result[0]
        assert "level1.level2.string_field" in flattened_record
        assert "level1.level2.number_field" in flattened_record
        assert "level1.level2.boolean_field" in flattened_record
        assert "level1.simple_field" in flattened_record
        assert "root_field" in flattened_record

        # Check values
        assert flattened_record["level1.level2.string_field"] == "test_string"
        assert flattened_record["level1.level2.number_field"] == "42"  # Should be string
        assert flattened_record["level1.level2.boolean_field"] == "True"  # Should be string

    def test_flatten_dict_with_lists(self, complex_nested_dict):
        """Test flattening dictionary containing lists."""
        result = flatten_dict_for_csv(complex_nested_dict)

        flattened_record = result[0]
        # Lists should be joined with semicolons
        assert flattened_record["level1.level2.list_field"] == "item1;item2;item3"

    def test_flatten_dict_with_none_values(self, complex_nested_dict):
        """Test flattening dictionary with None values."""
        result = flatten_dict_for_csv(complex_nested_dict)

        flattened_record = result[0]
        # None values should be converted to empty strings
        assert flattened_record["null_field"] == ""

    def test_flatten_empty_dict(self):
        """Test flattening empty dictionary."""
        data = {}
        result = flatten_dict_for_csv(data)
        assert result == []

    def test_flatten_dict_with_empty_nested_dict(self, complex_nested_dict):
        """Test flattening dictionary with empty nested dictionaries."""
        result = flatten_dict_for_csv(complex_nested_dict)

        # Should not include empty nested dicts in result
        flattened_record = result[0]
        assert "empty_dict." not in str(flattened_record.keys())

    def test_flatten_deeply_nested_dict(self):
        """Test flattening deeply nested dictionary."""
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "deep_value": "found_it"
                        }
                    }
                }
            }
        }

        result = flatten_dict_for_csv(data)
        assert len(result) == 1
        assert "level1.level2.level3.level4.deep_value" in result[0]
        assert result[0]["level1.level2.level3.level4.deep_value"] == "found_it"

    def test_flatten_dict_with_special_characters(self):
        """Test flattening dictionary with special characters."""
        data = {
            "key with spaces": {
                "key-with-dashes": {
                    "key_with_underscores": "value"
                }
            }
        }

        result = flatten_dict_for_csv(data)
        flattened_record = result[0]
        assert "key with spaces.key-with-dashes.key_with_underscores" in flattened_record

    def test_flatten_dict_with_unicode_values(self):
        """Test flattening dictionary with Unicode values."""
        data = {
            "unicode_key": {
                "chinese": "测试中文",
                "emoji": "🚀🎯",
                "arabic": "اختبار العربية"
            }
        }

        result = flatten_dict_for_csv(data)
        flattened_record = result[0]
        assert flattened_record["unicode_key.chinese"] == "测试中文"
        assert flattened_record["unicode_key.emoji"] == "🚀🎯"
        assert flattened_record["unicode_key.arabic"] == "اختبار العربية"

    def test_flatten_dict_with_numeric_types(self):
        """Test flattening dictionary with various numeric types."""
        data = {
            "integer": 42,
            "float": 3.14159,
            "scientific": 1.23e-4,
            "negative": -999,
            "zero": 0
        }

        result = flatten_dict_for_csv(data)
        flattened_record = result[0]

        # All numeric values should be converted to strings
        assert flattened_record["integer"] == "42"
        assert flattened_record["float"] == "3.14159"
        assert flattened_record["scientific"] == "0.000123"
        assert flattened_record["negative"] == "-999"
        assert flattened_record["zero"] == "0"

    def test_flatten_dict_with_boolean_types(self):
        """Test flattening dictionary with boolean values."""
        data = {
            "true_value": True,
            "false_value": False
        }

        result = flatten_dict_for_csv(data)
        flattened_record = result[0]

        assert flattened_record["true_value"] == "True"
        assert flattened_record["false_value"] == "False"

    def test_flatten_dict_with_complex_nested_types(self):
        """Test flattening dictionary with complex nested structures."""
        data = {
            "mixed": {
                "string": "text",
                "number": 42,
                "boolean": True,
                "null": None,
                "list": [1, "two", {"nested": "item"}],
                "empty_dict": {}
            }
        }

        result = flatten_dict_for_csv(data)
        flattened_record = result[0]

        assert flattened_record["mixed.string"] == "text"
        assert flattened_record["mixed.number"] == "42"
        assert flattened_record["mixed.boolean"] == "True"
        assert flattened_record["mixed.null"] == ""
        assert "1;two;{'nested': 'item'}" in flattened_record["mixed.list"]

    def test_flatten_dict_custom_separator(self):
        """Test flattening dictionary with custom separator."""
        data = {
            "level1": {
                "level2": "value"
            }
        }

        result = flatten_dict_for_csv(data, sep="__")
        assert "level1__level2" in result[0]

    def test_flatten_dict_with_parent_key(self):
        """Test flattening dictionary with parent key."""
        data = {
            "nested": {
                "key": "value"
            }
        }

        result = flatten_dict_for_csv(data, parent_key="root")
        assert "root.nested.key" in result[0]

    def test_flatten_dict_edge_cases(self):
        """Test edge cases for dictionary flattening."""
        # Dictionary with only None values
        data = {"key1": None, "key2": None}
        result = flatten_dict_for_csv(data)
        assert result == []  # Should return empty list if all values are None

        # Dictionary with keys that become empty after flattening
        data = {"empty": {}}
        result = flatten_dict_for_csv(data)
        assert result == []  # Should return empty list for empty dict

    def test_flatten_dict_preserves_order_in_python36_plus(self):
        """Test that flattening preserves key order in Python 3.6+."""
        data = {
            "z_key": "z_value",
            "a_key": "a_value",
            "m_key": "m_value"
        }

        result = flatten_dict_for_csv(data)
        # In Python 3.6+, dict order is preserved
        keys = list(result[0].keys())
        assert keys == ["z_key", "a_key", "m_key"]