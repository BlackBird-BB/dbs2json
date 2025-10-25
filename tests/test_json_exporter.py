#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for exporters.json_exporter module.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

from exporters.json_exporter import JSONExporter


class TestJSONExporter:
    """Test cases for JSONExporter class."""

    def test_init(self, temp_dir):
        """Test JSONExporter initialization."""
        exporter = JSONExporter(temp_dir)
        assert exporter.output_path == temp_dir

    def test_export_results_small_dataset(self, temp_dir, sample_key_files_data):
        """Test exporting small dataset (single JSON file)."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create smaller dataset (less than 20 files)
        small_dataset = {k: v for k, v in list(sample_key_files_data.items())[:1]}

        result = exporter.export_results(small_dataset, input_path)

        assert result is True

        # Check that JSON file was created
        json_file = temp_dir / "test_input.json"
        assert json_file.exists()

        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == small_dataset

    def test_export_results_large_dataset(self, temp_dir, sample_key_files_data):
        """Test exporting large dataset (multiple JSON files)."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create large dataset (more than 20 files by duplicating)
        large_dataset = {}
        for i in range(25):
            for key, value in sample_key_files_data.items():
                large_dataset[f"{key}_{i}"] = value

        result = exporter.export_results(large_dataset, input_path)

        assert result is True

        # Check that output directory was created
        output_dir = temp_dir / "test_input"
        assert output_dir.exists()
        assert output_dir.is_dir()

        # Check that multiple JSON files were created
        json_files = list(output_dir.glob("*.json"))
        assert len(json_files) == 50  # 2 original files * 25 copies

        # Verify content of one file
        with open(json_files[0], 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert "info" in loaded_data
            assert "content" in loaded_data

    def test_export_results_create_output_directory(self, temp_dir, sample_key_files_data):
        """Test that output directory is created if it doesn't exist."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create large dataset to trigger directory creation
        large_dataset = {}
        for i in range(25):
            for key, value in sample_key_files_data.items():
                large_dataset[f"{key}_{i}"] = value

        # Ensure output directory doesn't exist
        output_dir = temp_dir / "test_input"
        assert not output_dir.exists()

        result = exporter.export_results(large_dataset, input_path)

        assert result is True
        assert output_dir.exists()
        assert output_dir.is_dir()

    def test_export_results_file_exists_overwrite(self, temp_dir, sample_key_files_data):
        """Test handling existing file with overwrite."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create existing file
        json_file = temp_dir / "test_input.json"
        json_file.write_text('{"existing": "data"}')

        # Mock input to return 'y'
        with patch('builtins.input', return_value='y'):
            result = exporter.export_results(sample_key_files_data, input_path)

        assert result is True
        assert json_file.exists()

        # Verify file was overwritten
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data != {"existing": "data"}

    def test_export_results_file_exists_cancel(self, temp_dir, sample_key_files_data):
        """Test handling existing file with cancellation."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create existing file
        json_file = temp_dir / "test_input.json"
        json_file.write_text('{"existing": "data"}')

        # Mock input to return 'n'
        with patch('builtins.input', return_value='n'):
            result = exporter.export_results(sample_key_files_data, input_path)

        assert result is False

        # Verify file was not overwritten
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == {"existing": "data"}

    def test_export_results_file_exists_keyboard_interrupt(self, temp_dir, sample_key_files_data):
        """Test handling keyboard interrupt when asking about overwrite."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create existing file
        json_file = temp_dir / "test_input.json"
        json_file.write_text('{"existing": "data"}')

        # Mock input to raise KeyboardInterrupt
        with patch('builtins.input', side_effect=KeyboardInterrupt):
            result = exporter.export_results(sample_key_files_data, input_path)

        assert result is False

    def test_export_results_json_serialization_error(self, temp_dir):
        """Test handling JSON serialization errors with fallback."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create data with non-serializable object
        non_serializable_data = {
            "test.db": {
                "info": {"type": "sqlite"},
                "content": {"non_serializable": object()}  # object() is not JSON serializable
            }
        }

        result = exporter.export_results(non_serializable_data, input_path)

        assert result is True

        # Check that fallback file was created
        json_file = temp_dir / "test_input.json"
        assert json_file.exists()

        # Content should be string representation due to fallback
        content = json_file.read_text(encoding='utf-8')
        assert "object" in content  # String representation should contain "object"

    def test_export_results_json_error_fallback_failure(self, temp_dir):
        """Test handling fallback write failure."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create data with non-serializable object
        non_serializable_data = {
            "test.db": {
                "info": {"type": "sqlite"},
                "content": {"non_serializable": object()}
            }
        }

        # Mock file operations to raise exception
        with patch('builtins.open', side_effect=OSError("Permission denied")):
            result = exporter.export_results(non_serializable_data, input_path)

        assert result is False

    def test_export_results_large_dataset_individual_file_error(self, temp_dir, sample_key_files_data):
        """Test handling errors when exporting individual files in large dataset."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create large dataset
        large_dataset = {}
        for i in range(25):
            for key, value in sample_key_files_data.items():
                large_dataset[f"{key}_{i}"] = value

        # Add one problematic file
        large_dataset["problematic.db"] = {
            "info": {"type": "sqlite"},
            "content": {"non_serializable": object()}
        }

        with patch('loguru.logger.error') as mock_error:
            result = exporter.export_results(large_dataset, input_path)

        assert result is True  # Should continue despite error
        mock_error.assert_called()

        # Check that other files were still created
        output_dir = temp_dir / "test_input"
        json_files = list(output_dir.glob("*.json"))
        assert len(json_files) >= 50  # Most files should be created

    def test_export_results_empty_dataset(self, temp_dir):
        """Test exporting empty dataset."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        result = exporter.export_results({}, input_path)

        assert result is True

        # Check that empty JSON file was created
        json_file = temp_dir / "test_input.json"
        assert json_file.exists()

        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == {}

    def test_export_results_with_unicode_content(self, temp_dir):
        """Test exporting dataset with Unicode content."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        unicode_data = {
            "unicode_test.plist": {
                "info": {"type": "plist"},
                "content": {
                    "chinese": "测试中文",
                    "emoji": "🚀🎯",
                    "arabic": "اختبار العربية",
                    "mixed": "Hello 世界 🌍"
                }
            }
        }

        result = exporter.export_results(unicode_data, input_path)

        assert result is True

        json_file = temp_dir / "test_input.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == unicode_data

    def test_export_results_with_binary_data(self, temp_dir):
        """Test exporting dataset with binary data references."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        binary_data = {
            "binary_test.db": {
                "info": {"type": "sqlite"},
                "content": {
                    "binary_field": str(temp_dir / "temp_binary_file.bin"),
                    "normal_field": "normal_value"
                }
            }
        }

        result = exporter.export_results(binary_data, input_path)

        assert result is True

        json_file = temp_dir / "test_input.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == binary_data

    def test_export_results_with_complex_nested_structure(self, temp_dir):
        """Test exporting dataset with complex nested structures."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        complex_data = {
            "complex_test.db": {
                "info": {"type": "sqlite"},
                "content": {
                    "users": [
                        ["id", "name", "data"],
                        [1, "Alice", {"preferences": {"theme": "dark", "notifications": True}}],
                        [2, "Bob", {"preferences": {"theme": "light", "notifications": False}}]
                    ],
                    "metadata": {
                        "version": "1.0.0",
                        "created": "2023-01-01T00:00:00Z",
                        "tags": ["production", "user-data"]
                    }
                }
            }
        }

        result = exporter.export_results(complex_data, input_path)

        assert result is True

        json_file = temp_dir / "test_input.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)
            assert loaded_data == complex_data

    def test_export_results_file_path_sanitization(self, temp_dir):
        """Test that file paths are properly sanitized for filenames."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/path/with spaces and@special#chars")

        # Create large dataset to trigger directory creation
        large_dataset = {}
        for i in range(25):
            large_dataset[f"test file {i}.db"] = {
                "info": {"type": "sqlite"},
                "content": {"data": f"test_{i}"}
            }

        result = exporter.export_results(large_dataset, input_path)

        assert result is True

        # Check that directory was created with sanitized name
        output_dir = temp_dir / "path_with_spaces_and_special_chars"
        assert output_dir.exists()

        # Check that individual files have sanitized names
        files = list(output_dir.glob("*.json"))
        assert len(files) == 25

    def test_export_results_permission_error_on_directory_creation(self, temp_dir, sample_key_files_data):
        """Test handling permission errors when creating output directory."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        # Create large dataset to trigger directory creation
        large_dataset = {}
        for i in range(25):
            for key, value in sample_key_files_data.items():
                large_dataset[f"{key}_{i}"] = value

        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Permission denied")):
            with patch('loguru.logger.error') as mock_error:
                result = exporter.export_results(large_dataset, input_path)

        assert result is False
        mock_error.assert_called()

    def test_export_results_file_write_error(self, temp_dir, sample_key_files_data):
        """Test handling file write errors."""
        exporter = JSONExporter(temp_dir)
        input_path = Path("/test/input")

        with patch('builtins.open', side_effect=OSError("Disk full")):
            with patch('loguru.logger.error') as mock_error:
                result = exporter.export_results(sample_key_files_data, input_path)

        assert result is False
        mock_error.assert_called()