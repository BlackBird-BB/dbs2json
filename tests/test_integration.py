#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration tests for the main application workflow.
"""

import json
import csv
import pytest
import sqlite3
import plistlib
from pathlib import Path
from unittest.mock import patch, MagicMock

from core.processor import DatabaseProcessor
from exporters.json_exporter import JSONExporter
from exporters.csv_exporter import CSVExporter


class TestIntegration:
    """Integration tests for the complete workflow."""

    def test_complete_workflow_json_export(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test complete workflow from discovery to JSON export."""
        # Create test directory with sample files
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy sample files to test directory
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")
        shutil.copy(sample_plist_file, test_dir / "sample.plist")

        # Stage 1: Discover files
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)

        key_files = processor.get_key_files()
        assert len(key_files) == 2
        assert "sample.db" in key_files
        assert "sample.plist" in key_files

        # Stage 2: Process files
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False, strict=False)

        processed_files = processor.get_key_files()
        assert "content" in processed_files["sample.db"]
        assert "content" in processed_files["sample.plist"]

        # Stage 3: Export to JSON
        exporter = JSONExporter(output_dir)
        result = exporter.export_results(processed_files, test_dir)

        assert result is True
        json_file = output_dir / "test_data.json"
        assert json_file.exists()

        # Verify JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            exported_data = json.load(f)
            assert "sample.db" in exported_data
            assert "sample.plist" in exported_data
            assert "info" in exported_data["sample.db"]
            assert "content" in exported_data["sample.db"]

    def test_complete_workflow_csv_export(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test complete workflow from discovery to CSV export."""
        # Create test directory with sample files
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy sample files to test directory
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")
        shutil.copy(sample_plist_file, test_dir / "sample.plist")

        # Stage 1: Discover files
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)

        # Stage 2: Process files
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False, strict=False)

        # Stage 3: Export to CSV
        exporter = CSVExporter(output_dir)
        result = exporter.export_results_to_csv(processor.get_key_files(), test_dir)

        assert result is True
        csv_dir = output_dir / "test_data_csv"
        assert csv_dir.exists()

        # Verify CSV files were created
        csv_files = list(csv_dir.glob("*.csv"))
        assert len(csv_files) >= 3  # users, products tables + plist file

    def test_workflow_with_nested_directories(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test workflow with nested directory structure."""
        # Create nested directory structure
        test_dir = temp_dir / "test_data"
        nested_dir = test_dir / "nested"
        deeply_nested = nested_dir / "deep"
        deeply_nested.mkdir(parents=True)

        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy files to different nested locations
        import shutil
        shutil.copy(sample_sqlite_db, deeply_nested / "nested_sample.db")
        shutil.copy(sample_plist_file, nested_dir / "nested_sample.plist")

        # Process nested structure
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=True)

        key_files = processor.get_key_files()
        assert len(key_files) == 2
        assert "nested/nested_sample.plist" in key_files
        assert "nested/deep/nested_sample.db" in key_files

        # Process and export
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        exporter = JSONExporter(output_dir)
        result = exporter.export_results(processor.get_key_files(), test_dir)
        assert result is True

    def test_workflow_with_encrypted_files(self, temp_dir, sample_sqlite_db, encrypted_sqlite_file):
        """Test workflow handling encrypted SQLite files."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy files to test directory
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "normal.db")
        shutil.copy(encrypted_sqlite_file, test_dir / "encrypted.db")

        # Discover files
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)

        # Should find normal file but list encrypted file separately
        key_files = processor.get_key_files()
        encrypted_files = processor.get_encrypted_files()

        assert len(key_files) == 1
        assert "normal.db" in key_files
        assert len(encrypted_files) == 1
        assert "encrypted.db" in encrypted_files

        # Process only the normal file
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        processed_files = processor.get_key_files()
        assert "content" in processed_files["normal.db"]

    def test_workflow_with_large_dataset(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test workflow with large dataset (triggers multiple file export)."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Create many files to trigger large dataset handling
        import shutil
        for i in range(25):
            shutil.copy(sample_sqlite_db, test_dir / f"sample_{i}.db")
            shutil.copy(sample_plist_file, test_dir / f"sample_{i}.plist")

        # Discover and process
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        # Export (should create directory with multiple files)
        exporter = JSONExporter(output_dir)
        result = exporter.export_results(processor.get_key_files(), test_dir)

        assert result is True

        # Check that directory with multiple files was created
        export_dir = output_dir / "test_data"
        assert export_dir.exists()
        json_files = list(export_dir.glob("*.json"))
        assert len(json_files) == 50  # 25 SQLite + 25 plist files

    def test_workflow_with_unicode_content(self, temp_dir):
        """Test workflow with Unicode content in files."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Create SQLite database with Unicode content
        unicode_db = test_dir / "unicode.db"
        conn = sqlite3.connect(str(unicode_db))
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE unicode_test (
                id INTEGER PRIMARY KEY,
                text_chinese TEXT,
                text_arabic TEXT,
                text_emoji TEXT
            )
        """)

        unicode_data = [
            (1, "测试中文", "اختبار العربية", "🚀🎯"),
            (2, "你好世界", "مرحبا", "🌍💫")
        ]

        cursor.executemany("INSERT INTO unicode_test VALUES (?, ?, ?, ?)", unicode_data)
        conn.commit()
        conn.close()

        # Create plist with Unicode content
        unicode_plist = test_dir / "unicode.plist"
        unicode_plist_data = {
            "chinese": "测试中文",
            "arabic": "اختبار العربية",
            "emoji": "🚀🎯",
            "mixed": "Hello 世界 🌍"
        }

        with open(unicode_plist, 'wb') as f:
            plistlib.dump(unicode_plist_data, f)

        # Process workflow
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        # Export to JSON
        exporter = JSONExporter(output_dir)
        result = exporter.export_results(processor.get_key_files(), test_dir)
        assert result is True

        # Verify Unicode content is preserved
        json_file = output_dir / "test_data.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            exported_data = json.load(f)

        unicode_content = exported_data["unicode.db"]["content"]["unicode_test"]
        assert "测试中文" in unicode_content[1]  # Chinese text should be preserved

        plist_content = exported_data["unicode.plist"]["content"]
        assert plist_content["chinese"] == "测试中文"
        assert plist_content["emoji"] == "🚀🎯"

    def test_workflow_with_binary_data(self, temp_dir):
        """Test workflow with binary data that gets processed."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Create plist with binary data and nested plist
        nested_plist_data = {"nested": "value", "number": 42}
        nested_plist_bytes = plistlib.dumps(nested_plist_data, fmt=plistlib.FMT_BINARY)

        main_plist_data = {
            "text_field": "normal text",
            "binary_plist_field": nested_plist_bytes,
            "binary_blob": b"some random binary data \x00\x01\x02\xff",
            "normal_dict": {"key": "value"}
        }

        main_plist = test_dir / "main.plist"
        with open(main_plist, 'wb') as f:
            plistlib.dump(main_plist_data, f)

        # Process workflow
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        processed_files = processor.get_key_files()
        content = processed_files["main.plist"]["content"]

        # Binary plist should be decoded
        assert isinstance(content["binary_plist_field"], dict)
        assert content["binary_plist_field"]["nested"] == "value"

        # Binary blob should be saved to file
        assert isinstance(content["binary_blob"], str)
        assert Path(content["binary_blob"]).exists()

        # Normal data should be unchanged
        assert content["text_field"] == "normal text"
        assert content["normal_dict"]["key"] == "value"

    def test_workflow_error_handling_strict_mode(self, temp_dir, sample_sqlite_db):
        """Test workflow error handling in strict mode."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy sample file
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")

        # Mock extract function to raise an error
        with patch('core.database.extract_sqlite_to_dict', side_effect=Exception("Database error")):
            with patch('sys.exit') as mock_exit:
                processor = DatabaseProcessor()
                processor.discover_database_files(test_dir, test_dir, verbose=False)
                processor.set_paths(test_dir, output_dir)
                processor.process_database_files(sorted_flag="mtime", verbose=False, strict=True)

                mock_exit.assert_called_with(-1)

    def test_workflow_error_handling_non_strict_mode(self, temp_dir, sample_sqlite_db):
        """Test workflow error handling in non-strict mode."""
        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy sample file
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")

        # Mock extract function to raise an error
        with patch('core.database.extract_sqlite_to_dict', side_effect=Exception("Database error")):
            processor = DatabaseProcessor()
            processor.discover_database_files(test_dir, test_dir, verbose=False)
            processor.set_paths(test_dir, output_dir)
            processor.process_database_files(sorted_flag="mtime", verbose=False, strict=False)

            # Should continue processing
            processed_files = processor.get_key_files()
            assert "sample.db" in processed_files
            assert processed_files["sample.db"]["content"] == {}  # Empty due to error

    def test_workflow_with_different_sorting_options(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test workflow with different sorting options."""
        # Create test directory with files having different timestamps
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy files with different timestamps
        import shutil
        import time
        import os

        file1 = test_dir / "file1.db"
        file2 = test_dir / "file2.db"
        file3 = test_dir / "file3.plist"

        shutil.copy(sample_sqlite_db, file1)
        shutil.copy(sample_sqlite_db, file2)
        shutil.copy(sample_plist_file, file3)

        # Set different modification times
        current_time = time.time()
        os.utime(file1, (current_time, current_time))
        os.utime(file2, (current_time + 100, current_time + 100))
        os.utime(file3, (current_time + 50, current_time + 50))

        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)
        processor.set_paths(test_dir, output_dir)

        # Test different sorting options
        for sort_option in ["mtime", "ctime", "atime", "size"]:
            processor.key_files = {}  # Reset processed files
            processor.discover_database_files(test_dir, test_dir, verbose=False)
            processor.process_database_files(sorted_flag=sort_option, verbose=False)

            processed_files = processor.get_key_files()
            assert len(processed_files) == 3

    def test_workflow_empty_directory(self, temp_dir):
        """Test workflow with empty directory."""
        # Create empty test directory
        test_dir = temp_dir / "empty_test"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=False)

        # Should find no files
        key_files = processor.get_key_files()
        assert len(key_files) == 0

        # Export should handle empty dataset gracefully
        exporter = JSONExporter(output_dir)
        result = exporter.export_results(key_files, test_dir)
        assert result is True

        json_file = output_dir / "empty_test.json"
        assert json_file.exists()

    def test_workflow_with_mixed_file_types(self, temp_dir, sample_sqlite_db, sample_plist_file, encrypted_sqlite_file):
        """Test workflow with mixed file types including encrypted ones."""
        # Create test directory
        test_dir = temp_dir / "mixed_test"
        test_dir.mkdir()
        output_dir = temp_dir / "output"
        output_dir.mkdir()

        # Copy all file types
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "normal.db")
        shutil.copy(sample_plist_file, test_dir / "normal.plist")
        shutil.copy(encrypted_sqlite_file, test_dir / "encrypted.db")

        # Process mixed files
        processor = DatabaseProcessor()
        processor.discover_database_files(test_dir, test_dir, verbose=True)

        key_files = processor.get_key_files()
        encrypted_files = processor.get_encrypted_files()

        # Should find normal files and list encrypted separately
        assert len(key_files) == 2
        assert len(encrypted_files) == 1
        assert "normal.db" in key_files
        assert "normal.plist" in key_files
        assert "encrypted.db" in encrypted_files

        # Process and export normal files
        processor.set_paths(test_dir, output_dir)
        processor.process_database_files(sorted_flag="mtime", verbose=False)

        exporter = JSONExporter(output_dir)
        result = exporter.export_results(processor.get_key_files(), test_dir)
        assert result is True

        # Verify only normal files were exported
        json_file = output_dir / "mixed_test.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            exported_data = json.load(f)
            assert "normal.db" in exported_data
            assert "normal.plist" in exported_data
            assert "encrypted.db" not in exported_data