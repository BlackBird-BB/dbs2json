#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for core.processor module.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

from core.processor import DatabaseProcessor


class TestDatabaseProcessor:
    """Test cases for DatabaseProcessor class."""

    def test_init(self):
        """Test DatabaseProcessor initialization."""
        processor = DatabaseProcessor()

        assert processor.key_files == {}
        assert processor.encrypt_files == []
        assert processor.path_n is None
        assert processor.inp is None
        assert processor.opt is None

    def test_set_paths(self, temp_dir):
        """Test setting input and output paths."""
        processor = DatabaseProcessor()
        input_path = temp_dir / "input"
        output_path = temp_dir / "output"

        input_path.mkdir()
        output_path.mkdir()

        processor.set_paths(input_path, output_path)

        assert processor.inp == input_path
        assert processor.opt == output_path
        assert processor.path_n == output_path

    def test_get_key_files(self, temp_dir):
        """Test getting key files."""
        processor = DatabaseProcessor()

        # Initially empty
        assert processor.get_key_files() == {}

        # Add some test data
        processor.key_files = {"test.db": {"info": {"type": "sqlite"}}}
        assert processor.get_key_files() == {"test.db": {"info": {"type": "sqlite"}}}

    def test_get_encrypted_files(self, temp_dir):
        """Test getting encrypted files list."""
        processor = DatabaseProcessor()

        # Initially empty
        assert processor.get_encrypted_files() == []

        # Add some test data
        processor.encrypt_files = ["encrypted1.db", "encrypted2.db"]
        assert processor.get_encrypted_files() == ["encrypted1.db", "encrypted2.db"]

    def test_discover_database_files_with_sqlite_and_plist(self, temp_dir, sample_sqlite_db, sample_plist_file):
        """Test discovering SQLite and plist files."""
        processor = DatabaseProcessor()

        # Create test directory structure
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        # Copy sample files to test directory
        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")
        shutil.copy(sample_plist_file, test_dir / "sample.plist")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        key_files = processor.get_key_files()
        assert len(key_files) == 2
        assert "sample.db" in key_files
        assert "sample.plist" in key_files

        # Check SQLite file info
        sqlite_info = key_files["sample.db"]["info"]
        assert sqlite_info["type"] == "sqlite"
        assert "size" in sqlite_info
        assert "mtime" in sqlite_info
        assert "atime" in sqlite_info
        assert "ctime" in sqlite_info

        # Check plist file info
        plist_info = key_files["sample.plist"]["info"]
        assert plist_info["type"] == "plist"
        assert "size" in plist_info

    def test_discover_database_files_with_encrypted(self, temp_dir, encrypted_sqlite_file):
        """Test discovering encrypted SQLite files."""
        processor = DatabaseProcessor()

        # Create test directory structure
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        # Copy encrypted file to test directory
        import shutil
        shutil.copy(encrypted_sqlite_file, test_dir / "encrypted.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        key_files = processor.get_key_files()
        encrypted_files = processor.get_encrypted_files()

        # Should detect as encrypted, not in key_files
        assert len(key_files) == 0
        assert len(encrypted_files) == 1
        assert "encrypted.db" in encrypted_files

    def test_discover_database_files_recursive(self, temp_dir, sample_sqlite_db):
        """Test recursive directory scanning."""
        processor = DatabaseProcessor()

        # Create nested directory structure
        test_dir = temp_dir / "test_data"
        nested_dir = test_dir / "nested"
        deeply_nested = nested_dir / "deep"

        nested_dir.mkdir()
        deeply_nested.mkdir()

        # Copy sample file to nested directory
        import shutil
        shutil.copy(sample_sqlite_db, deeply_nested / "nested_sample.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        key_files = processor.get_key_files()
        assert len(key_files) == 1
        assert "nested/nested_sample.db" in key_files

    def test_discover_database_files_verbose(self, temp_dir, sample_sqlite_db):
        """Test verbose output during file discovery."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "sample.db")

        with patch('loguru.logger.success') as mock_success:
            processor.discover_database_files(test_dir, test_dir, verbose=True)
            mock_success.assert_called()

    def test_discover_database_files_permission_error(self, temp_dir):
        """Test handling of permission errors during discovery."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        # Mock Path.iterdir to raise PermissionError
        with patch.object(Path, 'iterdir', side_effect=PermissionError("Permission denied")):
            with patch('loguru.logger.error') as mock_error:
                processor.discover_database_files(test_dir, test_dir, verbose=False)
                mock_error.assert_called()

    def test_process_binary_data_in_dict_with_plist(self, temp_dir):
        """Test processing binary plist data in dictionary."""
        processor = DatabaseProcessor()
        processor.path_n = temp_dir

        # Create test plist data
        test_plist = {"key": "value", "number": 42}
        plist_bytes = __import__('plistlib').dumps(test_plist, fmt=__import__('plistlib').FMT_BINARY)

        test_dict = {
            "string_field": "test_string",
            "plist_field": plist_bytes,
            "nested": {
                "inner_plist": plist_bytes
            }
        }

        processor.process_binary_data_in_dict(test_dict)

        # Check that plist data was decoded
        assert test_dict["plist_field"] == test_plist
        assert test_dict["nested"]["inner_plist"] == test_plist

    def test_process_binary_data_in_dict_with_binary_blob(self, temp_dir):
        """Test processing binary blob data in dictionary."""
        processor = DatabaseProcessor()
        processor.path_n = temp_dir

        test_dict = {
            "string_field": "test_string",
            "binary_field": b"some binary data that's not a plist"
        }

        processor.process_binary_data_in_dict(test_dict)

        # Check that binary data was saved exactly with reconstructable metadata
        assert "binary_field" in test_dict
        binary_ref = test_dict["binary_field"]["$binary"]
        assert Path(binary_ref["path"]).exists()
        assert Path(binary_ref["path"]).read_bytes() == b"some binary data that's not a plist"
        assert binary_ref["length"] == len(b"some binary data that's not a plist")

    def test_process_binary_data_in_dict_with_text_bytes(self, temp_dir):
        """Test processing UTF-8 text bytes in dictionary."""
        processor = DatabaseProcessor()
        processor.path_n = temp_dir

        test_dict = {
            "text_bytes": b"hello world",
            "normal_string": "normal value"
        }

        processor.process_binary_data_in_dict(test_dict)

        # SQLite/plist bytes remain binary even when they happen to be valid UTF-8
        binary_ref = test_dict["text_bytes"]["$binary"]
        assert Path(binary_ref["path"]).read_bytes() == b"hello world"
        assert binary_ref["length"] == len(b"hello world")
        assert test_dict["normal_string"] == "normal value"

    def test_process_database_files_sorting(self, temp_dir, sample_sqlite_db):
        """Test database file processing with different sorting criteria."""
        processor = DatabaseProcessor()

        # Create test directory with multiple files
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        # Create multiple SQLite files with different timestamps
        import shutil
        import time
        from pathlib import Path

        file1 = test_dir / "file1.db"
        file2 = test_dir / "file2.db"

        shutil.copy(sample_sqlite_db, file1)
        shutil.copy(sample_sqlite_db, file2)

        # Modify file times to ensure different ordering
        current_time = time.time()
        os.utime(file1, (current_time, current_time))
        os.utime(file2, (current_time + 100, current_time + 100))

        # Set up processor with discovered files
        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict') as mock_extract:
            mock_extract.return_value = {"users": []}

            # Test mtime sorting
            processor.process_database_files(sorted_flag="mtime", verbose=False)
            processed_files = list(processor.key_files.keys())
            # Should be sorted by mtime (largest first)
            assert processed_files[0] == "file2.db"  # More recent mtime

    def test_process_database_files_strict_mode_error(self, temp_dir, sample_sqlite_db):
        """Test database processing in strict mode with errors."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', side_effect=Exception("Database error")):
            with patch('sys.exit') as mock_exit:
                processor.process_database_files(sorted_flag="mtime", verbose=False, strict=True)
                mock_exit.assert_called_with(-1)

    def test_process_database_files_non_strict_mode_continue(self, temp_dir, sample_sqlite_db):
        """Test database processing in non-strict mode continues on errors."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', side_effect=Exception("Database error")):
            with patch('loguru.logger.error') as mock_error:
                processor.process_database_files(sorted_flag="mtime", verbose=False, strict=False)
                mock_error.assert_called()
                # Should not exit, continue processing

    def test_process_database_files_verbose(self, temp_dir, sample_sqlite_db):
        """Test verbose output during database processing."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', return_value={"users": []}):
            with patch('loguru.logger.info') as mock_info:
                processor.process_database_files(sorted_flag="mtime", verbose=True)
                mock_info.assert_called()

    def test_process_database_files_with_unknown_type(self, temp_dir):
        """Test processing files with unknown file type."""
        processor = DatabaseProcessor()

        # Manually add a file with unknown type
        processor.key_files = {
            "unknown_file.xyz": {
                "info": {"type": "unknown", "path": str(temp_dir / "unknown_file.xyz")}
            }
        }

        with patch('loguru.logger.warning') as mock_warning:
            processor.process_database_files(sorted_flag="mtime", verbose=False)
            mock_warning.assert_called()

    def test_sorting_functions(self, temp_dir, sample_sqlite_db):
        """Test all sorting criteria functions."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', return_value={"users": []}):
            # Test each sorting option
            for sort_flag in ["mtime", "ctime", "atime", "size"]:
                processor.process_database_files(sorted_flag=sort_flag, verbose=False)

            # Test unknown sorting flag
            with patch('loguru.logger.warning') as mock_warning:
                processor.process_database_files(sorted_flag="unknown", verbose=False)
                mock_warning.assert_called()

    def test_cleanup_temporary_file(self, temp_dir, sample_sqlite_db):
        """Test that temporary files are cleaned up after processing."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', return_value={"users": []}):
            with patch('os.unlink') as mock_unlink:
                processor.process_database_files(sorted_flag="mtime", verbose=False)
                mock_unlink.assert_called_once()

    def test_process_database_files_error_handling(self, temp_dir, sample_sqlite_db):
        """Test error handling during database processing."""
        processor = DatabaseProcessor()

        # Create test directory
        test_dir = temp_dir / "test_data"
        test_dir.mkdir()

        import shutil
        shutil.copy(sample_sqlite_db, test_dir / "test.db")

        processor.discover_database_files(test_dir, test_dir, verbose=False)

        with patch('core.database.extract_sqlite_to_dict', side_effect=Exception("Test error")):
            with patch('loguru.logger.error') as mock_error:
                processor.process_database_files(sorted_flag="mtime", verbose=False, strict=False)

                # Should have logged the error
                assert mock_error.call_count >= 2  # One for the error, one for the file info

                # Content should be empty dict due to error handling
                assert processor.key_files["test.db"]["content"] == {}