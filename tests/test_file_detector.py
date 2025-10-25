#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for utils.file_detector module.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, mock_open

from utils.file_detector import detect_sqlite_file, detect_plist_file, detect_encrypted_sqlite_file


class TestDetectSqliteFile:
    """Test cases for detect_sqlite_file function."""

    def test_detect_valid_sqlite_file(self, sample_sqlite_db):
        """Test detecting a valid SQLite file."""
        result = detect_sqlite_file(sample_sqlite_db)
        assert result is True

    def test_detect_sqlite_file_by_magic_bytes(self, temp_dir):
        """Test detecting SQLite file by magic bytes."""
        sqlite_path = temp_dir / "test.sqlite"

        # Write SQLite magic bytes
        with open(sqlite_path, 'wb') as f:
            f.write(b"SQLite format 3\x00")

        result = detect_sqlite_file(sqlite_path)
        assert result is True

    def test_detect_sqlite_file_wrong_magic_bytes(self, temp_dir):
        """Test detecting file with wrong magic bytes."""
        fake_path = temp_dir / "fake.sqlite"

        # Write wrong magic bytes
        with open(fake_path, 'wb') as f:
            f.write(b"Not a SQLite file")

        result = detect_sqlite_file(fake_path)
        assert result is False

    def test_detect_sqlite_file_empty(self, temp_dir):
        """Test detecting empty file."""
        empty_path = temp_dir / "empty.sqlite"
        empty_path.touch()

        result = detect_sqlite_file(empty_path)
        assert result is False

    def test_detect_sqlite_file_insufficient_bytes(self, temp_dir):
        """Test detecting file with insufficient bytes."""
        short_path = temp_dir / "short.sqlite"

        # Write fewer bytes than needed for magic bytes
        with open(short_path, 'wb') as f:
            f.write(b"SQLite")

        result = detect_sqlite_file(short_path)
        assert result is False

    def test_detect_sqlite_file_nonexistent(self):
        """Test detecting nonexistent file."""
        nonexistent_path = Path("/nonexistent/path/file.db")
        result = detect_sqlite_file(nonexistent_path)
        assert result is False

    def test_detect_sqlite_file_permission_error(self, temp_dir):
        """Test handling permission errors."""
        test_path = temp_dir / "test.db"

        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = detect_sqlite_file(test_path)
            assert result is False

    def test_detect_sqlite_file_with_corrupted_data(self, temp_dir):
        """Test detecting file with corrupted SQLite data."""
        corrupted_path = temp_dir / "corrupted.db"

        # Write file that starts with SQLite magic but has invalid content
        with open(corrupted_path, 'wb') as f:
            f.write(b"SQLite format 3\x00")
            f.write(b"\x00" * 1000)  # Add null bytes

        # Should still detect as SQLite based on magic bytes
        result = detect_sqlite_file(corrupted_path)
        assert result is True

    def test_detect_sqlite_file_with_various_extensions(self, temp_dir):
        """Test detecting SQLite files with different extensions."""
        extensions = [".db", ".sqlite", ".sqlite3", ".sdb", ".sl3"]

        for ext in extensions:
            test_path = temp_dir / f"test{ext}"
            with open(test_path, 'wb') as f:
                f.write(b"SQLite format 3\x00")

            result = detect_sqlite_file(test_path)
            assert result is True, f"Failed to detect SQLite file with extension {ext}"


class TestDetectPlistFile:
    """Test cases for detect_plist_file function."""

    def test_detect_binary_plist(self, sample_plist_file):
        """Test detecting a binary plist file."""
        result = detect_plist_file(sample_plist_file)
        assert result is True

    def test_detect_binary_plist_by_magic_bytes(self, temp_dir):
        """Test detecting binary plist by magic bytes."""
        plist_path = temp_dir / "test.plist"

        # Write binary plist magic bytes
        with open(plist_path, 'wb') as f:
            f.write(b"bplist00")

        result = detect_plist_file(plist_path)
        assert result is True

    def test_detect_xml_plist(self, temp_dir):
        """Test detecting an XML plist file."""
        xml_plist_path = temp_dir / "test.xml"

        # Write XML plist content
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>TestKey</key>
    <string>TestValue</string>
</dict>
</plist>"""

        with open(xml_plist_path, 'wb') as f:
            f.write(xml_content)

        result = detect_plist_file(xml_plist_path)
        assert result is True

    def test_detect_plist_file_wrong_magic_bytes(self, temp_dir):
        """Test detecting file with wrong magic bytes."""
        fake_path = temp_dir / "fake.plist"

        with open(fake_path, 'wb') as f:
            f.write(b"Not a plist file")

        result = detect_plist_file(fake_path)
        assert result is False

    def test_detect_plist_file_empty(self, temp_dir):
        """Test detecting empty file."""
        empty_path = temp_dir / "empty.plist"
        empty_path.touch()

        result = detect_plist_file(empty_path)
        assert result is False

    def test_detect_plist_file_insufficient_bytes(self, temp_dir):
        """Test detecting file with insufficient bytes."""
        short_path = temp_dir / "short.plist"

        with open(short_path, 'wb') as f:
            f.write(b"bpl")

        result = detect_plist_file(short_path)
        assert result is False

    def test_detect_plist_file_nonexistent(self):
        """Test detecting nonexistent file."""
        nonexistent_path = Path("/nonexistent/path/file.plist")
        result = detect_plist_file(nonexistent_path)
        assert result is False

    def test_detect_plist_file_permission_error(self, temp_dir):
        """Test handling permission errors."""
        test_path = temp_dir / "test.plist"

        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = detect_plist_file(test_path)
            assert result is False

    def test_detect_plist_file_with_various_extensions(self, temp_dir):
        """Test detecting plist files with different extensions."""
        extensions = [".plist", ".PLIST", ".Plist"]

        # Test binary plist
        for ext in extensions:
            test_path = temp_dir / f"test{ext}"
            with open(test_path, 'wb') as f:
                f.write(b"bplist00")

            result = detect_plist_file(test_path)
            assert result is True, f"Failed to detect binary plist with extension {ext}"

        # Test XML plist
        xml_content = b'<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"></plist>'
        for ext in extensions:
            test_path = temp_dir / f"xml_test{ext}"
            with open(test_path, 'wb') as f:
                f.write(xml_content)

            result = detect_plist_file(test_path)
            assert result is True, f"Failed to detect XML plist with extension {ext}"

    def test_detect_xml_plist_with_whitespace(self, temp_dir):
        """Test detecting XML plist with leading whitespace."""
        xml_plist_path = temp_dir / "whitespace.plist"

        # XML content with leading whitespace
        xml_content = b"""  \n\t  <?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN">
<plist version="1.0">
<dict/>
</plist>"""

        with open(xml_plist_path, 'wb') as f:
            f.write(xml_content)

        result = detect_plist_file(xml_plist_path)
        assert result is True

    def test_detect_malformed_xml_plist(self, temp_dir):
        """Test detecting malformed XML plist."""
        malformed_path = temp_dir / "malformed.plist"

        # Malformed XML (missing closing tags)
        malformed_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
    <key>TestKey</key>
    <string>TestValue"""

        with open(malformed_path, 'wb') as f:
            f.write(malformed_content)

        # Should still detect as plist based on XML declaration
        result = detect_plist_file(malformed_path)
        assert result is True


class TestDetectEncryptedSqliteFile:
    """Test cases for detect_encrypted_sqlite_file function."""

    def test_detect_encrypted_sqlite_file(self, encrypted_sqlite_file):
        """Test detecting an encrypted SQLite file."""
        result = detect_encrypted_sqlite_file(encrypted_sqlite_file)
        assert result is True

    def test_detect_encrypted_sqlite_by_pattern(self, temp_dir):
        """Test detecting encrypted SQLite by file pattern."""
        encrypted_path = temp_dir / "encrypted.db"

        # Write data that matches encrypted SQLite pattern
        # SQLite header followed by non-ASCII characters
        with open(encrypted_path, 'wb') as f:
            f.write(b"SQLite format 3\x00")
            f.write(b"\xff\xfe\xfd\xfc\xfb\xfa")  # Non-ASCII pattern

        result = detect_encrypted_sqlite_file(encrypted_path)
        assert result is True

    def test_detect_normal_sqlite_as_not_encrypted(self, sample_sqlite_db):
        """Test that normal SQLite files are not detected as encrypted."""
        result = detect_encrypted_sqlite_file(sample_sqlite_db)
        assert result is False

    def test_detect_encrypted_sqlite_wrong_magic_bytes(self, temp_dir):
        """Test detecting file with wrong magic bytes as not encrypted SQLite."""
        fake_path = temp_dir / "fake_encrypted.db"

        with open(fake_path, 'wb') as f:
            f.write(b"Not a SQLite file\x00\xff\xfe\xfd")

        result = detect_encrypted_sqlite_file(fake_path)
        assert result is False

    def test_detect_encrypted_sqlite_empty_file(self, temp_dir):
        """Test detecting empty file as not encrypted SQLite."""
        empty_path = temp_dir / "empty.db"
        empty_path.touch()

        result = detect_encrypted_sqlite_file(empty_path)
        assert result is False

    def test_detect_encrypted_sqlite_insufficient_bytes(self, temp_dir):
        """Test detecting file with insufficient bytes."""
        short_path = temp_dir / "short.db"

        with open(short_path, 'wb') as f:
            f.write(b"SQLite")

        result = detect_encrypted_sqlite_file(short_path)
        assert result is False

    def test_detect_encrypted_sqlite_nonexistent(self):
        """Test detecting nonexistent file."""
        nonexistent_path = Path("/nonexistent/path/encrypted.db")
        result = detect_encrypted_sqlite_file(nonexistent_path)
        assert result is False

    def test_detect_encrypted_sqlite_permission_error(self, temp_dir):
        """Test handling permission errors."""
        test_path = temp_dir / "test.db"

        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = detect_encrypted_sqlite_file(test_path)
            assert result is False

    def test_detect_encrypted_sqlite_with_various_extensions(self, temp_dir):
        """Test detecting encrypted SQLite files with different extensions."""
        extensions = [".db", ".sqlite", ".sqlite3", ".encrypted"]

        for ext in extensions:
            test_path = temp_dir / f"test{ext}"
            with open(test_path, 'wb') as f:
                f.write(b"SQLite format 3\x00")
                f.write(b"\xff\xfe\xfd")  # Encrypted pattern

            result = detect_encrypted_sqlite_file(test_path)
            assert result is True, f"Failed to detect encrypted SQLite with extension {ext}"

    def test_detect_sqlite_with_binary_data_as_not_encrypted(self, temp_dir):
        """Test that SQLite with binary data is not considered encrypted."""
        binary_path = temp_dir / "binary.db"

        # SQLite header with ASCII-compatible binary data
        with open(binary_path, 'wb') as f:
            f.write(b"SQLite format 3\x00")
            f.write(b"Some readable binary data123")  # ASCII compatible

        result = detect_encrypted_sqlite_file(binary_path)
        assert result is False

    def test_detect_sqlite_with_null_bytes_as_encrypted(self, temp_dir):
        """Test detecting SQLite with null bytes as potentially encrypted."""
        null_path = temp_dir / "null.db"

        # SQLite header followed by null bytes (common in encrypted databases)
        with open(null_path, 'wb') as f:
            f.write(b"SQLite format 3\x00")
            f.write(b"\x00" * 100)  # Many null bytes

        result = detect_encrypted_sqlite_file(null_path)
        assert result is True

    def test_edge_case_detection(self, temp_dir):
        """Test edge cases for encrypted SQLite detection."""
        test_cases = [
            # (data, should_be_encrypted, description)
            (b"SQLite format 3\x00Hello World", False, "SQLite with ASCII text"),
            (b"SQLite format 3\x00\xff", True, "SQLite with single non-ASCII byte"),
            (b"SQLite format 3\x00\x80\x81\x82", True, "SQLite with high-bit set bytes"),
            (b"SQLite format 3\x00Hello\xffWorld", True, "SQLite with mixed content"),
        ]

        for data, expected, description in test_cases:
            test_path = temp_dir / f"edge_case_{hash(data)}.db"
            with open(test_path, 'wb') as f:
                f.write(data)

            result = detect_encrypted_sqlite_file(test_path)
            assert result == expected, f"Failed for case: {description}"