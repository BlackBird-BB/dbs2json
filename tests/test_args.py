#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for cli.args module.
"""

import pytest
from unittest.mock import patch
from pathlib import Path

from cli.args import parse_arguments


class TestParseArguments:
    """Test cases for parse_arguments function."""

    def test_parse_default_arguments(self):
        """Test parsing arguments with default values."""
        with patch('sys.argv', ['main.py']):
            args = parse_arguments()

            assert args.input == Path('.').resolve()
            assert args.output == Path('.').resolve()
            assert args.sorted == "mtime"
            assert args.format == "json"
            assert args.verbose is False
            assert args.strict is False

    def test_parse_short_arguments(self):
        """Test parsing short form arguments."""
        test_args = [
            'main.py',
            '-i', '/test/input',
            '-o', '/test/output',
            '-s', 'size',
            '-f', 'csv',
            '-v',
            '-st'
        ]

        with patch('sys.argv', test_args):
            args = parse_arguments()

            assert args.input == Path('/test/input')
            assert args.output == Path('/test/output')
            assert args.sorted == "size"
            assert args.format == "csv"
            assert args.verbose is True
            assert args.strict is True

    def test_parse_long_arguments(self):
        """Test parsing long form arguments."""
        test_args = [
            'main.py',
            '--input', '/test/input',
            '--output', '/test/output',
            '--sorted', 'ctime',
            '--format', 'json',
            '--verbose',
            '--strict'
        ]

        with patch('sys.argv', test_args):
            args = parse_arguments()

            assert args.input == Path('/test/input')
            assert args.output == Path('/test/output')
            assert args.sorted == "ctime"
            assert args.format == "json"
            assert args.verbose is True
            assert args.strict is True

    def test_parse_mixed_arguments(self):
        """Test parsing mixed short and long arguments."""
        test_args = [
            'main.py',
            '--input', '/test/input',
            '-o', '/test/output',
            '-v',
            '--sorted', 'atime'
        ]

        with patch('sys.argv', test_args):
            args = parse_arguments()

            assert args.input == Path('/test/input')
            assert args.output == Path('/test/output')
            assert args.sorted == "atime"
            assert args.verbose is True
            assert args.strict is False

    def test_parse_all_valid_sorted_options(self):
        """Test all valid sorted options."""
        valid_options = ['mtime', 'ctime', 'atime', 'size']

        for option in valid_options:
            test_args = ['main.py', '--sorted', option]
            with patch('sys.argv', test_args):
                args = parse_arguments()
                assert args.sorted == option

    def test_parse_all_valid_format_options(self):
        """Test all valid format options."""
        valid_formats = ['json', 'csv']

        for format_option in valid_formats:
            test_args = ['main.py', '--format', format_option]
            with patch('sys.argv', test_args):
                args = parse_arguments()
                assert args.format == format_option

    def test_parse_invalid_sorted_option(self):
        """Test parsing invalid sorted option."""
        test_args = ['main.py', '--sorted', 'invalid']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_invalid_format_option(self):
        """Test parsing invalid format option."""
        test_args = ['main.py', '--format', 'invalid']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_flag_arguments(self):
        """Test parsing boolean flag arguments."""
        # Test verbose flag
        with patch('sys.argv', ['main.py', '-v']):
            args = parse_arguments()
            assert args.verbose is True

        with patch('sys.argv', ['main.py', '--verbose']):
            args = parse_arguments()
            assert args.verbose is True

        # Test strict flag
        with patch('sys.argv', ['main.py', '-st']):
            args = parse_arguments()
            assert args.strict is True

        with patch('sys.argv', ['main.py', '--strict']):
            args = parse_arguments()
            assert args.strict is True

    def test_parse_version_argument(self):
        """Test parsing version argument."""
        test_args = ['main.py', '--version']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_help_argument(self):
        """Test parsing help argument."""
        test_args = ['main.py', '--help']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_short_help_argument(self):
        """Test parsing short help argument."""
        test_args = ['main.py', '-h']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_input_with_spaces(self):
        """Test parsing input path with spaces."""
        test_args = ['main.py', '--input', '/path/with spaces/file']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/path/with spaces/file')

    def test_parse_output_with_spaces(self):
        """Test parsing output path with spaces."""
        test_args = ['main.py', '--output', '/path/with spaces/output']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.output == Path('/path/with spaces/output')

    def test_parse_input_with_special_characters(self):
        """Test parsing input path with special characters."""
        test_args = ['main.py', '--input', '/path/with@special#chars']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/path/with@special#chars')

    def test_parse_relative_input_path(self):
        """Test parsing relative input path."""
        test_args = ['main.py', '--input', '../relative/path']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('../relative/path')

    def test_parse_absolute_input_path(self):
        """Test parsing absolute input path."""
        test_args = ['main.py', '--input', '/absolute/path']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/absolute/path')

    def test_parse_current_directory_as_input(self):
        """Test parsing current directory as input."""
        test_args = ['main.py', '--input', '.']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('.')

    def test_parse_parent_directory_as_input(self):
        """Test parsing parent directory as input."""
        test_args = ['main.py', '--input', '..']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('..')

    def test_parse_multiple_flags_together(self):
        """Test parsing multiple flag arguments together."""
        test_args = ['main.py', '-v', '-st', '--sorted', 'size']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.verbose is True
            assert args.strict is True
            assert args.sorted == "size"

    def test_parse_arguments_case_sensitive(self):
        """Test that arguments are case sensitive."""
        # Test format argument case sensitivity
        test_args = ['main.py', '--format', 'JSON']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

        # Test sorted argument case sensitivity
        test_args = ['main.py', '--sorted', 'MTIME']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_arguments_with_unicode_values(self):
        """Test parsing arguments with Unicode values."""
        test_args = ['main.py', '--input', '/测试/路径']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/测试/路径')

    def test_parse_empty_string_values(self):
        """Test parsing empty string values."""
        test_args = ['main.py', '--input', '']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('')

    def test_parse_arguments_with_equals_syntax(self):
        """Test parsing arguments with equals syntax (not supported by argparse)."""
        # This should fail as argparse doesn't support --key=value format for custom args
        test_args = ['main.py', '--input=/test/path']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                parse_arguments()

    def test_parse_negative_values(self):
        """Test that negative values don't cause issues."""
        test_args = ['main.py', '--input', '/path/with-dash']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/path/with-dash')

    def test_parse_arguments_in_different_order(self):
        """Test parsing arguments in different order."""
        orders = [
            ['main.py', '-i', '/input', '-o', '/output', '-v', '-st'],
            ['main.py', '-v', '-st', '-i', '/input', '-o', '/output'],
            ['main.py', '-o', '/output', '-i', '/input', '-v', '-st'],
            ['main.py', '-st', '-v', '-o', '/output', '-i', '/input']
        ]

        for test_args in orders:
            with patch('sys.argv', test_args):
                args = parse_arguments()
                assert args.input == Path('/input')
                assert args.output == Path('/output')
                assert args.verbose is True
                assert args.strict is True

    def test_parse_duplicate_arguments(self):
        """Test parsing duplicate arguments (last one wins)."""
        test_args = ['main.py', '--input', '/first/path', '--input', '/second/path']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/second/path')

    def test_parse_arguments_without_script_name(self):
        """Test parsing arguments without script name."""
        test_args = ['--input', '/test']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/test')

    def test_argument_defaults_after_parsing(self):
        """Test that arguments have proper default values after parsing."""
        with patch('sys.argv', ['main.py']):
            args = parse_arguments()

            # Check all defaults
            assert args.input == Path('.').resolve()
            assert args.output == Path('.').resolve()
            assert args.sorted == "mtime"
            assert args.format == "json"
            assert args.verbose is False
            assert args.strict is False

    def test_parse_minimal_arguments(self):
        """Test parsing with minimal arguments."""
        test_args = ['main.py']

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args is not None

    def test_parse_maximum_arguments(self):
        """Test parsing with all possible arguments."""
        test_args = [
            'main.py',
            '--input', '/test/input',
            '--output', '/test/output',
            '--sorted', 'size',
            '--format', 'csv',
            '--verbose',
            '--strict'
        ]

        with patch('sys.argv', test_args):
            args = parse_arguments()
            assert args.input == Path('/test/input')
            assert args.output == Path('/test/output')
            assert args.sorted == "size"
            assert args.format == "csv"
            assert args.verbose is True
            assert args.strict is True