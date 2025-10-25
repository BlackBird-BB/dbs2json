#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helper utility functions.

This module provides various utility functions for path sanitization,
timestamp formatting, and data structure manipulation.
"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Union


def sanitize_path_for_filename(pth: Union[str, Path]) -> str:
    """
    Convert a path to a safe string representation for use as filename.

    Args:
        pth: Path string or Path object to convert

    Returns:
        Safe string representation with special characters replaced
    """
    return str(pth).replace(":\\", "-").replace("\\", "-").replace("/", "-").replace(" ", '_').replace(".", "_")


def format_timestamp_to_date(timestamp: float) -> str:
    """
    Convert Unix timestamp to date string format.

    Args:
        timestamp: Unix timestamp

    Returns:
        Formatted date string (YYYY-MM-DD) or empty string on error
    """
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return ""


def flatten_dict_for_csv(data: Dict[str, Any], parent_key: str = '', sep: str = '.') -> List[Dict[str, str]]:
    """
    Flatten nested dictionary for CSV export.

    Args:
        data: Dictionary to flatten
        parent_key: Parent key for nested items
        sep: Separator for nested keys

    Returns:
        List of flattened dictionaries
    """
    items = []

    def _flatten(obj, current_key=''):
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{current_key}{sep}{k}" if current_key else k
                _flatten(v, new_key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_key = f"{current_key}{sep}{i}" if current_key else str(i)
                _flatten(item, new_key)
        else:
            # Convert to string and handle special characters
            str_value = str(obj) if obj is not None else ''
            # Replace newlines and tabs to avoid CSV formatting issues
            str_value = str_value.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            items.append({current_key: str_value})

    _flatten(data, parent_key)

    # Merge all items into a single dictionary
    result = {}
    for item in items:
        result.update(item)

    return [result] if result else [{}]