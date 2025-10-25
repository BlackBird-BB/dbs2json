"""
Core functionality for database processing.
"""

from .database import extract_sqlite_to_dict, extract_plist_to_dict
from .processor import DatabaseProcessor

__all__ = [
    "extract_sqlite_to_dict",
    "extract_plist_to_dict",
    "DatabaseProcessor"
]