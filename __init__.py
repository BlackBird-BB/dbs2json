#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DBS2JSON - Database to JSON Converter for Forensic Analysis

This package converts SQLite and plist database files to JSON format for digital forensic analysis.
It processes directories of database files, extracts their contents, and outputs structured JSON data.

Author: Forensic Tools
License: MIT
Version: 1.0.0
"""

__version__ = "1.0.0"
__author__ = "Forensic Tools"
__license__ = "MIT"

from .core.processor import DatabaseProcessor
from .exporters.json_exporter import JSONExporter
from .exporters.csv_exporter import CSVExporter

__all__ = [
    "DatabaseProcessor",
    "JSONExporter",
    "CSVExporter"
]