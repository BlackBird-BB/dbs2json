#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helper utility functions.

This module provides path sanitization, collision-safe output naming,
timestamp formatting, and data structure manipulation helpers.
"""

from collections import Counter
from datetime import datetime
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union


class OutputPathCollisionError(ValueError):
    """Raised when an output plan still contains duplicate destination names."""


def sanitize_path_for_filename(pth: Union[str, Path]) -> str:
    """Convert a path to a safe string representation for use as a filename."""
    sanitized = (
        str(pth)
        .replace(":\\", "-")
        .replace("\\", "-")
        .replace("/", "-")
        .replace(" ", "_")
        .replace(".", "_")
    )
    return sanitized or "output"


def _normalized_output_name(filename: str) -> str:
    """Normalize a filename for collision checks on case-insensitive systems."""
    return filename.casefold()


def _append_hash_suffix(filename: str, source_id: str, length: int = 12) -> str:
    """Append a stable source-derived hash before the final extension."""
    path = Path(filename)
    digest = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:length]
    return f"{path.stem}__{digest}{path.suffix}"


def make_unique_output_filenames(
    candidates: Mapping[str, str], reserved_names: Optional[List[str]] = None
) -> Dict[str, str]:
    """Resolve sanitized filename collisions before any file is written.

    Args:
        candidates: Mapping from a stable source identifier to its preferred
            output filename.
        reserved_names: Filenames that cannot be assigned to a source, such as a
            manifest filename.

    Returns:
        Mapping from each source identifier to a unique filename.  Preferred
        names remain unchanged when safe; colliding names receive a stable short
        SHA-256 suffix.

    Raises:
        OutputPathCollisionError: If unique destinations cannot be produced.
    """
    reserved = {
        _normalized_output_name(name) for name in (reserved_names or [])
    }
    normalized_counts = Counter(
        _normalized_output_name(filename) for filename in candidates.values()
    )

    result: Dict[str, str] = {}
    used = set(reserved)

    for source_id in sorted(candidates):
        preferred = candidates[source_id]
        normalized_preferred = _normalized_output_name(preferred)

        needs_hash = (
            normalized_counts[normalized_preferred] > 1
            or normalized_preferred in reserved
        )
        candidate = (
            _append_hash_suffix(preferred, source_id) if needs_hash else preferred
        )

        normalized_candidate = _normalized_output_name(candidate)
        if normalized_candidate in used:
            # Extend from 12 to the full digest before failing.  This also covers
            # the unusual case where a natural filename equals another source's
            # generated short-hash filename.
            candidate = _append_hash_suffix(preferred, source_id, length=64)
            normalized_candidate = _normalized_output_name(candidate)

        if normalized_candidate in used:
            raise OutputPathCollisionError(
                f"Unable to create a unique output filename for {source_id!r}: "
                f"{candidate!r}"
            )

        used.add(normalized_candidate)
        result[source_id] = candidate

    if len({_normalized_output_name(name) for name in result.values()}) != len(result):
        raise OutputPathCollisionError("Output filename plan contains duplicates")

    return result


def format_timestamp_to_date(timestamp: float) -> str:
    """Convert Unix timestamp to a YYYY-MM-DD string."""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return ""


def flatten_dict_for_csv(
    data: Dict[str, Any], parent_key: str = "", sep: str = "."
) -> List[Dict[str, str]]:
    """Flatten nested dictionary for CSV export."""
    items = []

    def _flatten(obj: Any, current_key: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{current_key}{sep}{key}" if current_key else str(key)
                _flatten(value, new_key)
        elif isinstance(obj, list):
            for index, item in enumerate(obj):
                new_key = (
                    f"{current_key}{sep}{index}" if current_key else str(index)
                )
                _flatten(item, new_key)
        else:
            str_value = str(obj) if obj is not None else ""
            str_value = (
                str_value.replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
            )
            items.append({current_key: str_value})

    _flatten(data, parent_key)

    result: Dict[str, str] = {}
    for item in items:
        result.update(item)

    return [result] if result else [{}]
