#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""JSON export module with collision-safe output planning."""

import json
import os
from pathlib import Path
from typing import Any, Dict
import uuid

from loguru import logger

from utils.helpers import (
    OutputPathCollisionError,
    make_unique_output_filenames,
    sanitize_path_for_filename,
)


_MANIFEST_NAME = "_dbs2json_manifest.json"


class JSONExporter:
    """Export processed database data to JSON files."""

    def __init__(self, output_path: Path):
        self.output_path = output_path

    @staticmethod
    def _write_json_atomic(file_path: Path, value: Any) -> None:
        """Write valid JSON atomically so partial files are not left behind."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = file_path.with_name(
            f".{file_path.name}.{uuid.uuid4().hex}.tmp"
        )
        try:
            with open(temporary_path, "w", encoding="utf-8") as json_file:
                json.dump(value, json_file, ensure_ascii=False, indent=2)
                json_file.flush()
                os.fsync(json_file.fileno())
            os.replace(temporary_path, file_path)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()

    def _export_large_dataset(
        self, key_files: Dict[str, Any], input_path: Path
    ) -> bool:
        output_dir = self.output_path / sanitize_path_for_filename(input_path)

        # Build and validate the complete destination plan before opening any
        # output file.  Distinct sources that sanitize to the same filename get a
        # stable source-derived hash suffix instead of overwriting one another.
        preferred_names = {
            db_name: f"{sanitize_path_for_filename(db_name)}.json"
            for db_name in key_files
        }
        output_names = make_unique_output_filenames(
            preferred_names, reserved_names=[_MANIFEST_NAME]
        )

        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")

        for db_name, db_con in key_files.items():
            file_path = output_dir / output_names[db_name]
            try:
                self._write_json_atomic(file_path, db_con)
                logger.success(f"Created: {file_path}")
            except (TypeError, ValueError, OSError) as exc:
                logger.error(f"Failed to write JSON for {db_name}: {exc}")
                return False

        manifest = {
            "version": 1,
            "format": "json",
            "input": str(input_path),
            "outputs": {
                db_name: {
                    "path": output_names[db_name],
                    "source_path": db_con.get("info", {}).get("path"),
                    "type": db_con.get("info", {}).get("type"),
                }
                for db_name, db_con in key_files.items()
            },
        }
        self._write_json_atomic(output_dir / _MANIFEST_NAME, manifest)
        logger.success(f"Created output manifest: {output_dir / _MANIFEST_NAME}")
        return True

    def _export_small_dataset(
        self, key_files: Dict[str, Any], input_path: Path
    ) -> bool:
        file_path = self.output_path / (
            sanitize_path_for_filename(input_path) + ".json"
        )

        if file_path.exists():
            try:
                choice = (
                    input(
                        "There is already an analysis result. "
                        "Do you want to overwrite it? (Y/n): "
                    )
                    .lower()
                    .strip()
                )
                if choice in ["n", "no"]:
                    logger.info("Operation cancelled by user")
                    return False
            except (KeyboardInterrupt, EOFError):
                logger.info("Operation cancelled by user")
                return False

        try:
            self._write_json_atomic(file_path, key_files)
            logger.success(f"Created output file: {file_path}")
            return True
        except (TypeError, ValueError, OSError) as exc:
            logger.error(f"JSON serialization error: {exc}")
            logger.error("Failed to create output file")
            return False

    def export_results(self, key_files: Dict[str, Any], input_path: Path) -> bool:
        """Export results, preventing sanitized output-name collisions."""
        try:
            if len(key_files) > 20:
                return self._export_large_dataset(key_files, input_path)
            return self._export_small_dataset(key_files, input_path)
        except OutputPathCollisionError as exc:
            logger.error(f"Output filename collision: {exc}")
            return False
        except Exception as exc:
            logger.error(f"Error during JSON export: {exc}")
            return False
