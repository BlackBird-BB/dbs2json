#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CSV export module with collision-safe output planning."""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from loguru import logger

from utils.helpers import (
    OutputPathCollisionError,
    flatten_dict_for_csv,
    make_unique_output_filenames,
    sanitize_path_for_filename,
)


_MANIFEST_NAME = "_dbs2json_manifest.json"


class CSVExporter:
    """Export SQLite tables and plist objects to CSV files."""

    def __init__(self, output_path: Path):
        self.output_path = output_path

    @staticmethod
    def _csv_string(value: Any) -> str:
        """Convert a value to a stable CSV cell representation."""
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        else:
            text = str(value)
        return text.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")

    @staticmethod
    def _write_manifest(csv_dir: Path, manifest: Dict[str, Any]) -> None:
        with open(csv_dir / _MANIFEST_NAME, "w", encoding="utf-8") as file_object:
            json.dump(manifest, file_object, ensure_ascii=False, indent=2)

    def export_sqlite_tables_to_csv(
        self,
        sqlite_data: Dict[str, List[List[Any]]],
        csv_file_path: Path,
        table_output_names: Optional[Mapping[str, str]] = None,
    ) -> bool:
        """Write each SQLite table to a separate, uniquely named CSV file."""
        try:
            csv_dir = csv_file_path.parent
            csv_dir.mkdir(parents=True, exist_ok=True)

            if table_output_names is None:
                preferred_names = {
                    table_name: (
                        f"{sanitize_path_for_filename(csv_file_path.stem)}_"
                        f"{sanitize_path_for_filename(table_name)}.csv"
                    )
                    for table_name in sqlite_data
                }
                table_output_names = make_unique_output_filenames(preferred_names)

            for table_name, table_data in sqlite_data.items():
                if not table_data:
                    logger.warning(f"No data found for table: {table_name}")
                    continue

                headers = table_data[0] if isinstance(table_data[0], list) else []
                rows = table_data[1:] if len(table_data) > 1 else []
                table_csv_path = csv_dir / table_output_names[table_name]

                with open(
                    table_csv_path, "w", newline="", encoding="utf-8"
                ) as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                    if headers:
                        writer.writerow(headers)
                    for row in rows:
                        writer.writerow([self._csv_string(item) for item in row])

                logger.success(
                    f"Created CSV for table '{table_name}': {table_csv_path}"
                )

            return True
        except Exception as exc:
            logger.error(f"Error writing SQLite data to CSV: {exc}")
            return False

    def export_plist_to_csv(
        self, plist_data: Dict[str, Any], csv_file_path: Path
    ) -> bool:
        """Flatten a plist dictionary and write it to one CSV file."""
        try:
            flattened_data = flatten_dict_for_csv(plist_data)
            if not flattened_data:
                logger.warning("No data to write to CSV")
                return False

            csv_file_path.parent.mkdir(parents=True, exist_ok=True)
            all_keys = set()
            for record in flattened_data:
                all_keys.update(record.keys())
            sorted_keys = sorted(all_keys)

            with open(
                csv_file_path, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.DictWriter(
                    csvfile, fieldnames=sorted_keys, quoting=csv.QUOTE_ALL
                )
                writer.writeheader()
                for record in flattened_data:
                    writer.writerow(
                        {key: record.get(key, "") for key in sorted_keys}
                    )

            logger.success(f"Created CSV: {csv_file_path}")
            return True
        except Exception as exc:
            logger.error(f"Error writing plist data to CSV: {exc}")
            return False

    @staticmethod
    def _sqlite_logical_id(db_name: str, table_name: str) -> str:
        return json.dumps(
            ["sqlite_table", db_name, table_name],
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _plist_logical_id(db_name: str) -> str:
        return json.dumps(
            ["plist", db_name], ensure_ascii=False, separators=(",", ":")
        )

    def _build_output_plan(
        self, key_files: Dict[str, Any]
    ) -> tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        """Build a globally unique filename plan for plist files and tables."""
        candidates: Dict[str, str] = {}
        logical_sources: Dict[str, Dict[str, str]] = {}

        for db_name, db_con in key_files.items():
            if "content" not in db_con or not db_con["content"]:
                continue

            db_type = db_con.get("info", {}).get("type")
            base_filename = sanitize_path_for_filename(db_name)

            if db_type == "sqlite":
                for table_name, table_data in db_con["content"].items():
                    if not table_data:
                        continue
                    logical_id = self._sqlite_logical_id(db_name, table_name)
                    candidates[logical_id] = (
                        f"{base_filename}_"
                        f"{sanitize_path_for_filename(table_name)}.csv"
                    )
                    logical_sources[logical_id] = {
                        "database": db_name,
                        "type": "sqlite_table",
                        "table": table_name,
                    }
            elif db_type == "plist":
                logical_id = self._plist_logical_id(db_name)
                candidates[logical_id] = f"{base_filename}.csv"
                logical_sources[logical_id] = {
                    "database": db_name,
                    "type": "plist",
                }

        output_names = make_unique_output_filenames(
            candidates, reserved_names=[_MANIFEST_NAME]
        )
        return output_names, logical_sources

    def export_results_to_csv(
        self, key_files: Dict[str, Any], input_path: Path
    ) -> bool:
        """Export all content after validating that every target is unique."""
        try:
            output_names, logical_sources = self._build_output_plan(key_files)
            csv_dir = self.output_path / (
                f"{sanitize_path_for_filename(input_path)}_csv"
            )
            csv_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created CSV output directory: {csv_dir}")

            for db_name, db_con in key_files.items():
                if "content" not in db_con or not db_con["content"]:
                    logger.warning(f"No content found for {db_name}")
                    continue

                db_type = db_con.get("info", {}).get("type")
                if db_type == "sqlite":
                    table_names = {
                        table_name: output_names[
                            self._sqlite_logical_id(db_name, table_name)
                        ]
                        for table_name, table_data in db_con["content"].items()
                        if table_data
                    }
                    if not self.export_sqlite_tables_to_csv(
                        db_con["content"],
                        csv_dir / sanitize_path_for_filename(db_name),
                        table_output_names=table_names,
                    ):
                        return False
                elif db_type == "plist":
                    output_name = output_names[self._plist_logical_id(db_name)]
                    if not self.export_plist_to_csv(
                        db_con["content"], csv_dir / output_name
                    ):
                        return False
                else:
                    logger.warning(
                        f"Unsupported file type for CSV export: {db_type}"
                    )

            manifest = {
                "version": 1,
                "format": "csv",
                "input": str(input_path),
                "outputs": {
                    logical_id: {
                        **logical_sources[logical_id],
                        "path": output_name,
                    }
                    for logical_id, output_name in output_names.items()
                },
            }
            self._write_manifest(csv_dir, manifest)
            logger.success(f"CSV export completed. Files saved to: {csv_dir}")
            return True

        except OutputPathCollisionError as exc:
            logger.error(f"Output filename collision: {exc}")
            return False
        except Exception as exc:
            logger.error(f"Error during CSV export: {exc}")
            return False
