#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Regression tests for the three critical data-loss/output-loss issues."""

import csv
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from core.database import create_sqlite_snapshot, extract_sqlite_to_dict
from core.processor import DatabaseProcessor
from exporters.csv_exporter import CSVExporter
from exporters.json_exporter import JSONExporter
from utils.helpers import make_unique_output_filenames


RAW_BLOB = bytes.fromhex("ff00418042")


def _create_blob_database(db_path: Path) -> None:
    connection = sqlite3.connect(str(db_path))
    connection.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload BLOB)")
    connection.execute("INSERT INTO blobs(payload) VALUES (?)", (RAW_BLOB,))
    connection.commit()
    connection.close()


def _create_wal_database(db_path: Path) -> sqlite3.Connection:
    """Create a committed row that remains in WAL while the connection is open."""
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("CREATE TABLE events (value INTEGER)")
    connection.commit()

    # Put schema/base pages into the main DB, then commit the evidence row only
    # to WAL.  Keeping this connection open prevents close-time checkpointing.
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.execute("INSERT INTO events(value) VALUES (42)")
    connection.commit()
    assert Path(f"{db_path}-wal").exists()
    return connection


def _table_rows(processor: DatabaseProcessor, db_name: str) -> list:
    return processor.get_key_files()[db_name]["content"]["events"][1:]


def test_sqlite_extraction_preserves_blob_bytes_exactly(temp_dir):
    db_path = temp_dir / "blob.db"
    _create_blob_database(db_path)

    extracted = extract_sqlite_to_dict("blob.db", str(db_path))

    assert extracted["blobs"][1][1] == RAW_BLOB
    assert hashlib.sha256(extracted["blobs"][1][1]).hexdigest() == hashlib.sha256(
        RAW_BLOB
    ).hexdigest()


def test_blob_postprocessing_is_byte_for_byte_reconstructable(temp_dir):
    db_path = temp_dir / "blob.db"
    output_path = temp_dir / "output"
    output_path.mkdir()
    _create_blob_database(db_path)

    processor = DatabaseProcessor()
    processor.set_paths(db_path, output_path)
    processor.process_single_file(db_path)

    binary_reference = processor.get_key_files()["blob.db"]["content"]["blobs"][1][
        1
    ]["$binary"]
    extracted_path = Path(binary_reference["path"])

    assert extracted_path.read_bytes() == RAW_BLOB
    assert binary_reference["length"] == len(RAW_BLOB)
    assert binary_reference["sha256"] == hashlib.sha256(RAW_BLOB).hexdigest()


def test_backup_api_snapshot_includes_committed_wal_record(temp_dir):
    db_path = temp_dir / "wal.db"
    snapshot_path = temp_dir / "snapshot.db"
    writer = _create_wal_database(db_path)
    try:
        metadata = create_sqlite_snapshot(db_path, snapshot_path)
        extracted = extract_sqlite_to_dict("snapshot.db", str(snapshot_path))
    finally:
        writer.close()

    assert extracted["events"][1:] == [[42]]
    assert metadata["method"] == "sqlite_backup_api"
    assert metadata["sidecars"]["wal"]["present"] is True


@pytest.mark.parametrize("threads", [1, 2])
def test_directory_mode_preserves_wal_for_single_and_multi_threading(
    temp_dir, threads
):
    input_path = temp_dir / f"input-{threads}"
    output_path = temp_dir / f"output-{threads}"
    input_path.mkdir()
    output_path.mkdir()

    db_path = input_path / "wal.db"
    writer = _create_wal_database(db_path)

    # A second DB forces the two-thread path instead of the one-file shortcut.
    second_db = input_path / "second.db"
    connection = sqlite3.connect(str(second_db))
    connection.execute("CREATE TABLE data(value INTEGER)")
    connection.execute("INSERT INTO data VALUES (7)")
    connection.commit()
    connection.close()

    try:
        processor = DatabaseProcessor()
        processor.set_paths(input_path, output_path)
        processor.discover_database_files(input_path, input_path)
        processor.process_database_files(threads=threads)
    finally:
        writer.close()

    assert _table_rows(processor, "wal.db") == [[42]]
    snapshot = processor.get_key_files()["wal.db"]["info"]["snapshot"]
    assert snapshot["method"] == "sqlite_backup_api"
    assert snapshot["sidecars"]["wal"]["present"] is True


def test_single_file_mode_uses_same_wal_safe_snapshot(temp_dir):
    db_path = temp_dir / "wal.db"
    output_path = temp_dir / "output"
    output_path.mkdir()
    writer = _create_wal_database(db_path)

    try:
        processor = DatabaseProcessor()
        processor.set_paths(db_path, output_path)
        processor.process_single_file(db_path)
    finally:
        writer.close()

    assert _table_rows(processor, "wal.db") == [[42]]
    assert (
        processor.get_key_files()["wal.db"]["info"]["snapshot"]["method"]
        == "sqlite_backup_api"
    )


def test_unique_filename_plan_resolves_sanitized_collision_stably():
    candidates = {
        "a/b.db": "a-b_db.json",
        "a-b.db": "a-b_db.json",
    }

    first = make_unique_output_filenames(candidates)
    second = make_unique_output_filenames(dict(reversed(list(candidates.items()))))

    assert first == second
    assert len(set(name.casefold() for name in first.values())) == 2
    assert all("__" in name for name in first.values())


def test_json_export_writes_all_21_colliding_inputs_and_manifest(temp_dir):
    output_path = temp_dir / "output"
    output_path.mkdir()
    key_files = {
        f"normal-{index}.db": {
            "info": {"type": "sqlite", "path": f"/normal-{index}.db"},
            "content": {"table": [["value"], [index]]},
        }
        for index in range(19)
    }
    key_files["a/b.db"] = {
        "info": {"type": "sqlite", "path": "/a/b.db"},
        "content": {"table": [["value"], [20]]},
    }
    key_files["a-b.db"] = {
        "info": {"type": "sqlite", "path": "/a-b.db"},
        "content": {"table": [["value"], [21]]},
    }

    exporter = JSONExporter(output_path)
    assert exporter.export_results(key_files, Path("input")) is True

    export_directory = output_path / "input"
    manifest = json.loads(
        (export_directory / "_dbs2json_manifest.json").read_text(encoding="utf-8")
    )
    output_names = [entry["path"] for entry in manifest["outputs"].values()]

    assert len(output_names) == 21
    assert len(set(name.casefold() for name in output_names)) == 21
    assert all((export_directory / name).is_file() for name in output_names)
    assert manifest["outputs"]["a/b.db"]["path"] != manifest["outputs"]["a-b.db"][
        "path"
    ]


def test_csv_export_resolves_database_table_and_plist_filename_collisions(temp_dir):
    output_path = temp_dir / "output"
    output_path.mkdir()
    key_files = {
        "a/b.db": {
            "info": {"type": "sqlite", "path": "/a/b.db"},
            "content": {
                "a/b": [["value"], [1]],
                "a-b": [["value"], [2]],
            },
        },
        "a-b.db": {
            "info": {"type": "sqlite", "path": "/a-b.db"},
            "content": {"a/b": [["value"], [3]]},
        },
        "p/list.plist": {
            "info": {"type": "plist", "path": "/p/list.plist"},
            "content": {"value": 4},
        },
        "p-list.plist": {
            "info": {"type": "plist", "path": "/p-list.plist"},
            "content": {"value": 5},
        },
    }

    exporter = CSVExporter(output_path)
    assert exporter.export_results_to_csv(key_files, Path("input")) is True

    export_directory = output_path / "input_csv"
    manifest = json.loads(
        (export_directory / "_dbs2json_manifest.json").read_text(encoding="utf-8")
    )
    output_names = [entry["path"] for entry in manifest["outputs"].values()]

    assert len(output_names) == 5
    assert len(set(name.casefold() for name in output_names)) == 5
    assert all((export_directory / name).is_file() for name in output_names)

    exported_values = []
    for output_name in output_names:
        with open(export_directory / output_name, newline="", encoding="utf-8") as f:
            rows = list(csv.reader(f))
            exported_values.extend(cell for row in rows[1:] for cell in row)
    assert {"1", "2", "3", "4", "5"}.issubset(set(exported_values))
