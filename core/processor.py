#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Database discovery and processing logic."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import hashlib
import os
from pathlib import Path
import plistlib
from shutil import copyfile
from tempfile import mkstemp
import threading
from typing import Any, Dict, List, Tuple
import uuid

from loguru import logger

from core.database import (
    create_sqlite_snapshot,
    extract_plist_to_dict,
    extract_sqlite_to_dict,
)
from utils.file_detector import (
    detect_encrypted_sqlite_file,
    detect_plist_file,
    detect_sqlite_file,
)
from utils.helpers import format_timestamp_to_date


class DatabaseProcessor:
    """Discover database files, create safe snapshots, and extract content."""

    def __init__(self):
        self.key_files: Dict[str, Dict[str, Any]] = {}
        self.encrypt_files: List[str] = []
        self.path_n = None
        self.inp = None
        self.opt = None

        self._key_files_lock = threading.Lock()
        self._encrypt_files_lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._binary_write_lock = threading.Lock()
        self._processed_count = 0
        self._total_files = 0

    @staticmethod
    def _file_info(file_path: Path, file_type: str) -> Dict[str, Any]:
        """Build the common evidence metadata stored for a discovered file."""
        stat = file_path.stat()
        return {
            "path": str(file_path.resolve()),
            "type": file_type,
            "size": stat.st_size,
            "st_mtime": stat.st_mtime,
            "mtime": format_timestamp_to_date(stat.st_mtime),
            "st_atime": stat.st_atime,
            "atime": format_timestamp_to_date(stat.st_atime),
            "st_ctime": stat.st_ctime,
            "ctime": format_timestamp_to_date(stat.st_ctime),
        }

    def discover_database_files(
        self, folder_path: Path, base_path: Path, verbose: bool = False
    ) -> None:
        """Recursively discover SQLite and plist files under ``folder_path``."""
        try:
            for file_path in folder_path.iterdir():
                if file_path.is_file():
                    relative_name = str(file_path.relative_to(base_path))

                    if detect_sqlite_file(file_path):
                        self.key_files[relative_name] = {
                            "info": self._file_info(file_path, "sqlite")
                        }
                        if verbose:
                            logger.success(
                                f"{file_path} is sqlite file.\n"
                                f"{self.key_files[relative_name]['info']}"
                            )

                    elif detect_plist_file(file_path):
                        self.key_files[relative_name] = {
                            "info": self._file_info(file_path, "plist")
                        }
                        if verbose:
                            logger.success(
                                f"{file_path} is plist file.\n"
                                f"{self.key_files[relative_name]['info']}"
                            )

                    elif detect_encrypted_sqlite_file(file_path):
                        if verbose:
                            logger.info(f"{file_path} is encrypted sqlite file.")
                        self.encrypt_files.append(relative_name)

                elif file_path.is_dir():
                    self.discover_database_files(file_path, base_path, verbose)
                elif verbose:
                    logger.info(f"{file_path} is not KEY file.")

        except (PermissionError, OSError) as exc:
            logger.error(f"Error accessing directory {folder_path}: {exc}")

    def _binary_output_directory(self) -> Path:
        """Return the directory used for exact raw-byte extraction."""
        if self.path_n is None:
            raise RuntimeError("Output path is not configured for binary extraction")
        output_directory = Path(self.path_n) / "extracted_binary"
        output_directory.mkdir(parents=True, exist_ok=True)
        return output_directory

    def _store_binary_blob(self, value: bytes) -> Dict[str, Any]:
        """Store raw bytes without decoding and return reconstructable metadata."""
        digest = hashlib.sha256(value).hexdigest()
        output_path = self._binary_output_directory() / f"{digest}.bin"

        # Content-addressed names make the result stable and deduplicate identical
        # blobs.  The lock prevents two worker threads from racing on the same file.
        with self._binary_write_lock:
            if not output_path.exists():
                temporary_path = output_path.with_name(
                    f".{output_path.name}.{uuid.uuid4().hex}.tmp"
                )
                try:
                    with open(temporary_path, "wb") as file_object:
                        file_object.write(value)
                        file_object.flush()
                        os.fsync(file_object.fileno())
                    os.replace(temporary_path, output_path)
                finally:
                    if temporary_path.exists():
                        temporary_path.unlink()

        return {
            "$binary": {
                "path": str(output_path.resolve()),
                "length": len(value),
                "sha256": digest,
                "encoding": "raw",
            }
        }

    def _process_value(self, value: Any) -> Any:
        """Recursively convert plist objects and preserve opaque binary values."""
        if isinstance(value, bytes):
            if value.startswith(b"bplist"):
                try:
                    return self._process_value(plistlib.loads(value))
                except Exception as exc:
                    logger.warning(
                        "Failed to decode nested bplist; preserving raw bytes: {}",
                        exc,
                    )
            return self._store_binary_blob(value)

        if isinstance(value, dict):
            return {key: self._process_value(item) for key, item in value.items()}

        if isinstance(value, list):
            return [self._process_value(item) for item in value]

        if isinstance(value, tuple):
            return [self._process_value(item) for item in value]

        if isinstance(value, datetime.datetime):
            return str(value)

        if isinstance(value, plistlib.UID):
            return value.data

        return value

    def process_binary_data_in_dict(self, dic: Dict[str, Any]) -> None:
        """Post-process extracted content in place.

        SQLite BLOB values remain ``bytes`` until this stage.  Opaque bytes are
        written exactly to ``extracted_binary/<sha256>.bin`` and replaced with a
        JSON-serializable reference containing path, length, and SHA-256.
        """
        processed = self._process_value(dic)
        dic.clear()
        dic.update(processed)

    @staticmethod
    def _new_temporary_file() -> str:
        descriptor, path = mkstemp(suffix=".dbs2json")
        os.close(descriptor)
        return path

    def _prepare_processing_copy(self, db_name: str, tmp_file: str) -> Dict[str, Any]:
        """Create a processing copy and return snapshot/copy metadata."""
        info = self.key_files[db_name]["info"]
        source_path = info["path"]
        file_type = info["type"]

        if file_type == "sqlite":
            metadata = create_sqlite_snapshot(source_path, tmp_file)
            logger.info(
                "SQLite snapshot for {} used {}; WAL present={}; SHM present={}",
                db_name,
                metadata["method"],
                metadata["sidecars"]["wal"]["present"],
                metadata["sidecars"]["shm"]["present"],
            )
            return metadata

        if file_type == "plist":
            copyfile(source_path, tmp_file)
            return {
                "method": "file_copy",
                "sidecars": {},
            }

        raise ValueError(f"Unknown file type: {file_type}")

    def _extract_processing_copy(
        self, db_name: str, tmp_file: str, strict: bool
    ) -> Dict[str, Any]:
        """Extract and post-process one already prepared temporary file."""
        with self._key_files_lock:
            file_type = self.key_files[db_name]["info"]["type"]

        if file_type == "sqlite":
            db_json = extract_sqlite_to_dict(db_name, tmp_file, strict)
        elif file_type == "plist":
            db_json = extract_plist_to_dict(tmp_file)
        else:
            raise ValueError(f"Unknown file type: {file_type}")

        self.process_binary_data_in_dict(db_json)
        return db_json

    def _process_single_file_threaded(
        self, db_name: str, tmp_file: str, verbose: bool, strict: bool
    ) -> Tuple[str, Dict[str, Any]]:
        """Extract one prepared temporary file in a worker thread."""
        try:
            if verbose:
                logger.info(
                    f"[Thread {threading.current_thread().name}] Start to analyze {db_name}"
                )

            db_json = self._extract_processing_copy(db_name, tmp_file, strict)

            if verbose:
                logger.info(
                    f"[Thread {threading.current_thread().name}] {db_name} analyze done."
                )

            with self._progress_lock:
                self._processed_count += 1
                if verbose and self._total_files > 0:
                    progress = (self._processed_count / self._total_files) * 100
                    logger.info(
                        f"Progress: {self._processed_count}/{self._total_files} "
                        f"({progress:.1f}%)"
                    )

            return db_name, db_json

        except Exception as exc:
            logger.error(
                f"[Thread {threading.current_thread().name}] {db_name} analyze error."
            )
            with self._key_files_lock:
                logger.error(f"File info: {self.key_files[db_name]}")
            logger.error(f"Error: {exc}")
            if strict:
                raise
            return db_name, {}

    def process_database_files(
        self,
        sorted_flag: str = "mtime",
        verbose: bool = False,
        strict: bool = False,
        threads: int = 1,
    ) -> None:
        """Process all discovered files using one or more worker threads."""
        sort_fields = {
            "mtime": "st_mtime",
            "ctime": "st_ctime",
            "atime": "st_atime",
            "size": "size",
        }
        sort_field = sort_fields.get(sorted_flag)
        if sort_field is None:
            logger.warning(f"Unknown sorting flag: {sorted_flag}, using mtime")
            sort_field = "st_mtime"

        key_files_name = sorted(
            self.key_files,
            key=lambda key: self.key_files[key]["info"][sort_field],
            reverse=True,
        )
        self._total_files = len(key_files_name)
        self._processed_count = 0

        if threads == 0:
            import multiprocessing

            threads = min(multiprocessing.cpu_count(), 8)
        elif threads < 1:
            logger.warning(f"Invalid thread count: {threads}, using 1")
            threads = 1

        if not key_files_name:
            logger.warning("No files to process")
            return

        if threads == 1 or len(key_files_name) == 1:
            logger.info("Using single-threaded processing...")
            self._process_single_threaded(key_files_name, verbose, strict)
        else:
            logger.info(f"Using multi-threaded processing with {threads} threads...")
            self._process_multi_threaded(key_files_name, verbose, strict, threads)

    def _process_single_threaded(
        self, key_files_name: List[str], verbose: bool, strict: bool
    ) -> None:
        """Create and process one independent snapshot per source file."""
        for db_name in key_files_name:
            tmp_file = self._new_temporary_file()
            try:
                try:
                    snapshot_metadata = self._prepare_processing_copy(db_name, tmp_file)
                    self.key_files[db_name]["info"]["snapshot"] = snapshot_metadata

                    if verbose:
                        logger.info(f"Start to analyze {db_name}")

                    db_json = self._extract_processing_copy(db_name, tmp_file, strict)

                    if verbose:
                        logger.info(f"{db_name} analyze done.")
                except Exception as exc:
                    db_json = {}
                    logger.error(f"{db_name} analyze error.")
                    logger.error(f"File info: {self.key_files[db_name]}")
                    logger.error(f"Error: {exc}")
                    self.key_files[db_name]["info"]["snapshot"] = {
                        "method": "failed",
                        "error": str(exc),
                    }
                    if strict:
                        raise

                self.key_files[db_name]["content"] = db_json
            finally:
                try:
                    os.unlink(tmp_file)
                except OSError:
                    pass

    def _process_multi_threaded(
        self, key_files_name: List[str], verbose: bool, strict: bool, threads: int
    ) -> None:
        """Create consistent snapshots, then extract them concurrently."""
        with ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_db_name: Dict[Any, Tuple[str, str]] = {}

            for db_name in key_files_name:
                tmp_file = self._new_temporary_file()
                try:
                    snapshot_metadata = self._prepare_processing_copy(db_name, tmp_file)
                    with self._key_files_lock:
                        self.key_files[db_name]["info"]["snapshot"] = snapshot_metadata
                except Exception as exc:
                    with self._key_files_lock:
                        self.key_files[db_name]["content"] = {}
                        self.key_files[db_name]["info"]["snapshot"] = {
                            "method": "failed",
                            "error": str(exc),
                        }
                    try:
                        os.unlink(tmp_file)
                    except OSError:
                        pass
                    logger.error(f"Failed to snapshot {db_name}: {exc}")
                    if strict:
                        raise
                    continue

                future = executor.submit(
                    self._process_single_file_threaded,
                    db_name,
                    tmp_file,
                    verbose,
                    strict,
                )
                future_to_db_name[future] = (db_name, tmp_file)

            for future in as_completed(future_to_db_name):
                db_name, tmp_file = future_to_db_name[future]
                try:
                    result_db_name, db_json = future.result()
                    with self._key_files_lock:
                        self.key_files[result_db_name]["content"] = db_json
                except Exception as exc:
                    logger.error(f"Unexpected error processing {db_name}: {exc}")
                    with self._key_files_lock:
                        self.key_files[db_name]["content"] = {}
                    if strict:
                        raise
                finally:
                    try:
                        os.unlink(tmp_file)
                    except OSError:
                        pass

    def set_paths(self, input_path: Path, output_path: Path) -> None:
        self.inp = input_path
        self.opt = output_path
        self.path_n = output_path

    def get_key_files(self) -> Dict[str, Any]:
        return self.key_files

    def get_encrypted_files(self) -> List[str]:
        return self.encrypt_files

    def process_single_file(self, file_path: Path, verbose: bool = False) -> None:
        """Process one SQLite or plist file using the same snapshot path as batch mode."""
        if detect_sqlite_file(file_path):
            file_type = "sqlite"
        elif detect_plist_file(file_path):
            file_type = "plist"
        elif detect_encrypted_sqlite_file(file_path):
            if verbose:
                logger.info(f"{file_path} is encrypted SQLite file.")
            self.encrypt_files.append(file_path.name)
            return
        else:
            if verbose:
                logger.info(f"{file_path} is not a supported database file.")
            return

        file_name = file_path.name
        self.key_files[file_name] = {
            "info": self._file_info(file_path, file_type)
        }

        if verbose:
            logger.success(
                f"{file_path} is {file_type} file.\n"
                f"{self.key_files[file_name]['info']}"
            )

        tmp_file = self._new_temporary_file()
        try:
            snapshot_metadata = self._prepare_processing_copy(file_name, tmp_file)
            self.key_files[file_name]["info"]["snapshot"] = snapshot_metadata
            self.key_files[file_name]["content"] = self._extract_processing_copy(
                file_name, tmp_file, strict=False
            )
            if verbose:
                logger.info(f"{file_name} analyze done.")
        except Exception as exc:
            logger.error(f"{file_name} analyze error.")
            logger.error(f"File info: {self.key_files[file_name]}")
            logger.error(f"Error: {exc}")
            self.key_files[file_name]["info"]["snapshot"] = {
                "method": "failed",
                "error": str(exc),
            }
            self.key_files[file_name]["content"] = {}
        finally:
            try:
                os.unlink(tmp_file)
            except OSError:
                pass
