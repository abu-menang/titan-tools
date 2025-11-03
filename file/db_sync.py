"""
MariaDB persistence helpers for file scanning results.

This module maps filesystem snapshots into the canonical `media` schema by
upserting base paths, directories, and media files, and by recording scan
runs plus change events. It integrates with `file.scanner.scan_filesystem`
so that each scan reflects the latest on-disk state.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from common.base.logging import get_logger

try:  # Optional dependency; imported lazily when database configuration is provided.
    import pymysql
    from pymysql.cursors import DictCursor
except ModuleNotFoundError:  # pragma: no cover - handled at runtime when DB isn't configured.
    pymysql = None  # type: ignore[assignment]
    DictCursor = None  # type: ignore[assignment]


log = get_logger(__name__)


class DatabaseSyncError(RuntimeError):
    """Raised when MariaDB operations fail."""


def _require_driver() -> None:
    if pymysql is None:
        raise DatabaseSyncError(
            "PyMySQL is not installed. Install the `PyMySQL` package to enable MariaDB synchronisation."
        )


def _normalize_path_text(path: Path) -> str:
    """Return a normalised, platform-agnostic textual representation of a path."""
    return os.path.normpath(str(path))


def _hash_path(path_text: str) -> bytes:
    """Return a consistent 16-byte hash for deduplicating paths."""
    return hashlib.md5(path_text.encode("utf-8"), usedforsecurity=False).digest()


def _dt_from_timestamp(value: Optional[float]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).replace(tzinfo=None)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def _json_dumps(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class DirectorySnapshot:
    full_path: str
    path_hash: bytes
    parent_path: Optional[str]
    depth: int
    modified_time: Optional[datetime]


@dataclass(frozen=True)
class FileSnapshot:
    full_path: str
    path_hash: bytes
    directory_path: Optional[str]
    file_name: str
    extension: Optional[str]
    size_bytes: Optional[int]
    checksum_sha256: Optional[bytes]
    modified_time: Optional[datetime]
    created_time: Optional[datetime]
    accessed_time: Optional[datetime]


SCHEMA_STATEMENTS: Tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS project (
        project_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        description TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS base_path (
        base_path_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        project_id BIGINT UNSIGNED NOT NULL,
        label VARCHAR(100) NOT NULL,
        root_path VARCHAR(1024) NOT NULL,
        path_hash BINARY(16) NOT NULL,
        last_observed_at DATETIME NULL,
        UNIQUE KEY uq_base_path (project_id, path_hash),
        FOREIGN KEY (project_id) REFERENCES project(project_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS directory (
        directory_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        project_id BIGINT UNSIGNED NOT NULL,
        base_path_id BIGINT UNSIGNED NOT NULL,
        parent_directory_id BIGINT UNSIGNED NULL,
        full_path VARCHAR(2048) NOT NULL,
        path_hash BINARY(16) NOT NULL,
        depth SMALLINT UNSIGNED NOT NULL,
        last_observed_at DATETIME NOT NULL,
        is_deleted TINYINT(1) NOT NULL DEFAULT 0,
        UNIQUE KEY uq_directory (project_id, path_hash),
        KEY idx_directory_parent (parent_directory_id),
        KEY idx_directory_base (base_path_id),
        FOREIGN KEY (project_id) REFERENCES project(project_id)
            ON DELETE CASCADE,
        FOREIGN KEY (base_path_id) REFERENCES base_path(base_path_id)
            ON DELETE CASCADE,
        FOREIGN KEY (parent_directory_id) REFERENCES directory(directory_id)
            ON DELETE SET NULL
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS library_entry (
        library_entry_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        directory_id BIGINT UNSIGNED NOT NULL,
        entry_type ENUM('movie','series','book','collection','other') NOT NULL,
        display_title VARCHAR(512) NOT NULL,
        sort_title VARCHAR(512) DEFAULT NULL,
        metadata_json JSON,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_library_entry (directory_id, entry_type),
        FOREIGN KEY (directory_id) REFERENCES directory(directory_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS media_file (
        media_file_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        directory_id BIGINT UNSIGNED NOT NULL,
        library_entry_id BIGINT UNSIGNED NULL,
        file_name VARCHAR(1024) NOT NULL,
        extension VARCHAR(16),
        canonical_path VARCHAR(2048) NOT NULL,
        path_hash BINARY(16) NOT NULL,
        size_bytes BIGINT UNSIGNED NULL,
        checksum_sha256 BINARY(32) NULL,
        modified_time DATETIME NULL,
        accessed_time DATETIME NULL,
        created_time DATETIME NULL,
        last_observed_at DATETIME NOT NULL,
        is_deleted TINYINT(1) NOT NULL DEFAULT 0,
        UNIQUE KEY uq_media_file_hash (directory_id, path_hash),
        UNIQUE KEY uq_media_file_name (directory_id, file_name),
        KEY idx_media_file_library (library_entry_id),
        FOREIGN KEY (directory_id) REFERENCES directory(directory_id)
            ON DELETE CASCADE,
        FOREIGN KEY (library_entry_id) REFERENCES library_entry(library_entry_id)
            ON DELETE SET NULL
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS media_metadata (
        media_metadata_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        media_file_id BIGINT UNSIGNED NOT NULL,
        title VARCHAR(512),
        metadata_json JSON,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (media_file_id) REFERENCES media_file(media_file_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS media_track (
        media_track_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        media_file_id BIGINT UNSIGNED NOT NULL,
        track_type ENUM('video','audio','subtitle','attachment') NOT NULL,
        track_index SMALLINT UNSIGNED NOT NULL,
        codec VARCHAR(128),
        language VARCHAR(32),
        channels VARCHAR(32),
        bitrate INT,
        resolution VARCHAR(32),
        default_flag TINYINT(1) NOT NULL DEFAULT 0,
        forced_flag TINYINT(1) NOT NULL DEFAULT 0,
        track_info JSON,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_media_track (media_file_id, track_type, track_index),
        FOREIGN KEY (media_file_id) REFERENCES media_file(media_file_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_run (
        scan_run_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        project_id BIGINT UNSIGNED NOT NULL,
        started_at DATETIME NOT NULL,
        completed_at DATETIME NULL,
        status ENUM('running','completed','failed','no_change') NOT NULL,
        scanner_version VARCHAR(50),
        items_scanned INT UNSIGNED NULL,
        notes TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (project_id) REFERENCES project(project_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB
    """,
    """
    CREATE TABLE IF NOT EXISTS change_event (
        change_event_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        scan_run_id BIGINT UNSIGNED NOT NULL,
        media_file_id BIGINT UNSIGNED NULL,
        directory_id BIGINT UNSIGNED NULL,
        change_type ENUM('added','removed','updated','moved','metadata_changed') NOT NULL,
        change_detail JSON,
        detected_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (scan_run_id) REFERENCES scan_run(scan_run_id)
            ON DELETE CASCADE,
        FOREIGN KEY (media_file_id) REFERENCES media_file(media_file_id)
            ON DELETE SET NULL,
        FOREIGN KEY (directory_id) REFERENCES directory(directory_id)
            ON DELETE SET NULL
    ) ENGINE=InnoDB
    """,
)


class MariaDBSync:
    """
    Handles synchronisation of filesystem scan results into the MariaDB schema.
    """

    def __init__(self, config: Mapping[str, object]):
        _require_driver()
        self._config = dict(config)
        self._host: str = str(self._config.get("host") or "127.0.0.1")
        self._port: int = int(self._config.get("port") or 3306)
        self._user: str = str(self._config.get("user") or "")
        self._password: str = str(self._config.get("password") or "")
        self._database: str = str(self._config.get("name") or self._config.get("database") or "media")
        self._ensure_schema: bool = bool(self._config.get("ensure_schema", True))
        self._project_name: str = str(self._config.get("project") or "default")
        self._base_label_override: Optional[str] = (
            str(self._config["base_label"]) if self._config.get("base_label") else None
        )
        self._compute_checksums: bool = bool(self._config.get("compute_checksums", False))
        self._checksum_algorithm: str = str(self._config.get("checksum_algorithm") or "sha256")
        if self._checksum_algorithm.lower() != "sha256":
            raise DatabaseSyncError("Only SHA-256 checksums are currently supported.")

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def apply(
        self,
        *,
        root: Path,
        directory_paths: Sequence[Path],
        file_paths: Sequence[Path],
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
    ) -> None:
        """
        Synchronise the provided filesystem snapshot into MariaDB.
        """
        root = root.resolve()
        started_at = started_at or _utcnow()
        completed_at = completed_at or _utcnow()
        directory_snapshots = self._build_directory_snapshots(root, directory_paths)
        file_snapshots = self._build_file_snapshots(root, file_paths)
        log.debug(
            "Preparing database sync: %s directories, %s files",
            len(directory_snapshots),
            len(file_snapshots),
        )
        connection = self._connect()
        try:
            if self._ensure_schema:
                self._install_schema(connection)
            with connection.cursor() as cursor:
                project_id = self._ensure_project(cursor)
                base_path_id = self._ensure_base_path(cursor, project_id, root)
                scan_run_id = self._start_scan_run(cursor, project_id, base_path_id, root, started_at)
            connection.commit()

            dir_result = self._sync_directories(
                connection,
                project_id,
                base_path_id,
                directory_snapshots,
                completed_at,
                scan_run_id,
            )
            self._sync_files(
                connection,
                project_id,
                base_path_id,
                dir_result,
                file_snapshots,
                completed_at,
                scan_run_id,
            )
            status = "completed"
            if not file_snapshots and not directory_snapshots:
                status = "no_change"
            self._complete_scan_run(connection, scan_run_id, completed_at, status, len(directory_snapshots), len(file_snapshots))
            connection.commit()
        except Exception:
            with contextlib.suppress(Exception):
                connection.rollback()
            log.exception("Failed to synchronise scan results to MariaDB.")
            raise
        finally:
            connection.close()

    # ------------------------------------------------------------------ #
    # Connection and schema helpers
    # ------------------------------------------------------------------ #

    def _connect(self) -> "pymysql.connections.Connection":
        try:
            return pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database,
                charset="utf8mb4",
                autocommit=False,
                cursorclass=DictCursor,
            )
        except Exception as exc:  # pragma: no cover - connectivity errors depend on environment
            raise DatabaseSyncError(f"Unable to connect to MariaDB at {self._host}:{self._port}") from exc

    def _install_schema(self, connection: "pymysql.connections.Connection") -> None:
        with connection.cursor() as cursor:
            for statement in SCHEMA_STATEMENTS:
                cursor.execute(statement)
        connection.commit()

    # ------------------------------------------------------------------ #
    # Project / base-path helpers
    # ------------------------------------------------------------------ #

    def _ensure_project(self, cursor: "pymysql.cursors.Cursor") -> int:
        cursor.execute(
            """
            INSERT INTO project (name)
            VALUES (%s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
            """,
            (self._project_name,),
        )
        cursor.execute("SELECT project_id FROM project WHERE name = %s", (self._project_name,))
        row = cursor.fetchone()
        if not row:
            raise DatabaseSyncError("Failed to retrieve or create project entry.")
        return int(row["project_id"])

    def _ensure_base_path(
        self,
        cursor: "pymysql.cursors.Cursor",
        project_id: int,
        root: Path,
    ) -> int:
        root_text = _normalize_path_text(root)
        path_hash = _hash_path(root_text)
        label = self._base_label_override or root.name or root_text
        cursor.execute(
            """
            INSERT INTO base_path (project_id, label, root_path, path_hash)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                label = VALUES(label),
                root_path = VALUES(root_path)
            """,
            (project_id, label, root_text, path_hash),
        )
        cursor.execute(
            "SELECT base_path_id FROM base_path WHERE project_id = %s AND path_hash = %s",
            (project_id, path_hash),
        )
        row = cursor.fetchone()
        if not row:
            raise DatabaseSyncError("Failed to upsert base_path entry.")
        return int(row["base_path_id"])

    # ------------------------------------------------------------------ #
    # Scan run handling
    # ------------------------------------------------------------------ #

    def _start_scan_run(
        self,
        cursor: "pymysql.cursors.Cursor",
        project_id: int,
        base_path_id: int,
        root: Path,
        started_at: datetime,
    ) -> int:
        cursor.execute(
            """
            INSERT INTO scan_run (project_id, started_at, status, scanner_version, notes)
            VALUES (%s, %s, 'running', %s, %s)
            """,
            (
                project_id,
                started_at,
                "file.scanner",
                f"base_path_id={base_path_id} root={_normalize_path_text(root)}",
            ),
        )
        return int(cursor.lastrowid)

    def _complete_scan_run(
        self,
        connection: "pymysql.connections.Connection",
        scan_run_id: int,
        completed_at: datetime,
        status: str,
        total_directories: int,
        total_files: int,
    ) -> None:
        with connection.cursor() as cursor:
            items_scanned = total_directories + total_files
            cursor.execute(
                """
                UPDATE scan_run
                SET completed_at = %s,
                    status = %s,
                    items_scanned = %s
                WHERE scan_run_id = %s
                """,
                (completed_at, status, items_scanned, scan_run_id),
            )

    # ------------------------------------------------------------------ #
    # Snapshot builders
    # ------------------------------------------------------------------ #

    def _build_directory_snapshots(
        self,
        root: Path,
        directory_paths: Sequence[Path],
    ) -> List[DirectorySnapshot]:
        snapshots: List[DirectorySnapshot] = []
        try:
            normalized_root = root.resolve()
        except FileNotFoundError:
            normalized_root = root
        root_text = _normalize_path_text(normalized_root)
        unique_paths = {normalized_root}
        unique_paths.update(path.resolve() for path in directory_paths)
        for directory_path in sorted(unique_paths):
            try:
                relative = directory_path.relative_to(normalized_root)
            except ValueError:
                log.debug("Skipping directory outside root: %s", directory_path)
                continue
            depth = len(relative.parts)
            if directory_path == normalized_root:
                parent_text = None
            else:
                parent = directory_path.parent
                try:
                    parent.relative_to(normalized_root)
                    parent_text = _normalize_path_text(parent)
                except ValueError:
                    parent_text = None
            path_text = _normalize_path_text(directory_path)
            stat_result = None
            with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
                stat_result = directory_path.stat()
            snapshots.append(
                DirectorySnapshot(
                    full_path=path_text,
                    path_hash=_hash_path(path_text),
                    parent_path=parent_text,
                    depth=depth,
                    modified_time=None,
                )
            )
        return snapshots

    def _build_file_snapshots(
        self,
        root: Path,
        file_paths: Sequence[Path],
    ) -> List[FileSnapshot]:
        snapshots: List[FileSnapshot] = []
        try:
            normalized_root = root.resolve()
        except FileNotFoundError:
            normalized_root = root
        for file_path in sorted({p.resolve() for p in file_paths}):
            try:
                file_path.relative_to(normalized_root)
            except ValueError:
                log.debug("Skipping file outside root: %s", file_path)
                continue
            directory = file_path.parent
            directory_text = _normalize_path_text(directory)
            stat_result = None
            with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
                stat_result = file_path.stat()
            size_bytes: Optional[int] = stat_result.st_size if stat_result else None
            modified_time = _dt_from_timestamp(stat_result.st_mtime) if stat_result else None
            created_time = _dt_from_timestamp(stat_result.st_ctime) if stat_result else None
            accessed_time = _dt_from_timestamp(stat_result.st_atime) if stat_result else None
            checksum: Optional[bytes] = None
            if self._compute_checksums and file_path.is_file():
                checksum = self._compute_sha256(file_path)
            path_text = _normalize_path_text(file_path)
            suffix = file_path.suffix[1:].lower() if file_path.suffix else None
            snapshots.append(
                FileSnapshot(
                    full_path=path_text,
                    path_hash=_hash_path(path_text),
                    directory_path=directory_text,
                    file_name=file_path.name,
                    extension=suffix,
                    size_bytes=size_bytes,
                    checksum_sha256=checksum,
                    modified_time=modified_time,
                    created_time=created_time,
                    accessed_time=accessed_time,
                )
            )
        return snapshots

    # ------------------------------------------------------------------ #
    # Directory sync
    # ------------------------------------------------------------------ #

    @dataclass
    class _DirectorySyncResult:
        directory_id_by_path: Dict[str, int]
        directory_id_by_hash: Dict[bytes, int]

    def _sync_directories(
        self,
        connection: "pymysql.connections.Connection",
        project_id: int,
        base_path_id: int,
        snapshots: Sequence[DirectorySnapshot],
        observed_at: datetime,
        scan_run_id: int,
    ) -> "_DirectorySyncResult":
        directory_id_by_path: Dict[str, int] = {}
        directory_id_by_hash: Dict[bytes, int] = {}
        existing: Dict[bytes, Dict[str, object]] = {}
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT directory_id, path_hash, full_path, parent_directory_id, is_deleted,
                       last_observed_at, depth, base_path_id
                FROM directory
                WHERE project_id = %s AND base_path_id = %s
                """,
                (project_id, base_path_id),
            )
            for row in cursor.fetchall():
                row_hash: bytes = bytes(row["path_hash"])
                existing[row_hash] = dict(row)
                existing[row_hash]["seen"] = False
                directory_id_by_hash[row_hash] = int(row["directory_id"])
                directory_id_by_path[row["full_path"]] = int(row["directory_id"])

        cursor = connection.cursor()
        try:
            # Ensure deterministic processing (parents before children) by depth / path
            ordered_snapshots = sorted(snapshots, key=lambda item: (item.depth, item.full_path))
            for snapshot in ordered_snapshots:
                parent_id = None
                if snapshot.parent_path:
                    parent_id = directory_id_by_path.get(snapshot.parent_path)
                current = existing.get(snapshot.path_hash)
                if current:
                    updates = []
                    params: List[object] = []
                    if current["is_deleted"]:
                        updates.append("is_deleted = 0")
                    if current.get("parent_directory_id") != parent_id:
                        updates.append("parent_directory_id = %s")
                        params.append(parent_id)
                    if current.get("base_path_id") != base_path_id:
                        updates.append("base_path_id = %s")
                        params.append(base_path_id)
                    if current.get("depth") != snapshot.depth:
                        updates.append("depth = %s")
                        params.append(snapshot.depth)
                    updates.append("last_observed_at = %s")
                    params.append(observed_at)
                    if updates:
                        set_clause = ", ".join(updates)
                        cursor.execute(
                            f"UPDATE directory SET {set_clause} WHERE directory_id = %s",
                            (*params, current["directory_id"]),
                        )
                    existing[snapshot.path_hash]["seen"] = True
                    directory_id = int(current["directory_id"])
                else:
                    cursor.execute(
                        """
                        INSERT INTO directory (
                            project_id,
                            base_path_id,
                            parent_directory_id,
                            full_path,
                            path_hash,
                            depth,
                            last_observed_at,
                            is_deleted
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
                        """,
                        (
                            project_id,
                            base_path_id,
                            parent_id,
                            snapshot.full_path,
                            snapshot.path_hash,
                            snapshot.depth,
                            observed_at,
                        ),
                    )
                    directory_id = int(cursor.lastrowid)
                    directory_id_by_hash[snapshot.path_hash] = directory_id
                    directory_id_by_path[snapshot.full_path] = directory_id
                    existing[snapshot.path_hash] = {
                        "directory_id": directory_id,
                        "seen": True,
                        "is_deleted": 0,
                        "full_path": snapshot.full_path,
                    }
                    self._record_change_event(
                        connection,
                        scan_run_id,
                        media_file_id=None,
                        directory_id=directory_id,
                        change_type="added",
                        change_detail={"reason": "new_directory", "path": snapshot.full_path},
                    )

            # Mark missing directories as deleted
            for path_hash, metadata in existing.items():
                if metadata.get("seen"):
                    continue
                if metadata.get("is_deleted"):
                    continue
                cursor.execute(
                    """
                    UPDATE directory
                    SET is_deleted = 1,
                        last_observed_at = %s
                    WHERE directory_id = %s
                    """,
                    (observed_at, metadata["directory_id"]),
                )
                directory_id = int(metadata["directory_id"])
                directory_id_by_hash[path_hash] = directory_id
                directory_id_by_path.setdefault(metadata["full_path"], directory_id)
                self._record_change_event(
                    connection,
                    scan_run_id,
                    media_file_id=None,
                    directory_id=directory_id,
                    change_type="removed",
                    change_detail={"reason": "missing_in_scan", "path": metadata["full_path"]},
                )
        finally:
            cursor.close()

        return MariaDBSync._DirectorySyncResult(
            directory_id_by_path=directory_id_by_path,
            directory_id_by_hash=directory_id_by_hash,
        )

    # ------------------------------------------------------------------ #
    # File sync
    # ------------------------------------------------------------------ #

    def _sync_files(
        self,
        connection: "pymysql.connections.Connection",
        project_id: int,
        base_path_id: int,
        directory_result: "_DirectorySyncResult",
        snapshots: Sequence[FileSnapshot],
        observed_at: datetime,
        scan_run_id: int,
    ) -> None:
        existing: Dict[bytes, Dict[str, object]] = {}
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT mf.media_file_id,
                       mf.path_hash,
                       mf.canonical_path,
                       mf.directory_id,
                       mf.file_name,
                       mf.extension,
                       mf.size_bytes,
                       mf.checksum_sha256,
                       mf.modified_time,
                       mf.accessed_time,
                       mf.created_time,
                       mf.is_deleted,
                       mf.last_observed_at
                FROM media_file AS mf
                JOIN directory AS d ON d.directory_id = mf.directory_id
                WHERE d.project_id = %s AND d.base_path_id = %s
                """,
                (project_id, base_path_id),
            )
            for row in cursor.fetchall():
                row_hash: bytes = bytes(row["path_hash"])
                row["seen"] = False
                existing[row_hash] = dict(row)

        cursor = connection.cursor()
        try:
            for snapshot in snapshots:
                directory_id = None
                if snapshot.directory_path:
                    directory_id = directory_result.directory_id_by_path.get(snapshot.directory_path)
                if directory_id is None:
                    log.warning(
                        "Missing directory mapping for file %s (directory: %s)",
                        snapshot.full_path,
                        snapshot.directory_path,
                    )
                    continue
                current = existing.get(snapshot.path_hash)
                if current:
                    updates = []
                    params: List[object] = []
                    change_detail: Dict[str, object] = {}
                    if current["is_deleted"]:
                        updates.append("is_deleted = 0")
                    if current.get("directory_id") != directory_id:
                        updates.append("directory_id = %s")
                        params.append(directory_id)
                        change_detail["directory_id"] = directory_id
                    if current.get("file_name") != snapshot.file_name:
                        updates.append("file_name = %s")
                        params.append(snapshot.file_name)
                        change_detail["file_name"] = snapshot.file_name
                    if current.get("extension") != snapshot.extension:
                        updates.append("extension = %s")
                        params.append(snapshot.extension)
                    if snapshot.size_bytes is not None and current.get("size_bytes") != snapshot.size_bytes:
                        updates.append("size_bytes = %s")
                        params.append(snapshot.size_bytes)
                        change_detail["size_bytes"] = snapshot.size_bytes
                    if snapshot.modified_time and current.get("modified_time") != snapshot.modified_time:
                        updates.append("modified_time = %s")
                        params.append(snapshot.modified_time)
                        change_detail["modified_time"] = snapshot.modified_time.isoformat()
                    if snapshot.accessed_time and current.get("accessed_time") != snapshot.accessed_time:
                        updates.append("accessed_time = %s")
                        params.append(snapshot.accessed_time)
                    if snapshot.created_time and current.get("created_time") != snapshot.created_time:
                        updates.append("created_time = %s")
                        params.append(snapshot.created_time)
                    if self._compute_checksums:
                        if (
                            snapshot.checksum_sha256 is not None
                            and current.get("checksum_sha256") != snapshot.checksum_sha256
                        ):
                            updates.append("checksum_sha256 = %s")
                            params.append(snapshot.checksum_sha256)
                            change_detail["checksum"] = snapshot.checksum_sha256.hex()
                    updates.append("last_observed_at = %s")
                    params.append(observed_at)
                    if updates:
                        set_clause = ", ".join(updates)
                        cursor.execute(
                            f"UPDATE media_file SET {set_clause} WHERE media_file_id = %s",
                            (*params, current["media_file_id"]),
                        )
                        if change_detail:
                            self._record_change_event(
                                connection,
                                scan_run_id,
                                media_file_id=int(current["media_file_id"]),
                                directory_id=directory_id,
                                change_type="updated",
                                change_detail={**change_detail, "path": snapshot.full_path},
                            )
                    existing[snapshot.path_hash]["seen"] = True
                else:
                    cursor.execute(
                        """
                        INSERT INTO media_file (
                            directory_id,
                            library_entry_id,
                            file_name,
                            extension,
                            canonical_path,
                            path_hash,
                            size_bytes,
                            checksum_sha256,
                            modified_time,
                            created_time,
                            accessed_time,
                            last_observed_at,
                            is_deleted
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)
                        """,
                        (
                            directory_id,
                            None,
                            snapshot.file_name,
                            snapshot.extension,
                            snapshot.full_path,
                            snapshot.path_hash,
                            snapshot.size_bytes,
                            snapshot.checksum_sha256,
                            snapshot.modified_time,
                            snapshot.created_time,
                            snapshot.accessed_time,
                            observed_at,
                        ),
                    )
                    media_file_id = int(cursor.lastrowid)
                    existing[snapshot.path_hash] = {
                        "media_file_id": media_file_id,
                        "seen": True,
                        "is_deleted": 0,
                        "canonical_path": snapshot.full_path,
                        "directory_id": directory_id,
                    }
                    self._record_change_event(
                        connection,
                        scan_run_id,
                        media_file_id=media_file_id,
                        directory_id=directory_id,
                        change_type="added",
                        change_detail={"reason": "new_file", "path": snapshot.full_path},
                    )

            # Mark missing files as deleted
            for metadata in existing.values():
                if metadata.get("seen"):
                    continue
                if metadata.get("is_deleted"):
                    continue
                cursor.execute(
                    """
                    UPDATE media_file
                    SET is_deleted = 1,
                        last_observed_at = %s
                    WHERE media_file_id = %s
                    """,
                    (observed_at, metadata["media_file_id"]),
                )
                self._record_change_event(
                    connection,
                    scan_run_id,
                    media_file_id=int(metadata["media_file_id"]),
                    directory_id=metadata.get("directory_id"),
                    change_type="removed",
                    change_detail={"reason": "missing_in_scan", "path": metadata.get("canonical_path")},
                )
        finally:
            cursor.close()

    # ------------------------------------------------------------------ #
    # Change event helper
    # ------------------------------------------------------------------ #

    def _record_change_event(
        self,
        connection: "pymysql.connections.Connection",
        scan_run_id: int,
        *,
        media_file_id: Optional[int],
        directory_id: Optional[int],
        change_type: str,
        change_detail: Optional[Mapping[str, object]] = None,
    ) -> None:
        detail_text = _json_dumps(change_detail or {})
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO change_event (
                    scan_run_id,
                    media_file_id,
                    directory_id,
                    change_type,
                    change_detail
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (scan_run_id, media_file_id, directory_id, change_type, detail_text),
            )

    # ------------------------------------------------------------------ #
    # Checksum helper
    # ------------------------------------------------------------------ #

    def _compute_sha256(self, path: Path) -> Optional[bytes]:
        buffer_size = 1024 * 1024
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                while True:
                    chunk = handle.read(buffer_size)
                    if not chunk:
                        break
                    digest.update(chunk)
            return digest.digest()
        except (FileNotFoundError, PermissionError, OSError):
            log.warning("Unable to read file for checksum: %s", path)
            return None
