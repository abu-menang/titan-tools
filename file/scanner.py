"""
file.scanner

Lightweight filesystem scanner that records directory and file entries into an
XLS workbook (with optional CSV mirrors) suitable for manual editing. Each row
captures the absolute path, the type (`d` for directories, `f` for files), the
current name, and blank columns for both edited names and metadata titles. The
structure matches the `mkv_scan_name_list` output so downstream tools can
consume either format.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

from common.base.logging import get_logger
from common.shared.report import ColumnSpec, write_tabular_reports
from common.shared.utils import Progress

from .utils import resolve_output_directory

log = get_logger(__name__)

FILE_SCAN_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("type", "type", width=6),
    ColumnSpec("name", "name", width=40),
    ColumnSpec("edited_name", "edited_name", width=40),
    ColumnSpec("title", "title", width=40),
    ColumnSpec("edited_title", "edited_title", width=40),
    ColumnSpec("path", "path", width=80),
]


def _is_relative_to(path: Path, ancestor: Path) -> bool:
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def _iter_entries(root: Path, exclude_dir: Optional[Path]) -> Iterator[Tuple[Path, str]]:
    """Yield (path, type_code) for the root, its directories, and files."""

    root = root.resolve()

    for dirpath, dirnames, filenames in os.walk(root):
        dir_path = Path(dirpath)
        filtered_dirnames = []
        for dirname in dirnames:
            candidate = dir_path / dirname
            try:
                resolved_candidate = candidate.resolve()
            except FileNotFoundError:
                resolved_candidate = candidate
            if exclude_dir and _is_relative_to(resolved_candidate, exclude_dir.resolve()):
                continue
            filtered_dirnames.append(dirname)
            yield candidate, "d"
        dirnames[:] = filtered_dirnames
        for filename in filenames:
            path = dir_path / filename
            try:
                resolved_path = path.resolve()
            except FileNotFoundError:
                resolved_path = path
            if exclude_dir and _is_relative_to(resolved_path, exclude_dir.resolve()):
                continue
            yield path, "f"


def _strip_extension(path: Path) -> Tuple[str, str]:
    suffix = path.suffix
    if suffix:
        base = path.with_suffix("").name
        return base, suffix
    return path.name, ""


_PAREN_SUFFIX_RE = re.compile(r"(?:\s*\([^)]*\))+\s*$")
_TRUNCATE_AFTER_PATTERNS = [").", "].", "}.", "). "]


def _remove_parenthetical_suffix(name: str) -> str:
    return _PAREN_SUFFIX_RE.sub("", name).strip()


def _remove_release_suffix(name: str) -> str:
    match = re.search(r"\)\s*\.", name)
    if match:
        return name[: match.start() + 1].strip()
    if "." in name:
        head, tail = name.split(".", 1)
        if tail:
            return head.strip()
    return name


def _move_leading_article(name: str) -> str:
    lowered = name.lower()
    if lowered.startswith("the "):
        return f"{name[4:].strip()}, The"
    if lowered.startswith("a "):
        return f"{name[2:].strip()}, A"
    return name


def _build_names(path: Path, type_code: str) -> Tuple[str, str]:
    if type_code == "f":
        base_name, _ = _strip_extension(path)
    else:
        base_name, _ = path.name, ""

    cleaned = _remove_parenthetical_suffix(base_name)
    cleaned = _remove_release_suffix(cleaned)
    edited = _move_leading_article(cleaned)
    return base_name, edited


def scan_filesystem(
    root: Path,
    *,
    output_dir: Optional[Path] = None,
    base_name: str = "file_scan",
    include_progress: bool = True,
    batch_size: Optional[int] = None,
) -> List[Path]:
    """
    Scan a directory tree and write CSV report files.

    Args:
        root: Directory to scan (recursively).
        output_dir: Destination directory for the output (defaults to cwd).
        base_name: Base name for the generated files.
        include_progress: Display a progress bar via common.shared.utils.Progress.
        batch_size: Optional chunk size for splitting large outputs.
        Returns:
            List of generated CSV file paths (one entry when batching is disabled).
    """

    root = root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root directory not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    directory_rows: List[dict] = []
    directory_key_map: Dict[str, int] = {}
    file_rows: List[dict] = []
    target_dir = resolve_output_directory(root, output_dir)
    exclude_dir = target_dir
    iterator: Iterable[Tuple[Path, str]] = _iter_entries(root, exclude_dir)
    iterable = Progress(iterator, desc="Scanning") if include_progress else iterator

    for path, type_code in iterable:
        original_name, edited_name = _build_names(path, type_code)
        row = {
            "type": type_code,
            "name": original_name,
            "edited_name": edited_name,
            "title": "",
            "edited_title": "",
            "path": str(path),
        }

        if type_code == "d":
            directory_rows.append(row)
            index = len(directory_rows) - 1
            directory_key_map.setdefault(str(path), index)
            try:
                directory_key_map.setdefault(str(path.resolve()), index)
            except Exception:
                pass
        else:
            file_rows.append(row)

    # Ensure deterministic ordering so exported rows are sorted by path.
    file_rows.sort(key=lambda row: row["path"])

    try:
        normalized_batch = int(batch_size) if batch_size is not None else 0
    except (TypeError, ValueError):
        normalized_batch = 0
    if normalized_batch < 0:
        normalized_batch = 0

    if normalized_batch <= 0:
        file_chunks: List[List[dict]] = [file_rows] if file_rows else []
    else:
        file_chunks = [
            file_rows[i : i + normalized_batch]
            for i in range(0, len(file_rows), normalized_batch)
        ]

    def _collect_directory_rows(rows_for_chunk: List[dict]) -> List[dict]:
        required_indices: Set[int] = set()
        for file_row in rows_for_chunk:
            path_obj = Path(file_row["path"])
            current = path_obj.parent
            while True:
                candidates = {str(current)}
                try:
                    candidates.add(str(current.resolve()))
                except Exception:
                    pass
                for key in candidates:
                    index = directory_key_map.get(key)
                    if index is not None:
                        required_indices.add(index)
                if current.parent == current:
                    break
                current = current.parent
        return sorted(
            (directory_rows[idx] for idx in required_indices),
            key=lambda row: row["path"],
        )

    chunked_rows: List[List[dict]] = []
    for chunk in file_chunks:
        if not chunk:
            continue
        chunk.sort(key=lambda row: row["path"])
        chunk_directories = _collect_directory_rows(chunk)
        combined_rows = chunk_directories + chunk
        combined_rows.sort(key=lambda row: row["path"])
        chunked_rows.append(combined_rows)

    if not chunked_rows:
        if directory_rows:
            chunked_rows = [sorted(directory_rows, key=lambda row: row["path"])]
        else:
            log.warning("No entries captured — skipping export.")
            return []

    # XLS styling removed; only CSV outputs are produced by write_tabular_reports.

    report_result = write_tabular_reports(
        chunked_rows,
        base_name,
        FILE_SCAN_COLUMNS,
        output_dir=target_dir,
        dry_run=False,
    )

    for path in report_result.csv_paths:
        log.info(f"📄 CSV written to: {path}")

    return report_result.csv_paths


def cli(argv: Optional[Iterable[str]] = None) -> int:
    """Command-line entry point for the filesystem scanner."""

    import argparse

    parser = argparse.ArgumentParser(description="Scan a directory tree and emit CSV report files of entries.")
    parser.add_argument(
        "root",
        nargs="?",
        help="Root directory to scan (defaults to first root in config if omitted).",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        help="Directory where the reports should be written (defaults to cwd).",
    )
    parser.add_argument(
        "--base-name",
        "-b",
        default="file_scan",
        help="Base name for the generated files (timestamp is appended).",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable the progress bar during scanning.",
    )
    parser.add_argument(
        "--config",
        "-c",
        help="Path to YAML configuration (defaults to configs/config.yaml when root not provided).",
    )

    from common.shared.loader import load_media_types  # noqa: F401

    args = parser.parse_args(list(argv) if argv is not None else None)

    root_path: Optional[Path]
    output_path: Optional[Path]
    base_name = args.base_name

    logging_cfg: Optional[dict] = None

    batch_size_cfg: Optional[int] = None

    if args.root:
        root_path = Path(args.root).expanduser().resolve()
        output_path = Path(args.output_dir).expanduser() if args.output_dir else None
    else:
        candidates = []
        if args.config:
            candidates.append(Path(args.config).expanduser())
        module_config = Path(__file__).resolve().parents[1] / "configs" / "config.yaml"
        candidates.append(module_config)
        candidates.append(Path.cwd() / "configs" / "config.yaml")

        config_path = next((c for c in candidates if c.exists()), None)
        if config_path is None:
            raise SystemExit("Configuration file not found. Provide --config explicitly.")

        from common.shared.loader import load_task_config

        config = load_task_config("file_scan", config_path)
        roots = [Path(p).expanduser().resolve() for p in config.get("roots", [])]
        if not roots:
            raise SystemExit("file_scan config requires at least one root")
        root_path = roots[0]
        output_path = Path(args.output_dir).expanduser() if args.output_dir else (
            Path(config["output_dir"]).expanduser() if config.get("output_dir") else None
        )
        if base_name == parser.get_default("base_name"):
            base_name = config.get("base_name") or base_name
        logging_cfg = config.get("__logging__")
        batch_size_cfg = config.get("batch_size")
    # include_csv removed; CSV outputs are the default when writing is enabled

    if logging_cfg:
        from common.base.logging import setup_logging

        setup_logging(
            level=logging_cfg.get("level"),
            use_rich=logging_cfg.get("use_rich"),
            log_dir=logging_cfg.get("log_dir"),
            file_prefix=logging_cfg.get("file_prefix"),
        )

    workbook_paths = scan_filesystem(
        root_path,
        output_dir=output_path,
        base_name=base_name,
        include_progress=not args.no_progress,
        batch_size=batch_size_cfg,
    )

    for path in workbook_paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
