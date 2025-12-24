"""
common.shared.report

Centralized reporting utilities for Titan Tools.

Enhancements:
 - Added export_report() helper for unified CSV output
 - Integrated dry-run simulation (skip writing files)
 - Improved logging and error safety
 - Backward-compatible with existing domain modules
"""

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union, Callable
from dataclasses import dataclass

# XLS reading/writing removed. Excel libraries are no longer required.

XFStyleType = Any

from common.base.file_io import open_file
from common.base.logging import get_logger
from common.base.fs import ensure_dir

log = get_logger(__name__)


@dataclass(frozen=True)
class ColumnSpec:
    key: str
    header: str
    width: Optional[int] = None  # width in characters


StyleResolver = Callable[[Dict[str, Any], int, int], Optional[XFStyleType]]


def _require_xlrd() -> None:  # kept for backwards compatibility but now raises
    raise ImportError("XLS support removed: only CSV is supported")


# ----------------------------------------------------------------------
# TIMESTAMPED FILENAMES
# ----------------------------------------------------------------------

def timestamped_filename(base_name: str, ext: str = "csv", output_dir: Optional[Path] = None) -> Path:
    """
    Generate a timestamped output filename (e.g., results_2025-10-06_1030.csv)
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    name = f"{base_name}_{ts}.{ext}"
    output_dir = ensure_dir(output_dir or Path.cwd())
    return output_dir / name


# ----------------------------------------------------------------------
# CSV WRITERS
# ----------------------------------------------------------------------

def write_csv(
    data: List[Dict[str, Any]],
    output_path: Path,
    fieldnames: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> Path:
    """
    Write structured data to a CSV file.
    Respects dry-run (simulates write if enabled).
    """
    if not data:
        log.warning("No data provided for CSV export.")
        return output_path

    if dry_run:
        log.info(f"[DRY-RUN] Would write CSV: {output_path}")
        return output_path

    ensure_dir(output_path.parent)
    try:
        with open_file(output_path, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(fieldnames or data[0].keys()))
            writer.writeheader()
            writer.writerows(data)
        log.debug(f"📊 CSV report saved → {output_path}")
        return output_path
    except Exception as e:
        log.error(f"Failed to write CSV report: {e}")
        raise


# ----------------------------------------------------------------------
# HUMAN-READABLE SUMMARY
# ----------------------------------------------------------------------

def summarize_counts(title: str, summary: Dict[str, int]) -> str:
    """
    Return a formatted, human-readable summary string.
    Example:
        summarize_counts("Scan Summary", {"Videos": 12, "Skipped": 3})
    """
    lines = [f"\n===== {title.upper()} ====="]
    for key, val in summary.items():
        lines.append(f"{key}: {val}")
    lines.append("=====================\n")
    return "\n".join(lines)


# ----------------------------------------------------------------------
# UNIFIED EXPORT WRAPPER
# ----------------------------------------------------------------------

def write_csv_batches(
    data: List[Dict[str, Any]],
    base_name: str,
    output_dir: Optional[Path] = None,
    batch_size: Optional[int] = None,
    fieldnames: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> List[Path]:
    """Write data to one or more CSV files depending on batch size."""

    if not data:
        return []

    output_dir = ensure_dir(output_dir or Path.cwd())
    base_path = timestamped_filename(base_name, "csv", output_dir)

    normalized_batch = batch_size if batch_size is not None else 0
    try:
        normalized_batch = int(normalized_batch)
    except (TypeError, ValueError):
        normalized_batch = 0

    if normalized_batch <= 0 or len(data) <= normalized_batch:
        return [write_csv(data, base_path, fieldnames=fieldnames, dry_run=dry_run)]

    stem = base_path.stem
    suffix = base_path.suffix
    paths: List[Path] = []
    for index, start in enumerate(range(0, len(data), normalized_batch), start=1):
        chunk = data[start : start + normalized_batch]
        chunk_path = base_path.with_name(f"{stem}_part{index:02d}{suffix}")
        paths.append(write_csv(chunk, chunk_path, fieldnames=fieldnames, dry_run=dry_run))

    return paths


def export_report(
    data: List[Dict[str, Any]],
    base_name: str,
    output_dir: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
    batch_size: Optional[int] = None,
) -> Dict[str, Union[Path, List[Path]]]:
    """
    Export report data to CSV files.

    Args:
        data: List of dicts (structured data)
        base_name: Base filename for reports (e.g. 'mkv_scan')
        output_dir: Directory for report storage
        write_csv_file: Whether to generate CSV output
        dry_run: If True, no actual file I/O will occur

    Returns:
        Dict of written file paths (or simulated paths if dry-run)
    """
    written: Dict[str, Union[Path, List[Path]]] = {}
    output_dir = ensure_dir(output_dir or Path.cwd())

    if not data:
        log.warning("No report data to export.")
        return written

    if write_csv_file:
        csv_paths = write_csv_batches(
            data,
            base_name,
            output_dir=output_dir,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        if csv_paths:
            written["csv"] = csv_paths if len(csv_paths) > 1 else csv_paths[0]

    log.info(f"Report export completed for '{base_name}' ({'dry-run' if dry_run else 'saved'})")
    return written


def write_chunked_csvs(
    chunked_rows: List[List[Dict[str, Any]]],
    base_name: str,
    output_dir: Optional[Path] = None,
    fieldnames: Optional[Sequence[str]] = None,
    dry_run: bool = False,
) -> List[Path]:
    """Write pre-chunked row groups to timestamped CSV files."""

    if not chunked_rows:
        return []

    output_dir = ensure_dir(output_dir or Path.cwd())
    base_path = timestamped_filename(base_name, "csv", output_dir)

    if len(chunked_rows) == 1:
        return [write_csv(chunked_rows[0], base_path, fieldnames=fieldnames, dry_run=dry_run)]

    stem = base_path.stem
    suffix = base_path.suffix
    written_paths: List[Path] = []
    for index, rows in enumerate(chunked_rows, start=1):
        chunk_path = base_path.with_name(f"{stem}_part{index:02d}{suffix}")
        written_paths.append(write_csv(rows, chunk_path, fieldnames=fieldnames, dry_run=dry_run))

    return written_paths


_PART_SUFFIX_RE = re.compile(r"_part(\d+)$", re.IGNORECASE)

_STYLE_CACHE: Dict[tuple[Optional[str], bool], XFStyleType] = {}
_HEADER_STYLE: Optional[XFStyleType] = None


def _get_excel_style(*_args: Any, **_kwargs: Any) -> XFStyleType:  # placeholder
    # Excel styling removed; return Any placeholder for compatibility
    return None


def _determine_column_width(spec: ColumnSpec) -> int:
    width_chars = spec.width if spec.width is not None else max(len(spec.header) + 2, 14)
    width_chars = max(8, min(width_chars, 120))
    return width_chars * 256



@dataclass
class TabularWriteResult:
    csv_paths: List[Path]

    def all_paths(self) -> List[Path]:
        return self.csv_paths


def write_tabular_reports(
    chunked_rows: List[List[Dict[str, Any]]],
    base_name: str,
    columns: Sequence[ColumnSpec],
    output_dir: Optional[Path] = None,
    *,
    dry_run: bool = False,
) -> TabularWriteResult:
    """Write chunked data to CSV files only. XLS output removed.

    CSV files are written directly under the provided `output_dir` (or the
    current working directory when unset). CSV files are no longer placed into
    a `csv/` subdirectory.
    Returns a TabularWriteResult containing csv_paths.
    """

    if not chunked_rows:
        return TabularWriteResult([])

    output_dir = ensure_dir(output_dir or Path.cwd())
    base_path = timestamped_filename(base_name, "csv", output_dir)
    stem = base_path.stem
    suffix = base_path.suffix
    multiple_chunks = len(chunked_rows) > 1
    csv_paths: List[Path] = []

    fieldnames = [spec.key for spec in columns]

    for index, rows in enumerate(chunked_rows, start=1):
        part_suffix = f"_part{index:02d}" if multiple_chunks else ""
        # Always write CSV files directly under the output directory.
        csv_dir = ensure_dir(base_path.parent)
        csv_path = csv_dir / f"{stem}{part_suffix}{suffix}"
        csv_ready_rows = [
            {key: row.get(key, "") for key in fieldnames}
            for row in rows
        ]
        csv_paths.append(write_csv(csv_ready_rows, csv_path, fieldnames=fieldnames, dry_run=dry_run))

    return TabularWriteResult(csv_paths=csv_paths)


def _normalize_cell_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, (int, bool)):
        return str(value)
    return str(value).strip()


def load_tabular_rows(path: Path) -> tuple[List[Dict[str, str]], List[str]]:
    """Load rows from a CSV file, returning rows plus header order.

    XLS/XLSX support removed: scripts should use CSV outputs only.
    """

    resolved = Path(path).expanduser()
    suffix = resolved.suffix.lower()

    if suffix != ".csv":
        raise ValueError(f"Only CSV tabular format is supported: received '{resolved.suffix}'")

    with open_file(resolved, "r", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [name or "" for name in (reader.fieldnames or [])]
        rows: List[Dict[str, str]] = []
        for raw in reader:
            row = {key: _normalize_cell_value(value) for key, value in raw.items()}
            rows.append(row)
        return rows, fieldnames


def discover_latest_csvs(
    base_dirs: Iterable[Path],
    base_name: str,
    part_numbers: Optional[Iterable[int]] = None,
    extensions: Optional[Sequence[str]] = None,
) -> List[Path]:
    """Discover latest report exports for a given base name and optional part filters."""

    normalized_exts = tuple(
        ext.lower().lstrip(".") for ext in (extensions if extensions else ("csv",))
    )

    candidates: List[tuple[int, float, int, Path]] = []

    def _iter_search_dirs(base_dir: Path, ext: str) -> Iterable[Path]:
        if not base_dir.exists():
            return []
        dirs = [base_dir]
        if ext == "csv":
            csv_dir = base_dir / "csv"
            if csv_dir.exists():
                dirs.append(csv_dir)
        for directory in dirs:
            yield from directory.glob(f"{base_name}_*.{ext}")

    for ext_index, ext in enumerate(normalized_exts):
        for base_dir in base_dirs:
            try:
                directory = Path(base_dir).expanduser()
            except Exception:
                continue
            for candidate in sorted(_iter_search_dirs(directory, ext)):
                try:
                    mtime = candidate.stat().st_mtime
                except FileNotFoundError:
                    continue
                stem = candidate.stem
                match = _PART_SUFFIX_RE.search(stem)
                part = int(match.group(1)) if match else 0
                candidates.append((ext_index, mtime, part, candidate.resolve()))

    if not candidates:
        return []

    if not part_numbers:
        _, _, _, latest_path = max(candidates, key=lambda item: (item[1], -item[0]))
        return [latest_path]

    results: List[Path] = []
    for part in part_numbers:
        matching = [item for item in candidates if item[2] == part]
        if not matching:
            raise FileNotFoundError(
                f"No exports found for base '{base_name}' with part {part:02d}"
            )
        _, _, _, latest = max(matching, key=lambda item: (item[1], -item[0]))
        results.append(latest)

    return results


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    log.info("✅ common.shared.report self-test:")
    test_data = [
        {"file": "movie1.mkv", "codec": "hevc", "status": "OK"},
        {"file": "movie2.mp4", "codec": "h264", "status": "Non-HEVC"},
    ]

    # Normal export
    export_report(test_data, "test_report", write_csv_file=True)

    # Dry-run export
    export_report(test_data, "dryrun_report", write_csv_file=True, dry_run=True)
