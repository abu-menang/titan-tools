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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.base.file_io import open_file
from common.base.logging import get_logger
from common.base.fs import ensure_dir

log = get_logger(__name__)


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

def write_csv(data: List[Dict[str, Any]], output_path: Path, dry_run: bool = False) -> Path:
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
            writer = csv.DictWriter(handle, fieldnames=list(data[0].keys()))
            writer.writeheader()
            writer.writerows(data)
        log.info(f"📊 CSV report saved → {output_path}")
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

def export_report(
    data: List[Dict[str, Any]],
    base_name: str,
    output_dir: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
) -> Dict[str, Path]:
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
    written: Dict[str, Path] = {}
    output_dir = ensure_dir(output_dir or Path.cwd())

    if not data:
        log.warning("No report data to export.")
        return written

    if write_csv_file:
        csv_path = timestamped_filename(base_name, "csv", output_dir)
        written["csv"] = write_csv(data, csv_path, dry_run=dry_run)

    log.info(f"Report export completed for '{base_name}' ({'dry-run' if dry_run else 'saved'})")
    return written


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
