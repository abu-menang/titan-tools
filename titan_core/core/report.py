"""
titan_core.core.report

Centralized reporting utilities for Titan Tools.

Enhancements:
 - Added export_report() helper for unified JSON/CSV output
 - Integrated dry-run simulation (skip writing files)
 - Improved logging and error safety
 - Backward-compatible with existing domain modules
"""

from __future__ import annotations
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from titan_core.core.utils import ensure_dir
from titan_core.core.logging import get_logger

log = get_logger(__name__)


# ----------------------------------------------------------------------
# TIMESTAMPED FILENAMES
# ----------------------------------------------------------------------

def timestamped_filename(base_name: str, ext: str = "json", output_dir: Optional[Path] = None) -> Path:
    """
    Generate a timestamped output filename (e.g., results_2025-10-06_1030.json)
    """
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    name = f"{base_name}_{ts}.{ext}"
    output_dir = ensure_dir(output_dir or Path.cwd())
    return output_dir / name


# ----------------------------------------------------------------------
# JSON + CSV WRITERS
# ----------------------------------------------------------------------

def write_json(data: Any, output_path: Path, indent: int = 2, dry_run: bool = False) -> Path:
    """
    Write structured data to a JSON file.
    Respects dry-run (will only simulate write if enabled).
    """
    if dry_run:
        log.info(f"[DRY-RUN] Would write JSON: {output_path}")
        return output_path

    ensure_dir(output_path.parent)
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        log.info(f"📝 JSON report saved → {output_path}")
        return output_path
    except Exception as e:
        log.error(f"Failed to write JSON report: {e}")
        raise


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
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
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
    write_json_file: bool = True,
    write_csv_file: bool = False,
    dry_run: bool = False,
) -> Dict[str, Path]:
    """
    Export report data to JSON and/or CSV files.

    Args:
        data: List of dicts (structured data)
        base_name: Base filename for reports (e.g. 'mkv_scan')
        output_dir: Directory for report storage
        write_json_file: Whether to generate JSON output
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

    if write_json_file:
        json_path = timestamped_filename(base_name, "json", output_dir)
        written["json"] = write_json(data, json_path, dry_run=dry_run)

    if write_csv_file:
        csv_path = timestamped_filename(base_name, "csv", output_dir)
        written["csv"] = write_csv(data, csv_path, dry_run=dry_run)

    log.info(f"Report export completed for '{base_name}' ({'dry-run' if dry_run else 'saved'})")
    return written


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    log.info("✅ titan_core.core.report self-test:")
    test_data = [
        {"file": "movie1.mkv", "codec": "hevc", "status": "OK"},
        {"file": "movie2.mp4", "codec": "h264", "status": "Non-HEVC"},
    ]

    # Normal export
    export_report(test_data, "test_report", write_json_file=True, write_csv_file=True)

    # Dry-run export
    export_report(test_data, "dryrun_report", write_json_file=True, write_csv_file=True, dry_run=True)
