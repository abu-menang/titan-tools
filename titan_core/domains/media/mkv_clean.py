"""
titan_core.domains.media.mkv_clean

Refactored MKV cleaning logic for Titan Tools.
Replaces legacy `media/mkv-cleaner.py`.

Features:
 - Scans MKV metadata using mkvmerge
 - Compares to previous scan results (optional)
 - Cleans or remuxes MKV files safely
 - Skips unchanged files
 - Supports dry-run, logging, and report export
"""

from __future__ import annotations
import json
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any

from titan_core.core.logging import get_logger
from titan_core.core.ops import run_command, move_to_trash, file_info
from titan_core.core.utils import ensure_dir, Progress
from titan_core.core.report import write_json, timestamped_filename

log = get_logger(__name__)


# ----------------------------------------------------------------------
# METADATA UTILITIES
# ----------------------------------------------------------------------

def get_mkv_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract metadata from an MKV file using mkvmerge.
    Returns an empty dict if mkvmerge is unavailable or fails.
    """
    cmd = ["mkvmerge", "-J", str(file_path)]
    code, out, err = run_command(cmd, capture=True)
    if code != 0 or not out:
        log.warning(f"Failed to read metadata for {file_path.name}: {err}")
        return {}
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.error(f"Invalid JSON from mkvmerge for {file_path}")
        return {}


def compare_metadata(current: Dict[str, Any], reference: Optional[Dict[str, Any]]) -> bool:
    """
    Compare two metadata dictionaries to determine if file changed.
    Returns True if update is needed.
    """
    if not reference:
        return True
    # Compare essential properties only
    return current.get("tracks") != reference.get("tracks")


# ----------------------------------------------------------------------
# CLEANING WORKFLOW
# ----------------------------------------------------------------------

def clean_mkv_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    Perform cleaning/remux of a single MKV file.
    Returns True if file was cleaned, False if skipped or failed.
    """
    log.info(f"🎬 Cleaning: {file_path.name}")

    tmp_output = file_path.with_name(f"cleaned-{file_path.name}")
    cmd = ["mkvmerge", "-o", str(tmp_output), str(file_path)]

    if dry_run:
        log.info(f"[DRY-RUN] Would execute: {' '.join(cmd)}")
        return True

    code, _, err = run_command(cmd)
    if code != 0:
        log.error(f"Failed to clean {file_path.name}: {err}")
        if tmp_output.exists():
            tmp_output.unlink(missing_ok=True)
        return False

    # Replace original safely
    try:
        backup_path = file_path.with_suffix(".bak")
        shutil.move(file_path, backup_path)
        shutil.move(tmp_output, file_path)
        move_to_trash(backup_path)
        log.info(f"✅ Cleaned and replaced: {file_path.name}")
        return True
    except Exception as e:
        log.error(f"Error replacing file {file_path.name}: {e}")
        return False


# ----------------------------------------------------------------------
# MAIN PROCESSOR
# ----------------------------------------------------------------------

def clean_directory(root: Path, dry_run: bool = False, reference_json: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    Iterate through all MKV files in directory and clean as needed.

    Args:
        root: Root directory
        dry_run: Only simulate cleaning
        reference_json: Optional previous scan JSON to detect changes

    Returns:
        List of dict results for report
    """
    results: List[Dict[str, Any]] = []
    mkv_files = list(root.rglob("*.mkv"))
    log.info(f"Found {len(mkv_files)} MKV files in {root}")

    ref_data = {}
    if reference_json and reference_json.exists():
        try:
            ref_data = {item["file"]: item for item in json.loads(reference_json.read_text())}
            log.info(f"Loaded reference metadata from {reference_json}")
        except Exception as e:
            log.warning(f"Failed to load reference JSON: {e}")

    for f in Progress(mkv_files, desc="Cleaning MKVs"):
        meta = get_mkv_metadata(f)
        changed = compare_metadata(meta, ref_data.get(str(f))) if ref_data else True
        if not changed:
            log.info(f"⏩ Skipping unchanged: {f.name}")
            results.append({"file": str(f), "status": "Skipped", "changed": False})
            continue

        success = clean_mkv_file(f, dry_run=dry_run)
        results.append({
            "file": str(f),
            "status": "Cleaned" if success else "Failed",
            "changed": True,
            "size": file_info(f)["size_human"] if success else "N/A",
        })

    return results


# ----------------------------------------------------------------------
# CLI ENTRYPOINT
# ----------------------------------------------------------------------

def vid_mkv_clean():
    """
    CLI command: Clean or remux MKV files safely.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Clean or remux MKV files safely.")
    parser.add_argument("--root", required=True, help="Root directory to scan for MKV files.")
    parser.add_argument("--ref-json", help="Optional reference metadata JSON to compare against.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate cleaning without changes.")
    parser.add_argument("--json-report", action="store_true", help="Write output to JSON report.")
    parser.add_argument("--log-level", default="INFO", help="Set logging verbosity.")
    args = parser.parse_args()

    log = get_logger("titan_clean")
    log.setLevel(args.log_level.upper())

    root = Path(args.root).expanduser().resolve()
    results = clean_directory(root, dry_run=args.dry_run, reference_json=Path(args.ref_json) if args.ref_json else None)

    log.info("Cleaning completed.")
    summary = {
        "Cleaned": sum(1 for r in results if r["status"] == "Cleaned"),
        "Skipped": sum(1 for r in results if r["status"] == "Skipped"),
        "Failed": sum(1 for r in results if r["status"] == "Failed"),
    }
    log.info(f"Summary: {summary}")

    if args.json_report:
        out_path = write_json(results, timestamped_filename("mkv_clean", "json"))
        log.info(f"Results written to: {out_path}")

    return 0


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Example usage: python -m titan_core.domains.media.mkv_clean --root ./movies --dry-run
    vid_mkv_clean()
