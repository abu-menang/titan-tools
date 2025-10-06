"""
titan_core.domains.media.rename

Batch file renaming utility for Titan Tools.
Refactored from legacy `media/file-renamer.py`.

Features:
 - Batch rename files safely (with dry-run option)
 - Optional JSON/CSV reporting
 - Name sanitization and extension preservation
 - Automatic skip on duplicate or invalid rename
 - Integrates with Titan core logging, ops, and report modules
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional

from titan_core.core.logging import get_logger
from titan_core.core.utils import safe_filename, Progress, move_file
from titan_core.core.ops import move_to_trash
from titan_core.core.report import write_json, write_csv, timestamped_filename, summarize_counts



log = get_logger(__name__)


# ----------------------------------------------------------------------
# RENAME CORE LOGIC
# ----------------------------------------------------------------------

def rename_file(src: Path, new_name: str, dry_run: bool = False) -> bool:
    """
    Rename a file safely. Moves to trash on conflict.
    Returns True if renamed successfully.
    """
    dest = src.with_name(safe_filename(new_name) + src.suffix)

    if dest == src:
        log.debug(f"⏩ No rename needed: {src.name}")
        return False

    if dest.exists():
        log.warning(f"⚠️ Target already exists, moving old one to trash: {dest.name}")
        if not dry_run:
            move_to_trash(dest)

    if dry_run:
        log.info(f"[DRY-RUN] Would rename {src.name} → {dest.name}")
        return True

    try:
        move_file(src, dest, overwrite=True)
        log.info(f"✅ Renamed {src.name} → {dest.name}")
        return True
    except Exception as e:
        log.error(f"❌ Failed to rename {src.name}: {e}")
        return False


def rename_directory(
    root: Path,
    rename_map: Dict[str, str],
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    """
    Apply batch rename operations under a root directory.
    Returns list of rename results for reporting.
    """
    results: List[Dict[str, str]] = []

    for src_name, new_name in Progress(rename_map.items(), desc="Renaming files"):
        src = root / src_name
        if not src.exists():
            log.warning(f"🚫 Source not found: {src}")
            results.append({"source": str(src), "new_name": new_name, "status": "Missing"})
            continue

        success = rename_file(src, new_name, dry_run=dry_run)
        results.append({
            "source": str(src),
            "new_name": new_name,
            "status": "Renamed" if success else "Skipped",
        })

    return results


# ----------------------------------------------------------------------
# NAME GENERATION UTILITIES
# ----------------------------------------------------------------------

def auto_generate_names(root: Path, prefix: str = "vid", zero_pad: int = 3) -> Dict[str, str]:
    """
    Auto-generate sequential rename mapping for all video files.
    Example:  vid001.mkv, vid002.mkv, etc.
    """
    exts = (".mkv", ".mp4", ".avi", ".mov")
    files = sorted([f.name for f in root.iterdir() if f.suffix.lower() in exts])

    mapping: Dict[str, str] = {}
    for i, name in enumerate(files, 1):
        new_base = f"{prefix}{str(i).zfill(zero_pad)}"
        mapping[name] = new_base
    return mapping


# ----------------------------------------------------------------------
# CLI ENTRYPOINT
# ----------------------------------------------------------------------

def vid_rename():
    """
    CLI command: Batch rename media files safely.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Batch rename media files safely.")
    parser.add_argument("--root", required=True, help="Root directory containing files to rename.")
    parser.add_argument("--map-json", help="Optional JSON file containing rename mapping.")
    parser.add_argument("--prefix", default="vid", help="Prefix for auto-generated names (default: vid).")
    parser.add_argument("--zero-pad", type=int, default=3, help="Zero-padding for sequence numbers (default: 3).")
    parser.add_argument("--dry-run", action="store_true", help="Simulate rename without making changes.")
    parser.add_argument("--json-report", action="store_true", help="Write output JSON report.")
    parser.add_argument("--csv-report", action="store_true", help="Write output CSV report.")
    parser.add_argument("--log-level", default="INFO", help="Logging verbosity level.")
    args = parser.parse_args()

    log = get_logger("titan_rename")
    log.setLevel(args.log_level.upper())
    root = Path(args.root).expanduser().resolve()

    # Load or generate rename mapping
    if args.map_json:
        import json
        mapping = json.loads(Path(args.map_json).read_text())
        log.info(f"Loaded rename map from {args.map_json}")
    else:
        mapping = auto_generate_names(root, prefix=args.prefix, zero_pad=args.zero_pad)
        log.info(f"Auto-generated rename mapping for {len(mapping)} files.")

    results = rename_directory(root, mapping, dry_run=args.dry_run)

    # Summary and reporting
    summary = {
        "Renamed": sum(1 for r in results if r["status"] == "Renamed"),
        "Skipped": sum(1 for r in results if r["status"] == "Skipped"),
        "Missing": sum(1 for r in results if r["status"] == "Missing"),
    }
    log.info(summarize_counts("Rename Summary", summary))

    if args.json_report:
        write_json(results, timestamped_filename("vid_rename", "json"))
    if args.csv_report:
        write_csv(results, timestamped_filename("vid_rename", "csv"))

    return 0


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Example: python -m titan_core.domains.media.rename --root ./videos --dry-run
    vid_rename()
