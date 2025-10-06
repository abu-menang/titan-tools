"""
titan_core.domains.media.non_hevc

Scan media libraries for non-HEVC (non-H.265) encoded videos.

Features:
 - Detects video codec using ffprobe
 - Reports non-HEVC videos (H.264, VP9, AV1, etc.)
 - Supports JSON/CSV reporting
 - Optional move or delete for non-HEVC files
 - Fully dry-run compatible
"""

from __future__ import annotations
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from titan_core.core.logging import get_logger
from titan_core.core.utils import ensure_dir, Progress, move_file
from titan_core.core.ops import run_command, move_to_trash
from titan_core.core.report import write_json, write_csv, timestamped_filename, summarize_counts

log = get_logger(__name__)


# ----------------------------------------------------------------------
# CODEC DETECTION
# ----------------------------------------------------------------------

def get_video_codec(file_path: Path) -> Optional[str]:
    """
    Get the video codec of a file using ffprobe.
    Returns None if ffprobe fails or codec is not found.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "json", str(file_path),
    ]
    code, out, err = run_command(cmd, capture=True)
    if code != 0 or not out:
        log.warning(f"ffprobe failed for {file_path.name}: {err}")
        return None

    try:
        data = json.loads(out)
        streams = data.get("streams", [])
        if not streams:
            return None
        return streams[0].get("codec_name")
    except Exception as e:
        log.error(f"Failed to parse ffprobe output for {file_path.name}: {e}")
        return None


def is_hevc_codec(codec: Optional[str]) -> bool:
    """
    Return True if codec is HEVC (h265 / hevc / H.265).
    """
    if not codec:
        return False
    return codec.lower() in {"hevc", "h265", "h.265"}


# ----------------------------------------------------------------------
# MAIN WORKFLOW
# ----------------------------------------------------------------------

def scan_non_hevc(
    root: Path,
    move_dir: Optional[Path] = None,
    delete: bool = False,
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    """
    Scan a directory for non-HEVC videos and optionally move/delete them.
    """
    results: List[Dict[str, str]] = []
    video_exts = (".mkv", ".mp4", ".avi", ".mov", ".m4v")

    files = [f for f in root.rglob("*") if f.suffix.lower() in video_exts]
    log.info(f"Scanning {len(files)} video files in {root}")

    for f in Progress(files, desc="Checking codecs"):
        codec = get_video_codec(f)
        if is_hevc_codec(codec):
            results.append({"file": str(f), "codec": codec or "unknown", "status": "HEVC"})
            continue

        log.warning(f"❌ Non-HEVC detected: {f.name} ({codec})")

        action_status = "Detected"

        if move_dir and not dry_run:
            try:
                ensure_dir(move_dir)
                move_file(f, move_dir / f.name, overwrite=True)
                action_status = "Moved"
            except Exception as e:
                log.error(f"Failed to move {f.name}: {e}")
                action_status = "FailedMove"

        elif delete and not dry_run:
            try:
                move_to_trash(f)
                action_status = "Deleted"
            except Exception as e:
                log.error(f"Failed to delete {f.name}: {e}")
                action_status = "FailedDelete"

        elif dry_run:
            action_status = "WouldMove" if move_dir else "WouldDelete" if delete else "Detected"

        results.append({
            "file": str(f),
            "codec": codec or "unknown",
            "status": action_status,
        })

    return results


# ----------------------------------------------------------------------
# CLI ENTRYPOINT
# ----------------------------------------------------------------------

def vid_non_hevc():
    """
    CLI command: Detect and handle non-HEVC videos.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Scan for non-HEVC video files.")
    parser.add_argument("--root", required=True, help="Root directory to scan.")
    parser.add_argument("--move-dir", help="Move non-HEVC files here.")
    parser.add_argument("--delete", action="store_true", help="Delete non-HEVC files (send to trash).")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions without changes.")
    parser.add_argument("--json-report", action="store_true", help="Write JSON report.")
    parser.add_argument("--csv-report", action="store_true", help="Write CSV report.")
    parser.add_argument("--log-level", default="INFO", help="Logging verbosity.")
    args = parser.parse_args()

    log = get_logger("titan_non_hevc")
    log.setLevel(args.log_level.upper())

    root = Path(args.root).expanduser().resolve()
    move_dir = Path(args.move_dir).expanduser().resolve() if args.move_dir else None

    results = scan_non_hevc(root, move_dir=move_dir, delete=args.delete, dry_run=args.dry_run)

    summary = {
        "HEVC": sum(1 for r in results if r["status"] == "HEVC"),
        "NonHEVC": sum(1 for r in results if r["status"] in ("Detected", "Moved", "Deleted")),
        "Moved": sum(1 for r in results if r["status"] == "Moved"),
        "Deleted": sum(1 for r in results if r["status"] == "Deleted"),
    }
    log.info(summarize_counts("Non-HEVC Summary", summary))

    if args.json_report:
        write_json(results, timestamped_filename("non_hevc", "json"))
    if args.csv_report:
        write_csv(results, timestamped_filename("non_hevc", "csv"))

    return 0


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Example: python -m titan_core.domains.media.non_hevc --root ./videos --dry-run
    vid_non_hevc()
