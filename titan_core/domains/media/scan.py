"""
Media scanning and file listing utilities.

Merged refactor from:
 - media/file-lister.py
 - media/mkv-scan.py
"""

import argparse
import json
import os
import subprocess
from pathlib import Path
from typing import List, Dict

from titan_core.cli.base import add_common_args
from titan_core.core.logging import setup_logging


# ----------------------------------------------------------------------
# Core reusable functions
# ----------------------------------------------------------------------

def list_videos(root: Path, extensions: tuple[str, ...] = (".mkv", ".mp4", ".avi", ".mov")) -> List[Path]:
    """Return a list of all video files recursively under the given root."""
    return [
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in extensions
    ]


def get_mkv_metadata(file_path: Path) -> Dict:
    """
    Return MKV metadata using mkvmerge (if available).
    Falls back to empty dict if the command fails.
    """
    try:
        cmd = ["mkvmerge", "-J", str(file_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
        return {}


# ----------------------------------------------------------------------
# CLI entry functions
# ----------------------------------------------------------------------

def vid_name_list():
    """CLI entrypoint: list all video files under a root directory."""
    parser = argparse.ArgumentParser(description="List all video files under a given root directory.")
    parser.add_argument("--root", required=True, help="Root directory to scan for video files.")
    add_common_args(parser)
    args = parser.parse_args()

    log = setup_logging(args.log_level)
    root = Path(args.root).expanduser().resolve()

    if not root.exists():
        log.error(f"Root path not found: {root}")
        return 1

    files = list_videos(root)
    if not files:
        log.warning(f"No video files found in {root}")
        return 0

    log.info(f"Found {len(files)} video files in {root}")
    for f in files:
        print(f)

    return 0


def vid_mkv_scan():
    """CLI entrypoint: scan MKV files and print track metadata."""
    parser = argparse.ArgumentParser(description="Scan MKV files for track metadata using mkvmerge.")
    parser.add_argument("--root", required=True, help="Root directory to scan for MKV files.")
    parser.add_argument("--json", action="store_true", help="Output results as JSON.")
    add_common_args(parser)
    args = parser.parse_args()

    log = setup_logging(args.log_level)
    root = Path(args.root).expanduser().resolve()

    if not root.exists():
        log.error(f"Root path not found: {root}")
        return 1

    files = [f for f in list_videos(root) if f.suffix.lower() == ".mkv"]
    results = []

    for f in files:
        meta = get_mkv_metadata(f)
        results.append({"file": str(f), "metadata": meta})
        log.debug(f"Scanned {f.name} ({'OK' if meta else 'no data'})")

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for item in results:
            print(f"\n📁 {item['file']}")
            if not item["metadata"]:
                print("   ⚠️  No metadata found.")
                continue

            for track in item["metadata"].get("tracks", []):
                print(f"   🎞️  Track ID {track.get('id')}: {track.get('codec') or track.get('codec_id')}")

    log.info(f"Completed scanning {len(files)} MKV files.")
    return 0
