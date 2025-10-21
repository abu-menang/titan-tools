"""
video.metadata

Unified metadata management utilities for Titan Tools.

Features:
 - Read video metadata (duration, resolution, codec, etc.)
 - Extract MKV tags and track info using mkvmerge or ffprobe
 - Update or inject metadata tags (title, language, etc.)
 - Export metadata to JSON or CSV for indexing
"""

from __future__ import annotations
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from common.base.logging import get_logger
from common.base.ops import run_command
from common.base.fs import ensure_dir
from common.shared.report import write_csv, timestamped_filename

log = get_logger(__name__)


# ----------------------------------------------------------------------
# METADATA READERS
# ----------------------------------------------------------------------

def get_ffprobe_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Get general media metadata via ffprobe.
    Returns a parsed JSON dict or {} on failure.
    """
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration,size,bit_rate:stream=codec_name,width,height,codec_type",
        "-of", "json", str(file_path),
    ]
    code, out, err = run_command(cmd, capture=True)
    if code != 0 or not out:
        log.warning(f"ffprobe failed for {file_path.name}: {err}")
        return {}

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.error(f"Invalid JSON from ffprobe for {file_path.name}")
        return {}


def get_mkvmerge_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Get detailed track metadata for MKV files via mkvmerge -J.
    Returns {} if mkvmerge is unavailable or parsing fails.
    """
    cmd = ["mkvmerge", "-J", str(file_path)]
    code, out, err = run_command(cmd, capture=True)
    if code != 0 or not out:
        log.debug(f"mkvmerge metadata read failed for {file_path.name}: {err}")
        return {}

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.error(f"Invalid JSON from mkvmerge for {file_path}")
        return {}


# ----------------------------------------------------------------------
# TAG WRITERS
# ----------------------------------------------------------------------

def set_mkv_title(file_path: Path, title: str) -> bool:
    """
    Set the MKV title using mkvpropedit.
    """
    cmd = ["mkvpropedit", str(file_path), "--edit", "info", "--set", f"title={title}"]
    code, _, err = run_command(cmd, capture=True)
    if code != 0:
        log.error(f"Failed to set title for {file_path.name}: {err}")
        return False
    log.info(f"✅ Updated title: {file_path.name} → {title}")
    return True


def set_track_language(file_path: Path, track_id: int, lang_code: str) -> bool:
    """
    Set language code for a specific MKV track.
    Example: track_id=1, lang_code="eng"
    """
    cmd = [
        "mkvpropedit", str(file_path),
        "--edit", f"track:{track_id}", "--set", f"language={lang_code}",
    ]
    code, _, err = run_command(cmd, capture=True)
    if code != 0:
        log.error(f"Failed to set language for {file_path.name} (track {track_id}): {err}")
        return False
    log.info(f"🗣️ Updated track {track_id} language to {lang_code} in {file_path.name}")
    return True


# ----------------------------------------------------------------------
# EXPORT / INDEXING
# ----------------------------------------------------------------------

def export_metadata(
    root: Path,
    output_dir: Optional[Path] = None,
    use_mkvmerge: bool = False,
) -> Path:
    """
    Export metadata for all video files in directory as JSON report.
    """
    results: List[Dict[str, Any]] = []
    video_exts = (".mkv", ".mp4", ".avi", ".mov")

    for f in root.rglob("*"):
        if f.suffix.lower() not in video_exts:
            continue

        log.info(f"Extracting metadata for {f.name}")
        data = (
            get_mkvmerge_metadata(f)
            if use_mkvmerge and f.suffix.lower() == ".mkv"
            else get_ffprobe_metadata(f)
        )

        if not data:
            results.append({"file": str(f), "status": "Failed"})
            continue

        # Flatten key metrics
        info = {
            "file": str(f),
            "size": data.get("format", {}).get("size"),
            "duration": data.get("format", {}).get("duration"),
            "bitrate": data.get("format", {}).get("bit_rate"),
            "video_codec": None,
            "audio_codec": None,
            "status": "OK",
        }

        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video" and not info["video_codec"]:
                info["video_codec"] = stream.get("codec_name")
            if stream.get("codec_type") == "audio" and not info["audio_codec"]:
                info["audio_codec"] = stream.get("codec_name")

        results.append(info)

    ensure_dir(output_dir or Path.cwd())
    csv_path = write_csv(results, timestamped_filename("metadata", "csv"))
    log.info(f"Metadata exported to: {csv_path}")
    return csv_path


# ----------------------------------------------------------------------
# CLI ENTRYPOINT
# ----------------------------------------------------------------------

def vid_metadata():
    """
    CLI command: Extract and export video metadata.
    """
    import argparse
    parser = argparse.ArgumentParser(description="Extract and export video metadata.")
    parser.add_argument("--root", required=True, help="Root directory to scan.")
    parser.add_argument("--use-mkvmerge", action="store_true", help="Use mkvmerge for MKV metadata.")
    parser.add_argument("--log-level", default="INFO", help="Logging verbosity.")
    args = parser.parse_args()

    log = get_logger("titan_metadata")
    log.setLevel(args.log_level.upper())

    root = Path(args.root).expanduser().resolve()
    export_metadata(root, use_mkvmerge=args.use_mkvmerge)


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Example:
    # python -m video.metadata --root ./movies --use-mkvmerge
    vid_metadata()
