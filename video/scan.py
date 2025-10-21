"""
video.scan

Media scanning utilities.

Implements:
 - vid_mkv_scan(): capture detailed mkvmerge track metadata and auxiliary reports.
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from common.base.logging import get_logger
from common.base.fs import ensure_dir, human_size
from common.base.ops import run_command
from common.shared.loader import load_media_types
from common.shared.report import export_report, write_csv, timestamped_filename
from common.shared.utils import Progress

log = get_logger(__name__)

MEDIA_TYPES = load_media_types()

# Include all formats you care about for *listing* videos.
VIDEO_EXTS: set[str] = set(MEDIA_TYPES.video_exts)

# For MKV scan, we only act on MKV files (mkvmerge-best path).
MKV_EXTS: set[str] = {".mkv"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _path_is_relative_to(path: Path, ancestor: Path) -> bool:
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def _iter_files(
    roots: Iterable[Path],
    exts: set[str],
    exclude_hidden: bool = True,
    exclude_dir: Optional[Path] = None,
) -> Iterable[Path]:
    """Yield files under given roots that match allowed extensions."""
    for root in roots:
        root = root.resolve()
        if not root.exists():
            log.warning(f"Path does not exist: {root}")
            continue

        if root.is_file():
            if root.suffix.lower() in exts and (not exclude_hidden or not root.name.startswith(".")):
                yield root
            continue

        for dirpath, dirnames, filenames in os.walk(root):
            if exclude_hidden:
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                filenames = [f for f in filenames if not f.startswith(".")]
            if exclude_dir:
                resolved_exclude = exclude_dir.resolve()
                filtered_dirs = []
                for d in dirnames:
                    candidate = Path(dirpath) / d
                    try:
                        resolved_candidate = candidate.resolve()
                    except FileNotFoundError:
                        resolved_candidate = candidate
                    if not _path_is_relative_to(resolved_candidate, resolved_exclude):
                        filtered_dirs.append(d)
                dirnames[:] = filtered_dirs

                filtered_files = []
                for f in filenames:
                    candidate = Path(dirpath) / f
                    try:
                        resolved_candidate = candidate.resolve()
                    except FileNotFoundError:
                        resolved_candidate = candidate
                    if not _path_is_relative_to(resolved_candidate, resolved_exclude):
                        filtered_files.append(f)
                filenames = filtered_files
            for fname in filenames:
                p = Path(dirpath) / fname
                if p.suffix.lower() in exts:
                    yield p


def _extract_track_rows(file_path: Path, mkvmerge_json: dict, file_size: int) -> List[Dict[str, object]]:
    """
    Flatten mkvmerge JSON into per-track rows similar to the legacy mkv-scan utility.
    """
    tracks = mkvmerge_json.get("tracks") or []
    base_name = file_path.stem

    rows: List[Dict[str, object]] = []
    for track in tracks:
        track_type = track.get("type") or ""
        props = track.get("properties") or {}
        codec = track.get("codec") or props.get("codec_id") or ""
        lang = props.get("language") or "und"
        track_name = props.get("track_name") or ""
        default = "true" if props.get("default_track") else "false"
        forced = "true" if props.get("forced_track") else "false"
        track_id = track.get("id")

        suggested = ""
        if track_type == "video":
            suggested = base_name
        elif track_type in {"audio", "subtitles"}:
            lang_token = (lang or "und").strip().upper()
            lang_token = lang_token[:3] if len(lang_token) > 3 else lang_token
            codec_token = (codec or "").strip().upper()
            if lang_token and codec_token:
                suggested = f"{lang_token} ({codec_token})"
            elif lang_token:
                suggested = lang_token
            elif codec_token:
                suggested = codec_token

        width = props.get("width")
        height = props.get("height")
        if (width is None or height is None) and props.get("pixel_dimensions"):
            try:
                w_val, h_val = str(props["pixel_dimensions"]).lower().split("x")
                width = width or int(w_val)
                height = height or int(h_val)
            except Exception:
                width = width or None
                height = height or None

        frame_rate = props.get("nominal_frame_rate") or props.get("frame_rate")

        row: Dict[str, object] = {
            "file": str(file_path),
            "filename": file_path.name,
            "type": track_type,
            "id": track_id,
            "codec": codec,
            "lang": lang,
            "name": track_name,
            "default": default,
            "forced": forced,
            "suggested_rename": suggested,
            "file_bytes": file_size,
            "file_size": human_size(file_size),
        }

        if track_type == "video":
            row["width"] = width or ""
            row["height"] = height or ""
            row["frame_rate"] = frame_rate or ""
        elif track_type == "audio":
            row["channels"] = props.get("audio_channels") or ""
            row["sample_rate"] = props.get("audio_sampling_frequency") or ""
            row["bit_depth"] = props.get("audio_sample_depth") or ""
        elif track_type == "subtitles":
            row["encoding"] = props.get("encoding") or props.get("codec_private_data") or ""

        # Ensure consistent CSV fieldnames across track types.
        row.setdefault("width", "")
        row.setdefault("height", "")
        row.setdefault("frame_rate", "")
        row.setdefault("channels", "")
        row.setdefault("sample_rate", "")
        row.setdefault("bit_depth", "")
        row.setdefault("encoding", "")

        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def vid_mkv_scan(
    roots: Optional[Iterable[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
) -> List[Dict[str, object]]:
    """
    Probe MKV files and collect detailed per-track metadata similar to the legacy mkv-scan utility.

    Args:
        roots: One or more starting paths. Defaults to [Path.cwd()].
        output_dir: Directory for report storage (takes precedence over output_root).
        output_root: Fallback directory sourced from task defaults when output_dir is unset.
        write_csv_file: Toggle CSV export.
        dry_run: If True, skip writing files (probing still runs).

    Returns:
        A list of per-track rows describing codec, language, and flags.
    """
    candidate_roots = [Path(p).expanduser() for p in (roots or [Path.cwd()])]
    resolved_roots = [p.resolve() for p in candidate_roots]
    log.info(f"🎞️ Scanning MKVs under: {', '.join(str(r) for r in resolved_roots)}")

    primary_root = resolved_roots[0] if resolved_roots else Path.cwd()
    base_output_dir = (
        Path(output_dir)
        if output_dir
        else Path(output_root)
        if output_root
        else primary_root
    )
    if not dry_run:
        ensure_dir(base_output_dir)
    log.info(f"📁 Report directory: {base_output_dir}")

    start = time.perf_counter()
    track_rows: List[Dict[str, object]] = []
    name_list_rows: List[Dict[str, str]] = []
    seen_directories: Set[Path] = set()

    try:
        resolved_output_dir = base_output_dir.resolve()
    except FileNotFoundError:
        resolved_output_dir = base_output_dir

    root_set = {r.resolve() for r in resolved_roots}

    def register_directory(directory: Path) -> None:
        try:
            resolved = directory.expanduser().resolve()
        except FileNotFoundError:
            resolved = directory.expanduser()
        if resolved in root_set or directory in root_set:
            return
        if resolved == resolved_output_dir:
            return
        if resolved in seen_directories:
            return
        seen_directories.add(resolved)
        name_list_rows.append(_build_directory_row(resolved))

    for root in resolved_roots:
        register_directory(root)
    failed_files: List[Dict[str, str]] = []
    non_hevc_rows: List[Dict[str, object]] = []
    scanned_files = 0

    for mkv in Progress(
        _iter_files(resolved_roots, MKV_EXTS, exclude_dir=base_output_dir),
        desc="Probing MKV",
    ):
        scanned_files += 1
        try:
            stat_result = mkv.stat()
            size = stat_result.st_size
        except FileNotFoundError:
            failed_files.append({"file": str(mkv), "reason": "file missing during scan"})
            continue

        current_dir = mkv.parent
        while True:
            register_directory(current_dir)
            try:
                resolved_current = current_dir.resolve()
            except FileNotFoundError:
                resolved_current = current_dir
            if resolved_current in root_set or current_dir.parent == current_dir:
                break
            current_dir = current_dir.parent
        name_list_rows.append(_build_name_list_row(mkv))

        code, out, err = run_command(["mkvmerge", "-J", str(mkv)], capture=True, stream=False)
        if code != 0 or not out:
            reason = (err or "").strip() or "mkvmerge returned no output"
            failed_files.append({"file": str(mkv), "reason": reason})
            log.error(f"❌ mkvmerge failed for {mkv.name}: {reason}")
            continue

        try:
            mkvmerge_json = json.loads(out)
        except json.JSONDecodeError:
            failed_files.append({"file": str(mkv), "reason": "invalid JSON from mkvmerge"})
            log.error(f"❌ Invalid JSON output from mkvmerge for {mkv.name}")
            continue

        rows = _extract_track_rows(mkv, mkvmerge_json, size)
        if not rows:
            failed_files.append({"file": str(mkv), "reason": "no track data"})
            log.warning(f"⚠️ No track data recorded for {mkv}")
            continue

        track_rows.extend(rows)

        raw_codecs = {
            (row.get("codec") or "").strip()
            for row in rows
            if (row.get("type") or "").lower() == "video"
        }
        normalized_codecs = {codec for codec in raw_codecs if codec}
        if normalized_codecs and not any("hevc" in codec.lower() for codec in normalized_codecs):
            non_hevc_rows.append({
                "file": str(mkv),
                "codecs": ", ".join(sorted(normalized_codecs)),
                "size_bytes": str(size),
                "size_human": human_size(size),
            })

    elapsed = time.perf_counter() - start
    log.info(f"Probed {scanned_files} MKV files in {elapsed:.2f}s.")

    written_reports: Dict[str, Dict[str, object]] = {}

    if track_rows and write_csv_file:
        written = export_report(
            track_rows,
            base_name="mkv_scan_tracks",
            output_dir=base_output_dir,
            write_csv_file=True,
            dry_run=dry_run,
        )
        written_reports["tracks"] = {
            "path": written.get("csv"),
            "rows": len(track_rows),
        }
    else:
        log.warning("No track rows captured — skipping CSV export.")

    if name_list_rows and write_csv_file:
        csv_path = timestamped_filename("mkv_scan_name_list", output_dir=base_output_dir)
        written_reports["name_list"] = {
            "path": write_csv(name_list_rows, csv_path, dry_run=dry_run),
            "rows": len(name_list_rows),
        }

    if non_hevc_rows and write_csv_file:
        csv_path = timestamped_filename("mkv_scan_non_hevc", output_dir=base_output_dir)
        written_reports["non_hevc"] = {
            "path": write_csv(non_hevc_rows, csv_path, dry_run=dry_run),
            "rows": len(non_hevc_rows),
        }

    if failed_files and write_csv_file:
        csv_path = timestamped_filename("mkv_scan_failures", output_dir=base_output_dir)
        written_reports["failures"] = {
            "path": write_csv(failed_files, csv_path, dry_run=dry_run),
            "rows": len(failed_files),
        }

    log.info(
        "Scan summary — files=%d, tracks=%d, non_hevc=%d, failures=%d, elapsed=%.2fs",
        scanned_files,
        len(track_rows),
        len(non_hevc_rows),
        len(failed_files),
        elapsed,
    )

    report_counts = {
        "tracks": (len(track_rows), written_reports.get("tracks", {}).get("path")),
        "name_list": (len(name_list_rows), written_reports.get("name_list", {}).get("path")),
        "non_hevc": (len(non_hevc_rows), written_reports.get("non_hevc", {}).get("path")),
        "failures": (len(failed_files), written_reports.get("failures", {}).get("path")),
    }

    for label, (count, path_obj) in report_counts.items():
        title = label.replace("_", " ").title()
        if count <= 0:
            log.info("%s report skipped — no rows captured.", title)
            continue

        path_str = str(path_obj) if path_obj else ("(skipped write)" if not write_csv_file else "(not written)")
        log.info("%s report → %d rows (%s)", title, count, path_str)

    return track_rows
def _get_metadata_title(file_path: Path) -> str:
    """Return metadata title for a file using ffprobe."""

    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_format",
        str(file_path),
    ]
    code, out, err = run_command(cmd, capture=True)
    if code != 0 or not out:
        if err:
            log.debug(f"ffprobe metadata title extraction failed for {file_path}: {err.strip()}")
        return ""

    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        log.debug(f"Invalid JSON from ffprobe for {file_path}")
        return ""

    return payload.get("format", {}).get("tags", {}).get("title", "") or ""


_PAREN_SUFFIX_RE = re.compile(r"(?:\s*\([^)]*\))+\s*$")


def _strip_extension(path: Path) -> Tuple[str, str]:
    suffix = path.suffix
    if suffix:
        base = path.with_suffix("").name
        return base, suffix
    return path.name, ""


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


def _derive_names(path: Path, type_code: str) -> Tuple[str, str]:
    if type_code == "f":
        base_name, _ = _strip_extension(path)
    else:
        base_name, _ = path.name, ""

    cleaned = _remove_parenthetical_suffix(base_name)
    cleaned = _remove_release_suffix(cleaned)
    edited = _move_leading_article(cleaned)
    return base_name, edited


def _build_name_list_row(file_path: Path) -> Dict[str, str]:
    """Construct a standardized name-list row for files."""

    base_name, edited_name = _derive_names(file_path, "f")
    metadata_title = _get_metadata_title(file_path)
    return {
        "path": str(file_path),
        "type": "f",
        "name": base_name,
        "edited_name": edited_name,
        "metadata_title": metadata_title,
        "edited_title": "",
    }


def _build_directory_row(directory: Path) -> Dict[str, str]:
    base_name, edited_name = _derive_names(directory, "d")
    return {
        "path": str(directory),
        "type": "d",
        "name": base_name,
        "edited_name": edited_name,
        "metadata_title": "",
        "edited_title": "",
    }
