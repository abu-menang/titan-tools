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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from common.base.logging import get_logger
from common.base.fs import ensure_dir, human_size
from common.base.ops import run_command
from common.shared.loader import load_media_types, load_task_config
from common.shared.report import ColumnSpec, write_tabular_reports, timestamped_filename
from common.base.file_io import open_file
from common.shared.utils import Progress

log = get_logger(__name__)

MEDIA_TYPES = load_media_types()

# Include all formats you care about for *listing* videos.
VIDEO_EXTS: set[str] = set(MEDIA_TYPES.video_exts)

# For MKV scan, we only act on MKV files (mkvmerge-best path).
MKV_EXTS: set[str] = {".mkv"}

NAME_LIST_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("type", "type", width=6),
    ColumnSpec("name", "name", width=40),
    ColumnSpec("edited_name", "edited_name", width=40),
    ColumnSpec("title", "title", width=40),
    ColumnSpec("edited_title", "edited_title", width=40),
    ColumnSpec("path", "path", width=80),
]

TRACK_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("filename", "filename", width=40),
    ColumnSpec("type", "type", width=10),
    ColumnSpec("id", "id", width=6),
    ColumnSpec("name", "name", width=40),
    ColumnSpec("edited_name", "edited_name", width=40),
    ColumnSpec("lang", "lang", width=8),
    ColumnSpec("codec", "codec", width=18),
    ColumnSpec("default", "default", width=8),
    ColumnSpec("forced", "forced", width=8),
    ColumnSpec("width", "width", width=10),
    ColumnSpec("height", "height", width=10),
    ColumnSpec("frame_rate", "frame_rate", width=12),
    ColumnSpec("channels", "channels", width=12),
    ColumnSpec("sample_rate", "sample_rate", width=12),
    ColumnSpec("bit_depth", "bit_depth", width=10),
    ColumnSpec("encoding", "encoding", width=16),
    ColumnSpec("path", "path", width=80),
]

NON_HEVC_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("filename", "filename", width=40),
    ColumnSpec("codecs", "codecs", width=40),
    ColumnSpec("path", "path", width=80),
]

FAILURE_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("path", "path", width=80),
    ColumnSpec("reason", "reason", width=60),
]

SKIPPED_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("filename", "filename", width=40),
    ColumnSpec("reason", "reason", width=40),
    ColumnSpec("path", "path", width=80),
]


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
    include_all: bool = False,
) -> Iterable[Path]:
    """Yield files under given roots, optionally filtering by extensions."""
    for root in roots:
        root = root.resolve()
        if not root.exists():
            log.warning(f"Path does not exist: {root}")
            continue

        if root.is_file():
            if (include_all or root.suffix.lower() in exts) and (not exclude_hidden or not root.name.startswith(".")):
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
                if include_all or p.suffix.lower() in exts:
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

        row: Dict[str, Any] = {
            "filename": file_path.name,
            "type": track_type,
            "id": str(track_id) if track_id is not None else "",
            "name": track_name,
            "edited_name": suggested,
            "lang": lang,
            "codec": codec,
            "default": default,
            "forced": forced,
            "width": "",
            "height": "",
            "frame_rate": "",
            "channels": "",
            "sample_rate": "",
            "bit_depth": "",
            "encoding": "",
            "path": str(file_path),
        }

        if track_type == "video":
            row["width"] = str(width) if width else ""
            row["height"] = str(height) if height else ""
            row["frame_rate"] = str(frame_rate or "")
        elif track_type == "audio":
            row["channels"] = str(props.get("audio_channels") or "")
            row["sample_rate"] = str(props.get("audio_sampling_frequency") or "")
            row["bit_depth"] = str(props.get("audio_sample_depth") or "")
        elif track_type == "subtitles":
            encoding_value = props.get("encoding") or props.get("codec_private_data") or ""
            row["encoding"] = str(encoding_value)

        # Preserve file size context for reference (hidden from main columns).
        row["size_bytes"] = file_size
        row["size_human"] = human_size(file_size)

        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _FileScanResult:
    path: Path
    name_row: Optional[Dict[str, str]] = None
    track_rows: List[Dict[str, object]] = field(default_factory=list)
    non_hevc_row: Optional[Dict[str, object]] = None
    failure_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def vid_mkv_scan(
    roots: Optional[Iterable[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path] = None,
    write_csv_file: bool = True,
    dry_run: bool = False,
    batch_size: Optional[int] = None,
) -> List[Dict[str, object]]:
    """
    Probe MKV files and collect detailed per-track metadata similar to the legacy mkv-scan utility.

    Args:
        roots: One or more starting paths. Defaults to [Path.cwd()].
        output_dir: Directory for report storage (takes precedence over output_root).
        output_root: Fallback directory sourced from task defaults when output_dir is unset.
        write_csv_file: Toggle CSV export. Scripts now produce CSV-only report files.
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
    file_results: List[_FileScanResult] = []
    seen_directories: Set[Path] = set()
    directory_rows: List[Dict[str, str]] = []
    directory_key_map: Dict[str, int] = {}

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
        row = _build_directory_row(resolved)
        directory_rows.append(row)
        index = len(directory_rows) - 1
        directory_key_map.setdefault(str(resolved), index)
        directory_key_map.setdefault(str(directory.expanduser()), index)

    for root in resolved_roots:
        register_directory(root)
    failed_files: List[Dict[str, str]] = []
    non_hevc_rows: List[Dict[str, object]] = []
    scanned_files = 0

    skipped_files: List[Dict[str, str]] = []

    for candidate in Progress(
        _iter_files(resolved_roots, MKV_EXTS, exclude_dir=base_output_dir, include_all=True),
        desc="Probing MKV",
    ):
        suffix = candidate.suffix.lower()
        if suffix not in MKV_EXTS:
            skipped_files.append({
                "path": str(candidate),
                "filename": candidate.name,
                "reason": f"unsupported extension ({suffix or 'none'})",
            })
            continue

        scanned_files += 1
        file_entry = _FileScanResult(path=candidate)
        file_results.append(file_entry)

        try:
            stat_result = candidate.stat()
            size = stat_result.st_size
        except FileNotFoundError:
            reason = "file missing during scan"
            failed_files.append({"path": str(candidate), "reason": reason})
            file_entry.failure_reason = reason
            continue

        current_dir = candidate.parent
        while True:
            register_directory(current_dir)
            try:
                resolved_current = current_dir.resolve()
            except FileNotFoundError:
                resolved_current = current_dir
            if resolved_current in root_set or current_dir.parent == current_dir:
                break
            current_dir = current_dir.parent

        file_entry.name_row = _build_name_list_row(candidate)

        code, out, err = run_command(["mkvmerge", "-J", str(candidate)], capture=True, stream=False)
        if code != 0 or not out:
            reason = (err or "").strip() or "mkvmerge returned no output"
            failed_files.append({"path": str(candidate), "reason": reason})
            file_entry.failure_reason = reason
            log.error(f"❌ mkvmerge failed for {candidate.name}: {reason}")
            continue

        try:
            mkvmerge_json = json.loads(out)
        except json.JSONDecodeError:
            reason = "invalid JSON from mkvmerge"
            failed_files.append({"path": str(candidate), "reason": reason})
            file_entry.failure_reason = reason
            log.error(f"❌ Invalid JSON output from mkvmerge for {candidate.name}")
            continue

        rows = _extract_track_rows(candidate, mkvmerge_json, size)
        if not rows:
            reason = "no track data"
            failed_files.append({"path": str(candidate), "reason": reason})
            file_entry.failure_reason = reason
            log.warning(f"⚠️ No track data recorded for {candidate}")
            continue

        track_rows.extend(rows)
        file_entry.track_rows = rows

        raw_codecs = {
            (row.get("codec") or "").strip()
            for row in rows
            if (row.get("type") or "").lower() == "video"
        }
        normalized_codecs = {codec for codec in raw_codecs if codec}
        if normalized_codecs and not any("hevc" in codec.lower() for codec in normalized_codecs):
            non_hevc_row = {
                "path": str(candidate),
                "filename": candidate.stem,
                "codecs": ", ".join(sorted(normalized_codecs)),
                "size_bytes": str(size),
                "size_human": human_size(size),
            }
            non_hevc_rows.append(non_hevc_row)
            file_entry.non_hevc_row = non_hevc_row

    elapsed = time.perf_counter() - start
    log.info(f"Probed {scanned_files} MKV files in {elapsed:.2f}s.")

    try:
        normalized_batch = int(batch_size) if batch_size is not None else 0
    except (TypeError, ValueError):
        normalized_batch = 0
    if normalized_batch < 0:
        normalized_batch = 0

    non_hevc_rows.sort(key=lambda row: row["path"])
    failed_files.sort(key=lambda row: row["path"])
    skipped_files.sort(key=lambda row: row["path"])

    chunkable_results = [entry for entry in file_results if entry.name_row]
    if normalized_batch <= 0:
        file_chunks: List[List[_FileScanResult]] = [chunkable_results] if chunkable_results else []
    else:
        file_chunks = [
            chunkable_results[i : i + normalized_batch]
            for i in range(0, len(chunkable_results), normalized_batch)
        ]

    # ------------------------------------------------------------------
    # Classification: use language whitelist rules (configurable per task)
    # Any track language NOT matching the configured whitelist for its
    # type will mark the file as an 'issue'. Also keep the existing
    # structural rules (more-than-one, none).
    # ------------------------------------------------------------------
    try:
        task_conf = load_task_config("vid_mkv_scan", None)
    except Exception:
        task_conf = {}

    allowed_vid = [str(s).strip().lower() for s in (task_conf.get("lang_vid") or ["eng"]) if s]
    allowed_aud = [str(s).strip().lower() for s in (task_conf.get("lang_aud") or ["eng"]) if s]
    allowed_sub = [str(s).strip().lower() for s in (task_conf.get("lang_sub") or ["eng"]) if s]

    # Log when a whitelist is explicitly empty (disabled) so users understand
    # language checks are being skipped for that type.
    if task_conf.get("lang_vid") is not None and not allowed_vid:
        log.info("vid_mkv_scan: 'lang_vid' is empty — video language whitelist disabled")
    if task_conf.get("lang_aud") is not None and not allowed_aud:
        log.info("vid_mkv_scan: 'lang_aud' is empty — audio language whitelist disabled")
    if task_conf.get("lang_sub") is not None and not allowed_sub:
        log.info("vid_mkv_scan: 'lang_sub' is empty — subtitle language whitelist disabled")

    video_counts: dict[str, int] = {}
    audio_counts: dict[str, int] = {}
    subtitle_counts: dict[str, int] = {}

    # Helper to check whether a given language value matches any allowed prefix
    def _lang_matches(lang_val: object, allowed_prefixes: List[str]) -> bool:
        # If no allowed prefixes are configured, treat the rule as disabled
        # (i.e., any language is acceptable).
        if not allowed_prefixes:
            return True
        if not isinstance(lang_val, str):
            return False
        l = lang_val.strip().lower()
        for pref in allowed_prefixes:
            if l.startswith(pref):
                return True
        return False

    # Track files that fail language checks per type
    bad_lang_vid: set[str] = set()
    bad_lang_aud: set[str] = set()
    bad_lang_sub: set[str] = set()

    for entry in file_results:
        fname = entry.path.name
        rows = entry.track_rows or []
        v = sum(1 for r in rows if (r.get("type") or "").lower() == "video")
        a = sum(1 for r in rows if (r.get("type") or "").lower() == "audio")
        s = sum(1 for r in rows if (r.get("type") or "").lower() == "subtitles")
        video_counts[fname] = v
        audio_counts[fname] = a
        subtitle_counts[fname] = s

        # Language mismatches: any track of a type with a language that
        # doesn't match the allowed prefixes marks the file as bad for
        # that type.
        for r in rows:
            rtype = (r.get("type") or "").lower()
            rlang = r.get("lang") or ""
            if rtype == "video":
                if not _lang_matches(rlang, allowed_vid):
                    bad_lang_vid.add(fname)
            elif rtype == "audio":
                if not _lang_matches(rlang, allowed_aud):
                    bad_lang_aud.add(fname)
            elif rtype == "subtitles":
                if not _lang_matches(rlang, allowed_sub):
                    bad_lang_sub.add(fname)

    issues_set: set[str] = set()
    all_names = sorted(set(list(video_counts.keys()) + list(audio_counts.keys()) + list(subtitle_counts.keys())))
    for fname in all_names:
        v = int(video_counts.get(fname, 0))
        a = int(audio_counts.get(fname, 0))
        s = int(subtitle_counts.get(fname, 0))

        lang_issue = fname in bad_lang_vid or fname in bad_lang_aud or fname in bad_lang_sub

        if (
            v > 1
            or a > 1
            or s > 1
            or v == 0
            or a == 0
            or s == 0
            or lang_issue
        ):
            issues_set.add(fname)

    ok_files = [entry for entry in chunkable_results if entry.path.name not in issues_set]
    issues_files = [entry for entry in chunkable_results if entry.path.name in issues_set]

    def _chunk_entries(entries: List[_FileScanResult]) -> List[List[_FileScanResult]]:
        if not entries:
            return []
        if normalized_batch <= 0:
            return [entries]
        return [entries[i : i + normalized_batch] for i in range(0, len(entries), normalized_batch)]

    ok_file_chunks = _chunk_entries(ok_files)
    issues_file_chunks = _chunk_entries(issues_files)

    # Build per-group track chunk rows
    ok_track_chunk_rows: List[List[Dict[str, object]]] = []
    for chunk in ok_file_chunks:
        rows = [row for entry in chunk for row in entry.track_rows]
        if rows:
            rows.sort(key=lambda row: (row["path"], row.get("type", ""), row.get("id", "")))
            ok_track_chunk_rows.append(rows)

    issues_track_chunk_rows: List[List[Dict[str, object]]] = []
    for chunk in issues_file_chunks:
        rows = [row for entry in chunk for row in entry.track_rows]
        if rows:
            rows.sort(key=lambda row: (row["path"], row.get("type", ""), row.get("id", "")))
            issues_track_chunk_rows.append(rows)

    # Prepare summary language-mismatch lists for later reporting
    bad_vid = sorted(bad_lang_vid)
    bad_aud = sorted(bad_lang_aud)
    bad_sub = sorted(bad_lang_sub)

    def _collect_directory_rows(file_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
        required_indices: Set[int] = set()
        for row in file_rows:
            path_obj = Path(row["path"])
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
        selected = [row for idx, row in enumerate(directory_rows) if idx in required_indices]
        selected.sort(key=lambda row: row["path"])
        return selected

    track_chunk_rows: List[List[Dict[str, object]]] = []
    name_list_chunk_rows: List[List[Dict[str, str]]] = []
    non_hevc_chunk_rows: List[List[Dict[str, object]]] = []
    failure_chunk_rows: List[List[Dict[str, str]]] = []

    for chunk in file_chunks:
        if not chunk:
            continue

        chunk_track_rows = [row for entry in chunk for row in entry.track_rows]
        if chunk_track_rows:
            chunk_track_rows.sort(key=lambda row: (row["path"], row.get("type", ""), row.get("id", "")))
            track_chunk_rows.append(chunk_track_rows)

        chunk_file_rows = [entry.name_row for entry in chunk if entry.name_row]
        if chunk_file_rows:
            chunk_file_rows.sort(key=lambda row: row["path"])
            chunk_directories = _collect_directory_rows(chunk_file_rows)
            combined = chunk_directories + chunk_file_rows
            combined.sort(key=lambda row: row["path"])
            name_list_chunk_rows.append(combined)

        chunk_non_hevc = [entry.non_hevc_row for entry in chunk if entry.non_hevc_row]
        if chunk_non_hevc:
            chunk_non_hevc.sort(key=lambda row: row["path"])
            non_hevc_chunk_rows.append(chunk_non_hevc)

        chunk_failures = [
            {"path": str(entry.path), "reason": entry.failure_reason}
            for entry in chunk
            if entry.failure_reason
        ]
        if chunk_failures:
            chunk_failures.sort(key=lambda row: row["path"])
            failure_chunk_rows.append(chunk_failures)

    orphan_failures = [
        {"path": str(entry.path), "reason": entry.failure_reason}
        for entry in file_results
        if entry.failure_reason and entry.name_row is None
    ]
    if orphan_failures:
        failure_chunk_rows.append(orphan_failures)

    written_reports: Dict[str, Dict[str, object]] = {}

    # XLS styling removed; reports are CSV-only.
    
    # Write split track CSVs: issues and ok. Each group's batching is handled
    # independently (we already built ok_track_chunk_rows and
    # issues_track_chunk_rows above).
    total_track_paths: List[Path] = []
    if issues_track_chunk_rows and write_csv_file:
        issues_result = write_tabular_reports(
            issues_track_chunk_rows,
            "mkv_scan_tracks_issues",
            TRACK_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["tracks_issues"] = {
            "paths": issues_result.csv_paths,
            "rows": sum(len(chunk) for chunk in issues_track_chunk_rows),
        }
        total_track_paths.extend(issues_result.csv_paths if isinstance(issues_result.csv_paths, list) else [issues_result.csv_paths])

    if ok_track_chunk_rows and write_csv_file:
        ok_result = write_tabular_reports(
            ok_track_chunk_rows,
            "mkv_scan_tracks_ok",
            TRACK_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["tracks_ok"] = {
            "paths": ok_result.csv_paths,
            "rows": sum(len(chunk) for chunk in ok_track_chunk_rows),
        }
        total_track_paths.extend(ok_result.csv_paths if isinstance(ok_result.csv_paths, list) else [ok_result.csv_paths])

    if not total_track_paths:
        log.warning("No track rows captured — skipping export.")

    if name_list_chunk_rows and write_csv_file:
        name_list_result = write_tabular_reports(
            name_list_chunk_rows,
            "mkv_scan_name_list",
            NAME_LIST_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["name_list"] = {
            "paths": name_list_result.csv_paths,
            "rows": sum(len(chunk) for chunk in name_list_chunk_rows),
        }

    if non_hevc_rows and write_csv_file:
        non_hevc_result = write_tabular_reports(
            non_hevc_chunk_rows if non_hevc_chunk_rows else [non_hevc_rows],
            "mkv_scan_non_hevc",
            NON_HEVC_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["non_hevc"] = {
            "paths": non_hevc_result.csv_paths,
            "rows": len(non_hevc_rows),
        }

    if failed_files and write_csv_file:
        failure_result = write_tabular_reports(
            failure_chunk_rows if failure_chunk_rows else [failed_files],
            "mkv_scan_failures",
            FAILURE_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["failures"] = {
            "paths": failure_result.csv_paths,
            "rows": len(failed_files),
        }

    if skipped_files and write_csv_file:
        skipped_result = write_tabular_reports(
            [skipped_files],
            "mkv_scan_skipped",
            SKIPPED_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["skipped"] = {
            "paths": skipped_result.csv_paths,
            "rows": len(skipped_files),
        }
        if skipped_result.csv_paths:
            written_reports["skipped"]["csv_paths"] = skipped_result.csv_paths
        log.info(f"Skipped files (non-MKV or excluded): {len(skipped_files)}")

    log.info(
        "Scan summary — files=%d, tracks=%d, non_hevc=%d, failures=%d, elapsed=%.2fs",
        scanned_files,
        len(track_rows),
        len(non_hevc_rows),
        len(failed_files),
        elapsed,
    )

    unique_name_list_count = len(directory_rows) + len(chunkable_results)

    report_counts = {
        "tracks": (len(track_rows), []),
        "name_list": (
            unique_name_list_count,
            written_reports.get("name_list", {}).get("paths", []),
        ),
        "non_hevc": (len(non_hevc_rows), written_reports.get("non_hevc", {}).get("paths", [])),
        "failures": (len(failed_files), written_reports.get("failures", {}).get("paths", [])),
        "skipped": (len(skipped_files), written_reports.get("skipped", {}).get("paths", [])),
    }

    for label, (count, paths) in report_counts.items():
        title = label.replace("_", " ").title()
        if count <= 0:
            log.info("%s report skipped — no rows captured.", title)
            continue

        # Some report entries (like tracks) aggregate multiple underlying
        # CSV exports (tracks_ok and tracks_issues). Resolve combined paths
        # for logging here.
        if label == "tracks":
            combined_paths: List[Path] = []
            for key in ("tracks_issues", "tracks_ok"):
                info = written_reports.get(key, {})
                p = info.get("paths")
                if not p:
                    continue
                if isinstance(p, list):
                    combined_paths.extend(p)
                else:
                    combined_paths.append(p)
            paths = combined_paths

        if not paths:
            path_str = "(skipped write)" if not write_csv_file else "(not written)"
        elif isinstance(paths, (list, tuple)) and len(paths) == 1:
            path_str = str(paths[0])
        elif isinstance(paths, (list, tuple)):
            path_str = ", ".join(str(p) for p in paths)
        else:
            path_str = str(paths)

        log.info("%s report → %d rows (%s)", title, count, path_str)

    # -------------------------
    # Write a human-readable summary file
    # -------------------------
    try:
        # Aggregate per-file track counts
        video_counts: dict[str, int] = {}
        audio_counts: dict[str, int] = {}
        subtitle_counts: dict[str, int] = {}
        eng_subtitle_present: set[str] = set()

        for entry in file_results:
            fname = entry.path.name
            rows = entry.track_rows or []
            v = sum(1 for r in rows if (r.get("type") or "").lower() == "video")
            a = sum(1 for r in rows if (r.get("type") or "").lower() == "audio")
            s = sum(1 for r in rows if (r.get("type") or "").lower() == "subtitles")
            video_counts[fname] = v
            audio_counts[fname] = a
            subtitle_counts[fname] = s
            if any(
                (r.get("type") or "").lower() == "subtitles"
                and (r.get("lang") or "").lower().startswith(("eng", "en"))
                for r in rows
            ):
                eng_subtitle_present.add(fname)

        more_than_1_video = sorted(((f, c) for f, c in video_counts.items() if c > 1), key=lambda t: -t[1])
        more_than_1_audio = sorted(((f, c) for f, c in audio_counts.items() if c > 1), key=lambda t: -t[1])
        more_than_1_subtitle = sorted(((f, c) for f, c in subtitle_counts.items() if c > 1), key=lambda t: -t[1])

        no_video = sorted([f for f, c in video_counts.items() if c == 0])
        no_audio = sorted([f for f, c in audio_counts.items() if c == 0])
        no_subtitles = sorted([f for f, c in subtitle_counts.items() if c == 0])

        # Prepare the summary file
        summary_path = timestamped_filename("mkv_scan_tracks_summary", "txt", base_output_dir)
        with open_file(summary_path, "w") as out:
            # ANSI color codes
            RESET = "\x1b[0m"
            BOLD = "\x1b[1m"
            CYAN = "\x1b[36m"
            YELLOW = "\x1b[33m"
            RED = "\x1b[31m"
            GREEN = "\x1b[32m"

            # Header
            out.write(f"{BOLD}{CYAN}📋 MKV Scan Tracks Summary{RESET}\n")
            out.write(f"{BOLD}Generated:{RESET} " + summary_path.name + "\n\n")

            # Build a combined table of per-file counts and dynamic language issues
            summary_rows: list[tuple[str, int, int, int, str]] = []
            for fname in sorted(set(list(video_counts.keys()) + list(audio_counts.keys()) + list(subtitle_counts.keys()))):
                v = int(video_counts.get(fname, 0))
                a = int(audio_counts.get(fname, 0))
                s = int(subtitle_counts.get(fname, 0))
                # Dynamic language issue flags
                lang_flags: list[str] = []
                if fname in bad_vid:
                    lang_flags.append("vid")
                if fname in bad_aud:
                    lang_flags.append("aud")
                if fname in bad_sub:
                    lang_flags.append("sub")
                lang_flag_str = ",".join(lang_flags) if lang_flags else ""
                summary_rows.append((fname, v, a, s, lang_flag_str))

            # Write CSV-like header (extra 'lang_issues' column is dynamic)
            out.write(f"{BOLD}{CYAN}filename,video,audio,sub,lang_issues{RESET}\n")
            for fname, v, a, s, lang_flag_str in summary_rows:
                # Colour rows with problems
                has_structural_problem = v == 0 or a == 0 or s == 0 or v > 1 or a > 1 or s > 1
                has_lang_problem = bool(lang_flag_str)
                line = f"{fname},{v},{a},{s},{lang_flag_str}"
                if has_structural_problem or has_lang_problem:
                    out.write(f"{YELLOW}{line}{RESET}\n")
                else:
                    out.write(f"{line}\n")
            out.write("\n")

        log.info("Wrote summary → %s", summary_path)
        # Also emit a rendered HTML summary alongside the text file so it can be
        # opened in a browser or KWrite preview. Keep this best-effort and do not
        # fail the run if HTML write fails.
        try:
            html_path = timestamped_filename("mkv_scan_tracks_summary", "html", base_output_dir)
            html_parts: list[str] = []
            html_parts.append("<!doctype html>")
            html_parts.append("<html><head><meta charset=\"utf-8\"><title>MKV Scan Tracks Summary</title>")
            html_parts.append(
                "<style>body{font-family:sans-serif;padding:18px}h1{font-size:1.3rem}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:6px}details{margin:8px 0}summary{cursor:pointer;font-weight:600;padding:6px}tr.warn td{background:#fff3cd}</style>"
            )
            html_parts.append("</head><body>")
            html_parts.append(f"<h1>📋 MKV Scan Tracks Summary</h1><p><strong>Generated:</strong> {html_path.name}</p>")

            # Small summary bar with totals
            totals_html = (
                f"<div style=\"margin:8px 0;padding:8px;background:#f4f8fb;border-radius:6px;\">"
                f"<strong>Files scanned:</strong> {scanned_files} &nbsp; "
                f"<strong>Tracks:</strong> {len(track_rows)} &nbsp; "
                f"<strong>Non-HEVC:</strong> {len(non_hevc_rows)} &nbsp; "
                f"<strong>Failures:</strong> {len(failed_files)} &nbsp; "
                f"<strong>Skipped:</strong> {len(skipped_files)}"
                f"</div>"
            )
            html_parts.append(totals_html)

            # Combined per-file summary table (filename, video, audio, sub, sub_eng)
            def _html_summary_table(rows: list[tuple[str,int,int,int,str]]):
                if not rows:
                    return "<h2>Summary</h2><p><em>None</em></p>"
                out = ["<h2>Per-file summary</h2>", "<table>", "<thead><tr><th>filename</th><th style=\"text-align:right\">video</th><th style=\"text-align:right\">audio</th><th style=\"text-align:right\">sub</th><th>lang_issues</th></tr></thead>", "<tbody>"]
                for fn, v, a, s, lang_flags in rows:
                    cls = ""
                    if v == 0 or a == 0 or s == 0 or lang_flags:
                        cls = " class=\"warn\""
                    out.append(f"<tr{cls}><td>{fn}</td><td style=\"text-align:right\">{v}</td><td style=\"text-align:right\">{a}</td><td style=\"text-align:right\">{s}</td><td>{lang_flags}</td></tr>")
                out.append("</tbody></table>")
                return "".join(out)

            # Make the per-file summary collapsible using the <details> element.
            html_parts.append("<details>")
            html_parts.append("<summary>📄 Per-file summary (click to expand)</summary>")
            if summary_rows:
                html_parts.append(_html_summary_table(summary_rows))
            else:
                html_parts.append("<p><em>None</em></p>")
            html_parts.append("</details>")

            def _html_table(title: str, rows: list[tuple[str,int]]):
                if not rows:
                    return f"<h2>{title}</h2><p><em>None</em></p>"
                out = [f"<h2>{title}</h2>", "<table>", "<thead><tr><th>filename</th><th style=\"text-align:right\">count</th></tr></thead>", "<tbody>"]
                for fn, cnt in rows:
                    out.append(f"<tr><td>{fn}</td><td style=\"text-align:right\">{cnt}</td></tr>")
                out.append("</tbody></table>")
                return "".join(out)

            html_parts.append(_html_table("🎞️ more_than_1_video", more_than_1_video))
            html_parts.append(_html_table("🔊 more_than_1_audio", more_than_1_audio))
            html_parts.append(_html_table("📝 more_than_1_subtitle", more_than_1_subtitle))

            def _html_list(title: str, items: list[str]):
                if not items:
                    return f"<h3>{title}</h3><p><em>None</em></p>"
                lines = [f"<h3>{title}</h3>", "<ul>"]
                for it in items:
                    lines.append(f"<li>{it}</li>")
                lines.append("</ul>")
                return "".join(lines)

            html_parts.append(_html_list("❌ no_video", no_video))
            html_parts.append(_html_list("❌ no_audio", no_audio))
            html_parts.append(_html_list("❗ no_subtitles", no_subtitles))
            html_parts.append(_html_list("⚠️ lang mismatch - video", bad_vid))
            html_parts.append(_html_list("⚠️ lang mismatch - audio", bad_aud))
            html_parts.append(_html_list("⚠️ lang mismatch - subtitles", bad_sub))

            # CSV exports links (if any)
            try:
                csv_links = []
                for label, info in written_reports.items():
                    paths = info.get("paths") if isinstance(info, dict) else None
                    if not paths:
                        continue
                    if not isinstance(paths, list):
                        paths = [paths]
                    for p in paths:
                        name = p.name if isinstance(p, Path) else str(p)
                        csv_links.append(f"<li><a href=\"{name}\">{label} → {name}</a></li>")
                if csv_links:
                    html_parts.append("<h2>CSV exports</h2><ul>")
                    html_parts.extend(csv_links)
                    html_parts.append("</ul>")
            except Exception:
                pass

            html_parts.append("</body></html>")
            html_content = "\n".join(html_parts)
            with open_file(html_path, "w") as h:
                h.write(html_content)
            log.info("Wrote HTML summary → %s", html_path)
        except Exception:
            log.exception("Failed to write HTML summary")
    except Exception:
        log.exception("Failed to write mkv scan summary")

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
        "type": "f",
        "name": base_name,
        "edited_name": edited_name,
        "title": metadata_title,
        "edited_title": "",
        "path": str(file_path),
    }


def _build_directory_row(directory: Path) -> Dict[str, str]:
    base_name, edited_name = _derive_names(directory, "d")
    return {
        "type": "d",
        "name": base_name,
        "edited_name": edited_name,
        "title": "",
        "edited_title": "",
        "path": str(directory),
    }
