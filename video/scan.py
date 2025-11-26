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
SUBTITLE_EXTS: set[str] = {
    ".srt",
    ".ass",
    ".ssa",
    ".sub",
    ".idx",
    ".sup",
    ".vtt",
}

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

EXTERNAL_SUB_COLUMNS: List[ColumnSpec] = [
    ColumnSpec("output_filename", "output_filename", width=60),
    ColumnSpec("type", "type", width=10),
    ColumnSpec("id", "id", width=6),
    ColumnSpec("name", "name", width=40),
    ColumnSpec("edited_name", "edited_name", width=40),
    ColumnSpec("lang", "lang", width=8),
    ColumnSpec("codec", "codec", width=24),
    ColumnSpec("default", "default", width=8),
    ColumnSpec("forced", "forced", width=8),
    ColumnSpec("output_path", "output_path", width=120),
    ColumnSpec("input_path", "input_path", width=120),
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


_LANG_HINTS: Set[str] = {
    "en",
    "eng",
    "english",
    "es",
    "spa",
    "spanish",
    "fr",
    "fra",
    "fre",
    "french",
    "de",
    "ger",
    "deu",
    "german",
    "it",
    "ita",
    "italian",
    "pt",
    "por",
    "portuguese",
    "hi",
    "hin",
    "ml",
    "mal",
    "ta",
    "tam",
    "te",
    "tel",
    "kn",
    "kan",
    "ja",
    "jpn",
    "jp",
    "zh",
    "zho",
    "chs",
    "cht",
    "cn",
    "ko",
    "kor",
    "ru",
    "rus",
    "ar",
    "ara",
    "he",
    "heb",
}

_TRAILING_HINTS: Set[str] = _LANG_HINTS | {
    "sdh",
    "cc",
    "forced",
    "signs",
    "sub",
    "subs",
    "subtitle",
}

_LANG_TOKEN_MAP: Dict[str, str] = {
    "en": "eng",
    "eng": "eng",
    "english": "eng",
    "es": "spa",
    "spa": "spa",
    "spanish": "spa",
    "fr": "fra",
    "fra": "fra",
    "fre": "fra",
    "french": "fra",
    "de": "deu",
    "deu": "deu",
    "ger": "deu",
    "german": "deu",
    "it": "ita",
    "ita": "ita",
    "italian": "ita",
    "pt": "por",
    "por": "por",
    "portuguese": "por",
    "hi": "hin",
    "hin": "hin",
    "ml": "mal",
    "mal": "mal",
    "malayalam": "mal",
    "ta": "tam",
    "tam": "tam",
    "tamil": "tam",
    "te": "tel",
    "tel": "tel",
    "telugu": "tel",
    "kn": "kan",
    "kan": "kan",
    "kannada": "kan",
    "ja": "jpn",
    "jp": "jpn",
    "jpn": "jpn",
    "japanese": "jpn",
    "zh": "zho",
    "zho": "zho",
    "chs": "zho",
    "cht": "zho",
    "cn": "zho",
    "chi": "zho",
    "ko": "kor",
    "kor": "kor",
    "korean": "kor",
    "ru": "rus",
    "rus": "rus",
    "russian": "rus",
    "ar": "ara",
    "ara": "ara",
    "arabic": "ara",
    "he": "heb",
    "heb": "heb",
    "hebrew": "heb",
}

def _tokenize(text: str) -> List[str]:
    return [t for t in re.split(r"[\\s._-]+", text) if t]


def _extract_lang_hint(tokens: Iterable[str]) -> str:
    for tok in tokens:
        key = tok.lower()
        if key in _LANG_TOKEN_MAP:
            return _LANG_TOKEN_MAP[key]
        if key in _LANG_HINTS:
            return key
    return ""


def _bool_flag(val: object, default: bool = False) -> str:
    if val is None:
        return "true" if default else "false"
    return "true" if bool(val) else "false"


def _subtitle_matches(video: Path, sub: Path) -> bool:
    """Heuristic match between a video and subtitle filename."""

    v_key = _build_match_key(video)
    s_key = _build_match_key(sub)
    if v_key and s_key and (v_key in s_key or s_key in v_key):
        return True

    def _alnum(token: str) -> str:
        return re.sub(r"[^a-z0-9]", "", token.lower())

    v_clean = _alnum(video.stem)
    s_clean = _alnum(sub.stem)
    return v_clean and s_clean and (v_clean in s_clean or s_clean in v_clean)


def _build_match_key(path: Path) -> str:
    """Return a normalized key for matching videos to external subtitles."""

    tokens = _tokenize(path.stem)
    while tokens and tokens[-1].lower() in _TRAILING_HINTS:
        tokens.pop()
    if not tokens:
        tokens = [path.stem]
    return " ".join(tokens).lower()


def _probe_av_streams(file_path: Path) -> Dict[str, str]:
    """Best-effort probe for primary video/audio stream metadata via mkvmerge."""

    code, out, err = run_command(["mkvmerge", "-J", str(file_path)], capture=True, stream=False)
    if code != 0 or not out:
        if err:
            log.debug(f"mkvmerge stream probe failed for {file_path}: {err.strip()}")
        return {}
    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        log.debug(f"Invalid JSON from mkvmerge for {file_path}")
        return {}

    tracks = payload.get("tracks") or []
    info: Dict[str, str] = {}

    def _extract(stream_type: str) -> Dict[str, str]:
        for t in tracks:
            if (t.get("type") or "").lower() != stream_type:
                continue
            props = t.get("properties") or {}
            codec = t.get("codec") or props.get("codec_id") or ""
            lang = props.get("language") or "und"
            return {
                "codec": codec,
                "lang": lang,
                "default": _bool_flag(props.get("default_track")),
                "forced": _bool_flag(props.get("forced_track")),
            }
        return {}

    v = _extract("video")
    a = _extract("audio")
    if v:
        info["video_codec"] = v.get("codec", "")
        info["video_lang"] = v.get("lang", "und")
        info["video_default"] = v.get("default", "FALSE")
        info["video_forced"] = v.get("forced", "FALSE")
    if a:
        info["audio_codec"] = a.get("codec", "")
        info["audio_lang"] = a.get("lang", "und")
        info["audio_default"] = a.get("default", "FALSE")
        info["audio_forced"] = a.get("forced", "FALSE")
    return info


def _probe_subtitle_stream(path: Path) -> Dict[str, str]:
    """
    Probe subtitle file for codec/lang using mkvmerge (more accurate for subs than ffprobe).
    """

    cmd = ["mkvmerge", "-J", str(path)]
    code, out, err = run_command(cmd, capture=True, stream=False)
    if code != 0 or not out:
        if err:
            log.debug(f"mkvmerge subtitle probe failed for {path}: {err.strip()}")
        return {}

    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        log.debug(f"Invalid JSON from mkvmerge for subtitle {path}")
        return {}

    tracks = payload.get("tracks") or []
    for track in tracks:
        if (track.get("type") or "").lower() not in {"subtitles", "subtitle"}:
            continue
        props = track.get("properties") or {}
        codec_id = track.get("codec") or props.get("codec_id") or ""
        lang = props.get("language_ietf") or props.get("language") or ""
        if not lang:
            name_tokens = _tokenize(props.get("track_name") or track.get("name") or "")
            lang_hint = _extract_lang_hint(reversed(name_tokens))
            if not lang_hint:
                lang_hint = _extract_lang_hint(reversed(_tokenize(path.stem)))
            lang = lang_hint or "und"
        default = _bool_flag(props.get("default_track"))
        forced = _bool_flag(props.get("forced_track"))
        return {"codec": codec_id, "lang": lang, "default": default, "forced": forced}
    return {}


def _build_external_subtitle_rows(
    video_files: List[Path],
    subtitle_files: List[Path],
    *,
    include_video_without_subs: bool = False,
) -> List[Dict[str, str]]:
    """
    Pair non-MKV video files with external subtitle files for later merging.
    Matches if the subtitle filename *contains* the video filename (case-insensitive).
    Produces track-like rows suitable for a merge plan.
    """

    def _lang_from_filename(path: Path) -> str:
        tokens = list(reversed(_tokenize(path.stem)))
        hint = _extract_lang_hint(tokens)
        if hint:
            return hint
        parent_tokens = list(reversed(_tokenize(path.parent.name)))
        hint = _extract_lang_hint(parent_tokens)
        return hint or "und"

    def _subtitle_codec_from_ext(path: Path) -> str:
        ext = path.suffix.lower()
        if ext == ".srt":
            return "SubRip/SRT"
        if ext in {".ass", ".ssa"}:
            return "SubStation Alpha/ASS"
        if ext in {".sub", ".idx"}:
            return "VobSub"
        if ext == ".sup":
            return "PGS"
        if ext == ".vtt":
            return "WebVTT"
        return ext.lstrip(".").upper()

    def _normalize_subtitle_codec(codec_hint: str, path: Path) -> str:
        hint = (codec_hint or "").upper()
        mapping = {
            "S_TEXT/UTF8": "SubRip/SRT",
            "S_TEXT/UTF-8": "SubRip/SRT",
            "S_TEXT/ASS": "SubStation Alpha/ASS",
            "S_TEXT/SSA": "SubStation Alpha/ASS",
            "S_TEXT/USF": "USF",
            "S_VOBSUB": "VobSub",
            "VOBSUB": "VobSub",
            "HDMV PGS": "PGS",
            "PGS": "PGS",
            "WEBVTT": "WebVTT",
        }
        if hint in mapping:
            return mapping[hint]
        return _subtitle_codec_from_ext(path)

    rows: List[Dict[str, str]] = []
    subtitle_candidates = list(subtitle_files)
    subtitle_probe_cache: Dict[Path, Dict[str, str]] = {}

    for video in sorted(video_files, key=lambda p: str(p).lower()):
        video_stem = video.stem.lower()
        matched_subs = [
            sub for sub in subtitle_candidates if _subtitle_matches(video, sub)
        ]
        matched_subs.sort(key=lambda p: (p.parent != video.parent, str(p)))
        if not matched_subs and not include_video_without_subs:
            # Skip videos that have no matching external subtitles; nothing to convert.
            continue

        probe_info = _probe_av_streams(video)
        output_filename = video.with_suffix(".mkv").name
        output_path = str(video.with_suffix(".mkv"))
        input_video_path = str(video)

        video_lang = probe_info.get("video_lang", "und")
        video_codec = probe_info.get("video_codec", "") or ""
        audio_lang = probe_info.get("audio_lang", "und")
        audio_codec = probe_info.get("audio_codec", "") or ""
        video_default = _bool_flag(probe_info.get("video_default"), default=True)
        video_forced = _bool_flag(probe_info.get("video_forced"))
        audio_default = _bool_flag(probe_info.get("audio_default"), default=True)
        audio_forced = _bool_flag(probe_info.get("audio_forced"))

        # video track row
        rows.append(
            {
                "output_filename": output_filename,
                "type": "video",
                "id": "0",
                "name": "",
                "edited_name": video.stem,
                "lang": video_lang,
                "codec": video_codec,
                "default": video_default,
                "forced": video_forced,
                "output_path": output_path,
                "input_path": input_video_path,
            }
        )

        # audio track row (single placeholder from primary audio stream)
        rows.append(
            {
                "output_filename": output_filename,
                "type": "audio",
                "id": "1",
                "name": "",
                "edited_name": f"{audio_lang.upper()} ({audio_codec.upper()})" if audio_codec else audio_lang.upper(),
                "lang": audio_lang,
                "codec": audio_codec,
                "default": audio_default,
                "forced": audio_forced,
                "output_path": output_path,
                "input_path": input_video_path,
            }
        )

        next_id = 2
        for sub in matched_subs:
            if sub not in subtitle_probe_cache:
                subtitle_probe_cache[sub] = _probe_subtitle_stream(sub)
            sub_probe = subtitle_probe_cache.get(sub) or {}
            lang = sub_probe.get("lang") or _lang_from_filename(sub)
            codec = _normalize_subtitle_codec(sub_probe.get("codec", ""), sub)
            sub_default = _bool_flag(sub_probe.get("default"))
            sub_forced = _bool_flag(sub_probe.get("forced"))
            rows.append(
                {
                    "output_filename": output_filename,
                    "type": "subtitles",
                    "id": str(next_id),
                    "name": "",
                    "edited_name": f"{lang.upper()} ({codec})" if codec else lang.upper(),
                    "lang": lang,
                    "codec": codec,
                    "default": sub_default,
                    "forced": sub_forced,
                    "output_path": output_path,
                    "input_path": str(sub),
                }
            )
            next_id += 1

    return rows


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
            "encoding": "",
            "path": str(file_path),
        }

        if track_type == "subtitles":
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
    log.info(
        "Config params: roots=%s, output_dir=%s, output_root=%s, dry_run=%s, batch_size=%s",
        [str(r) for r in candidate_roots],
        output_dir,
        output_root,
        dry_run,
        batch_size,
    )

    primary_root = resolved_roots[0] if resolved_roots else Path.cwd()
    base_output_dir = (
        Path(output_dir)
        if output_dir
        else Path(output_root)
        if output_root
        else primary_root
    )
    base_dir_exists = base_output_dir.exists()
    if not dry_run and not base_dir_exists:
        log.info("Report directory will be created: %s", base_output_dir)
    if not dry_run:
        ensure_dir(base_output_dir)
    writable_target = base_output_dir if base_output_dir.exists() else base_output_dir.parent
    log.info(
        "Report directory: %s (exists=%s, writable=%s)",
        base_output_dir,
        base_dir_exists,
        os.access(writable_target, os.W_OK),
    )
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
    log.info("Excluding output directory from scan: %s", resolved_output_dir)

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
    non_mkv_video_files: List[Path] = []
    subtitle_files: List[Path] = []
    skipped_files: List[Dict[str, str]] = []

    for candidate in Progress(
        _iter_files(resolved_roots, MKV_EXTS, exclude_dir=base_output_dir, include_all=True),
        desc="Probing MKV",
    ):
        suffix = candidate.suffix.lower()
        is_subtitle = suffix in SUBTITLE_EXTS
        is_video = suffix in VIDEO_EXTS

        if is_subtitle:
            subtitle_files.append(candidate)
        if is_video and suffix not in MKV_EXTS:
            non_mkv_video_files.append(candidate)
            # Skip MKV-specific probing but keep the non-MKV video recorded.
            continue
        if not is_video and not is_subtitle:
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
    log.info(
        "Scan counts: mkv=%d, non_mkv_video=%d, subtitle_files=%d, skipped_raw=%d",
        scanned_files,
        len(non_mkv_video_files),
        len(subtitle_files),
        len(skipped_files),
    )
    log.info(f"Probed {scanned_files} MKV files in {elapsed:.2f}s.")

    try:
        normalized_batch = int(batch_size) if batch_size is not None else 0
    except (TypeError, ValueError):
        normalized_batch = 0
    if normalized_batch < 0:
        normalized_batch = 0
    log.info("Batch size normalized to: %d", normalized_batch or 0)

    def _norm_path(val: object) -> str:
        try:
            return str(Path(str(val)).resolve())
        except Exception:
            return str(val)

    # Remove skipped entries that are already represented in other outputs
    reported_paths: set[str] = set()
    reported_paths.update(_norm_path(entry.path) for entry in file_results)
    reported_paths.update(_norm_path(p) for p in non_mkv_video_files)
    reported_paths.update(_norm_path(p) for p in subtitle_files)

    initial_skipped = len(skipped_files)
    filtered_skipped: list[Dict[str, str]] = []
    for row in skipped_files:
        p_norm = _norm_path(row.get("path", ""))
        if p_norm in reported_paths:
            continue
        filtered_skipped.append(row)

    skipped_files = filtered_skipped
    log.info(
        "Skipped files filtered: before=%d, after=%d",
        initial_skipped,
        len(skipped_files),
    )

    non_hevc_rows.sort(key=lambda row: row["path"])
    failed_files.sort(key=lambda row: row["path"])
    skipped_files.sort(key=lambda row: row["path"])
    log.info("Non-HEVC MKV files detected: %d", len(non_hevc_rows))
    log.info("MKV scan failures: %d", len(failed_files))

    external_subtitle_rows = _build_external_subtitle_rows(non_mkv_video_files, subtitle_files)
    non_mkv_scan_rows = _build_external_subtitle_rows(
        non_mkv_video_files,
        subtitle_files,
        include_video_without_subs=True,
    )
    mkv_ext_subtitle_rows = _build_external_subtitle_rows(
        [entry.path for entry in file_results if entry.track_rows],
        subtitle_files,
    )

    def _norm_variants(val: object) -> set[str]:
        variants: set[str] = set()
        try:
            p = Path(str(val))
        except Exception:
            variants.add(str(val))
            return variants
        candidates = [p, p.expanduser()]
        try:
            candidates.append(p.resolve())
        except Exception:
            pass
        for cand in candidates:
            variants.add(str(cand))
            variants.add(cand.as_posix())
            variants.add(str(cand).lower())
            variants.add(cand.as_posix().lower())
        return {v for v in variants if v}

    matched_subtitle_paths: set[str] = set()
    for row in list(external_subtitle_rows) + list(mkv_ext_subtitle_rows):
        if (row.get("type") or "").lower() != "subtitles":
            continue
        ipath = row.get("input_path")
        if not ipath:
            continue
        matched_subtitle_paths.update(_norm_variants(ipath))

    unmatched_subtitle_files = []
    for p in subtitle_files:
        variants = _norm_variants(p)
        if matched_subtitle_paths.intersection(variants):
            continue
        if any(_subtitle_matches(video, p) for video in non_mkv_video_files):
            continue
        if any(_subtitle_matches(entry.path, p) for entry in file_results if entry.track_rows):
            continue
        unmatched_subtitle_files.append(str(p))
    log.info(
        "External subtitles: unmatched=%d, matched_mkv_files=%d, matched_non_mkv_files=%d",
        len(unmatched_subtitle_files),
        len({row.get("output_path") for row in mkv_ext_subtitle_rows if row.get("type") == "video"}),
        len({row.get("output_path") for row in external_subtitle_rows if row.get("type") == "video"}),
    )
    if normalized_batch <= 0:
        external_subtitle_chunks: List[List[Dict[str, str]]] = (
            [external_subtitle_rows] if external_subtitle_rows else []
        )
    else:
        external_subtitle_chunks = [
            external_subtitle_rows[i : i + normalized_batch]
            for i in range(0, len(external_subtitle_rows), normalized_batch)
        ]

    if normalized_batch <= 0:
        mkv_ext_subtitle_chunks: List[List[Dict[str, str]]] = (
            [mkv_ext_subtitle_rows] if mkv_ext_subtitle_rows else []
        )
    else:
        mkv_ext_subtitle_chunks = [
            mkv_ext_subtitle_rows[i : i + normalized_batch]
            for i in range(0, len(mkv_ext_subtitle_rows), normalized_batch)
        ]

    if normalized_batch <= 0:
        non_mkv_issue_chunks: List[List[Dict[str, str]]] = []
        non_mkv_ok_chunks: List[List[Dict[str, str]]] = []
    else:
        non_mkv_issue_chunks = []
        non_mkv_ok_chunks = []

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

    def _normalize_lang_list(raw_value: object, default: list[str]) -> list[str]:
        if raw_value is None:
            raw_value = default
        return [str(s).strip().lower() for s in (raw_value or []) if s]

    allowed_vid = _normalize_lang_list(task_conf.get("lang_vid"), ["eng"])
    allowed_aud = _normalize_lang_list(task_conf.get("lang_aud"), ["eng"])
    allowed_sub = _normalize_lang_list(task_conf.get("lang_sub"), ["eng"])
    log.info(
        "Language whitelist config: video=%s, audio=%s, subtitles=%s",
        allowed_vid if allowed_vid else "(any)",
        allowed_aud if allowed_aud else "(any)",
        allowed_sub if allowed_sub else "(any)",
    )

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
    actual_langs: dict[str, dict[str, Set[str]]] = {
        "video": {},
        "audio": {},
        "subtitles": {},
    }

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
            rlang = (r.get("lang") or "").strip() or "und"
            actual_langs.setdefault(rtype, {}).setdefault(fname, set()).add(rlang)
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
    log.info(
        "Language/structure check: ok=%d, issues=%d",
        len(ok_files),
        len(issues_files),
    )
    more_than_one_video = sum(1 for c in video_counts.values() if c > 1)
    more_than_one_audio = sum(1 for c in audio_counts.values() if c > 1)
    more_than_one_subs = sum(1 for c in subtitle_counts.values() if c > 1)
    zero_video = sum(1 for c in video_counts.values() if c == 0)
    zero_audio = sum(1 for c in audio_counts.values() if c == 0)
    zero_subs = sum(1 for c in subtitle_counts.values() if c == 0)
    log.info(
        "MKV mismatch breakdown: >1 video=%d, >1 audio=%d, >1 subs=%d, zero video=%d, zero audio=%d, zero subs=%d, lang_issues=%d, matched=%d",
        more_than_one_video,
        more_than_one_audio,
        more_than_one_subs,
        zero_video,
        zero_audio,
        zero_subs,
        len(bad_lang_vid | bad_lang_aud | bad_lang_sub),
        len(ok_files),
    )

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

    # Non-MKV classification (language + structural) based on probed rows
    non_mkv_video_counts: dict[str, int] = {}
    non_mkv_audio_counts: dict[str, int] = {}
    non_mkv_subtitle_counts: dict[str, int] = {}
    non_mkv_langs: dict[str, dict[str, Set[str]]] = {"video": {}, "audio": {}, "subtitles": {}}
    bad_lang_vid_nm: set[str] = set()
    bad_lang_aud_nm: set[str] = set()
    bad_lang_sub_nm: set[str] = set()
    nm_rows_by_file: dict[str, list[dict[str, str]]] = {}

    def _nm_key(row: dict[str, str]) -> str:
        return (
            row.get("output_filename")
            or Path(row.get("output_path", "")).name
            or Path(row.get("input_path", "")).name
        )

    for row in non_mkv_scan_rows:
        fname = _nm_key(row)
        if not fname:
            continue
        nm_rows_by_file.setdefault(fname, []).append(row)
        rtype = (row.get("type") or "").lower()
        lang_val = (row.get("lang") or "").strip() or "und"

        if rtype == "video":
            non_mkv_video_counts[fname] = non_mkv_video_counts.get(fname, 0) + 1
        elif rtype == "audio":
            non_mkv_audio_counts[fname] = non_mkv_audio_counts.get(fname, 0) + 1
        elif rtype == "subtitles":
            non_mkv_subtitle_counts[fname] = non_mkv_subtitle_counts.get(fname, 0) + 1

        non_mkv_langs.setdefault(rtype, {}).setdefault(fname, set()).add(lang_val)
        if rtype == "video" and not _lang_matches(lang_val, allowed_vid):
            bad_lang_vid_nm.add(fname)
        if rtype == "audio" and not _lang_matches(lang_val, allowed_aud):
            bad_lang_aud_nm.add(fname)
        if rtype == "subtitles" and not _lang_matches(lang_val, allowed_sub):
            bad_lang_sub_nm.add(fname)

    nm_all_names = sorted(nm_rows_by_file.keys())
    non_mkv_issues_set: set[str] = set()
    for fname in nm_all_names:
        v = non_mkv_video_counts.get(fname, 0)
        a = non_mkv_audio_counts.get(fname, 0)
        s = non_mkv_subtitle_counts.get(fname, 0)
        lang_issue = (
            fname in bad_lang_vid_nm or fname in bad_lang_aud_nm or fname in bad_lang_sub_nm
        )
        if v > 1 or a > 1 or s > 1 or v == 0 or a == 0 or s == 0 or lang_issue:
            non_mkv_issues_set.add(fname)
    log.info(
        "Non-MKV classification: ok=%d, issues=%d",
        len([name for name in nm_all_names if name not in non_mkv_issues_set]),
        len(non_mkv_issues_set),
    )

    def _chunk_nm(names: List[str]) -> List[List[Dict[str, str]]]:
        if not names:
            return []
        sorted_names = sorted(names)
        if normalized_batch <= 0:
            combined: List[Dict[str, str]] = []
            for fname in sorted_names:
                rows = nm_rows_by_file.get(fname, [])
                rows.sort(key=lambda row: (row.get("output_path", ""), row.get("type", ""), row.get("id", "")))
                combined.extend(rows)
            return [combined] if combined else []
        chunks: List[List[Dict[str, str]]] = []
        for i in range(0, len(sorted_names), normalized_batch):
            chunk_rows: List[Dict[str, str]] = []
            for fname in sorted_names[i : i + normalized_batch]:
                rows = nm_rows_by_file.get(fname, [])
                rows.sort(key=lambda row: (row.get("output_path", ""), row.get("type", ""), row.get("id", "")))
                chunk_rows.extend(rows)
            if chunk_rows:
                chunks.append(chunk_rows)
        return chunks

    non_mkv_ok_chunks = _chunk_nm([name for name in nm_all_names if name not in non_mkv_issues_set])
    non_mkv_issue_chunks = _chunk_nm([name for name in nm_all_names if name in non_mkv_issues_set])

    non_mkv_non_hevc_rows: List[Dict[str, object]] = []
    for fname, rows in nm_rows_by_file.items():
        video_rows = [r for r in rows if (r.get("type") or "").lower() == "video"]
        codecs = {(r.get("codec") or "").strip() for r in video_rows if (r.get("codec") or "").strip()}
        if codecs and not any("hevc" in c.lower() for c in codecs):
            src_path = ""
            if video_rows:
                src_path = video_rows[0].get("input_path", "") or video_rows[0].get("output_path", "")
            non_mkv_non_hevc_rows.append(
                {
                    "filename": Path(src_path).name if src_path else fname,
                    "codecs": ", ".join(sorted(codecs)),
                    "path": src_path or fname,
                }
            )

    if normalized_batch <= 0:
        non_mkv_non_hevc_chunks: List[List[Dict[str, object]]] = (
            [non_mkv_non_hevc_rows] if non_mkv_non_hevc_rows else []
        )
    else:
        non_mkv_non_hevc_chunks = [
            non_mkv_non_hevc_rows[i : i + normalized_batch]
            for i in range(0, len(non_mkv_non_hevc_rows), normalized_batch)
        ]
    non_mkv_ok_total = sum(len(chunk) for chunk in non_mkv_ok_chunks)
    non_mkv_issue_total = sum(len(chunk) for chunk in non_mkv_issue_chunks)
    log.info(
        "Non-MKV row totals: ok_rows=%d, issue_rows=%d, non_hevc_rows=%d",
        non_mkv_ok_total,
        non_mkv_issue_total,
        len(non_mkv_non_hevc_rows),
    )

    written_reports: Dict[str, Dict[str, object]] = {}

    # XLS styling removed; reports are CSV-only.
    
    if external_subtitle_chunks and write_csv_file:
        log.info("Writing scan_non_mkv_ext_subs CSV (rows=%d)", len(external_subtitle_rows))
        external_result = write_tabular_reports(
            external_subtitle_chunks,
            "scan_non_mkv_ext_subs",
            EXTERNAL_SUB_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["non_mkv_ext_subs"] = {
            "paths": external_result.csv_paths,
            "rows": len(external_subtitle_rows),
        }

    if mkv_ext_subtitle_chunks and write_csv_file:
        log.info("Writing scan_mkv_ext_subs CSV (rows=%d)", len(mkv_ext_subtitle_rows))
        mkv_external_result = write_tabular_reports(
            mkv_ext_subtitle_chunks,
            "scan_mkv_ext_subs",
            EXTERNAL_SUB_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_ext_subs"] = {
            "paths": mkv_external_result.csv_paths,
            "rows": len(mkv_ext_subtitle_rows),
        }

    if non_mkv_issue_chunks and write_csv_file:
        log.info(
            "Writing scan_non_mkv_issues CSV (rows=%d)",
            sum(len(chunk) for chunk in non_mkv_issue_chunks),
        )
        nm_issues_result = write_tabular_reports(
            non_mkv_issue_chunks,
            "scan_non_mkv_issues",
            EXTERNAL_SUB_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["non_mkv_issues"] = {
            "paths": nm_issues_result.csv_paths,
            "rows": sum(len(chunk) for chunk in non_mkv_issue_chunks),
        }

    if non_mkv_ok_chunks and write_csv_file:
        log.info(
            "Writing scan_non_mkv_ok CSV (rows=%d)",
            sum(len(chunk) for chunk in non_mkv_ok_chunks),
        )
        nm_ok_result = write_tabular_reports(
            non_mkv_ok_chunks,
            "scan_non_mkv_ok",
            EXTERNAL_SUB_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["non_mkv_ok"] = {
            "paths": nm_ok_result.csv_paths,
            "rows": sum(len(chunk) for chunk in non_mkv_ok_chunks),
        }

    if non_mkv_non_hevc_rows and write_csv_file:
        log.info("Writing scan_non_mkv_non_hevc CSV (rows=%d)", len(non_mkv_non_hevc_rows))
        nm_non_hevc_result = write_tabular_reports(
            non_mkv_non_hevc_chunks,
            "scan_non_mkv_non_hevc",
            NON_HEVC_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["non_mkv_non_hevc"] = {
            "paths": nm_non_hevc_result.csv_paths,
            "rows": len(non_mkv_non_hevc_rows),
        }

    # Write split track CSVs: issues and ok. Each group's batching is handled
    # independently (we already built ok_track_chunk_rows and
    # issues_track_chunk_rows above).
    if issues_track_chunk_rows and write_csv_file:
        log.info(
            "Writing scan_mkv_issues CSV (rows=%d)",
            sum(len(chunk) for chunk in issues_track_chunk_rows),
        )
        issues_result = write_tabular_reports(
            issues_track_chunk_rows,
            "scan_mkv_issues",
            TRACK_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_issues"] = {
            "paths": issues_result.csv_paths,
            "rows": sum(len(chunk) for chunk in issues_track_chunk_rows),
        }

    if ok_track_chunk_rows and write_csv_file:
        log.info(
            "Writing scan_mkv_ok CSV (rows=%d)",
            sum(len(chunk) for chunk in ok_track_chunk_rows),
        )
        ok_result = write_tabular_reports(
            ok_track_chunk_rows,
            "scan_mkv_ok",
            TRACK_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_ok"] = {
            "paths": ok_result.csv_paths,
            "rows": sum(len(chunk) for chunk in ok_track_chunk_rows),
        }

    if name_list_chunk_rows and write_csv_file:
        log.info(
            "Writing mkv_scan_name_list CSV (rows=%d)",
            sum(len(chunk) for chunk in name_list_chunk_rows),
        )
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
        log.info("Writing scan_mkv_non_hevc CSV (rows=%d)", len(non_hevc_rows))
        non_hevc_result = write_tabular_reports(
            non_hevc_chunk_rows if non_hevc_chunk_rows else [non_hevc_rows],
            "scan_mkv_non_hevc",
            NON_HEVC_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_non_hevc"] = {
            "paths": non_hevc_result.csv_paths,
            "rows": len(non_hevc_rows),
        }

    if failed_files and write_csv_file:
        log.info("Writing scan_mkv_failures CSV (rows=%d)", len(failed_files))
        failure_result = write_tabular_reports(
            failure_chunk_rows if failure_chunk_rows else [failed_files],
            "scan_mkv_failures",
            FAILURE_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_failures"] = {
            "paths": failure_result.csv_paths,
            "rows": len(failed_files),
        }

    if skipped_files and write_csv_file:
        log.info("Writing scan_mkv_skipped CSV (rows=%d)", len(skipped_files))
        skipped_result = write_tabular_reports(
            [skipped_files],
            "scan_mkv_skipped",
            SKIPPED_COLUMNS,
            output_dir=base_output_dir,
            dry_run=dry_run,
        )
        written_reports["mkv_skipped"] = {
            "paths": skipped_result.csv_paths,
            "rows": len(skipped_files),
        }
        if skipped_result.csv_paths:
            written_reports["mkv_skipped"]["csv_paths"] = skipped_result.csv_paths
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
        "mkv_issues": (
            int(written_reports.get("mkv_issues", {}).get("rows", 0)),
            written_reports.get("mkv_issues", {}).get("paths", []),
        ),
        "mkv_ok": (
            int(written_reports.get("mkv_ok", {}).get("rows", 0)),
            written_reports.get("mkv_ok", {}).get("paths", []),
        ),
        "mkv_non_hevc": (
            int(written_reports.get("mkv_non_hevc", {}).get("rows", 0)),
            written_reports.get("mkv_non_hevc", {}).get("paths", []),
        ),
        "mkv_failures": (
            int(written_reports.get("mkv_failures", {}).get("rows", 0)),
            written_reports.get("mkv_failures", {}).get("paths", []),
        ),
        "mkv_skipped": (
            int(written_reports.get("mkv_skipped", {}).get("rows", 0)),
            written_reports.get("mkv_skipped", {}).get("paths", []),
        ),
        "name_list": (
            unique_name_list_count,
            written_reports.get("name_list", {}).get("paths", []),
        ),
        "non_mkv_issues": (
            int(written_reports.get("non_mkv_issues", {}).get("rows", 0)),
            written_reports.get("non_mkv_issues", {}).get("paths", []),
        ),
        "non_mkv_ok": (
            int(written_reports.get("non_mkv_ok", {}).get("rows", 0)),
            written_reports.get("non_mkv_ok", {}).get("paths", []),
        ),
        "non_mkv_non_hevc": (
            int(written_reports.get("non_mkv_non_hevc", {}).get("rows", 0)),
            written_reports.get("non_mkv_non_hevc", {}).get("paths", []),
        ),
        "non_mkv_ext_subs": (
            len(external_subtitle_rows),
            written_reports.get("non_mkv_ext_subs", {}).get("paths", []),
        ),
        "mkv_ext_subs": (
            len(mkv_ext_subtitle_rows),
            written_reports.get("mkv_ext_subs", {}).get("paths", []),
        ),
    }

    for label, (count, paths) in report_counts.items():
        title = label.replace("_", " ").title()
        if count <= 0:
            log.info("%s report skipped — no rows captured.", title)
            continue

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
        summary_path = timestamped_filename("scan_summary", "txt", base_output_dir)
        with open_file(summary_path, "w") as out:
            # ANSI color codes
            RESET = "\x1b[0m"
            BOLD = "\x1b[1m"
            CYAN = "\x1b[36m"
            YELLOW = "\x1b[33m"
            RED = "\x1b[31m"
            GREEN = "\x1b[32m"

            # Header
            out.write(f"{BOLD}{CYAN}📋 Scan Summary{RESET}\n")
            out.write(f"{BOLD}Generated:{RESET} " + summary_path.name + "\n\n")

            # Totals line covering all report types
            totals_line = (
                f"{BOLD}{CYAN}Totals:{RESET} "
                f"files={scanned_files}, "
                f"tracks={len(track_rows)}, "
                f"non_hevc={len(non_hevc_rows)}, "
                f"failures={len(failed_files)}, "
                f"skipped={len(skipped_files)}, "
                f"non_mkv_ext_subs={len(external_subtitle_rows)}, "
                f"mkv_ext_subs={len(mkv_ext_subtitle_rows)}, "
                f"non_mkv_non_hevc={len(non_mkv_non_hevc_rows)}"
            )
            out.write(totals_line + "\n\n")

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
            html_path = timestamped_filename("scan_summary", "html", base_output_dir)
            html_parts: list[str] = []
            # Precompute non-MKV summary rows (from non-MKV scan rows)
            subtitle_rows_nm = [
                r for r in non_mkv_scan_rows if (r.get("type") or "").lower() == "subtitles"
            ]
            video_rows_nm = [
                r for r in non_mkv_scan_rows if (r.get("type") or "").lower() == "video"
            ]
            audio_rows_nm = [
                r for r in non_mkv_scan_rows if (r.get("type") or "").lower() == "audio"
            ]

            nm_stats: dict[str, dict[str, object]] = {}
            for r in non_mkv_scan_rows:
                out_name = r.get("output_filename", "")
                t = (r.get("type") or "").lower()
                if not out_name or not t:
                    continue
                entry = nm_stats.setdefault(
                    out_name,
                    {
                        "video": 0,
                        "audio": 0,
                        "subtitles": 0,
                        "langs": {"video": set(), "audio": set(), "subtitles": set()},
                        "source_path": "",
                    },
                )
                entry[t] = int(entry[t]) + 1
                lang_val = (r.get("lang") or "und").strip() or "und"
                entry["langs"].setdefault(t, set()).add(lang_val)
                if t == "video" and r.get("input_path"):
                    entry["source_path"] = r.get("input_path", "")

            more_than_1_video_nm: list[tuple[str, int]] = []
            more_than_1_audio_nm: list[tuple[str, int]] = []
            more_than_1_subtitle_nm: list[tuple[str, int]] = []
            no_video_nm: list[str] = []
            no_audio_nm: list[str] = []
            no_subtitles_nm: list[str] = []
            bad_lang_vid_nm: list[str] = []
            bad_lang_aud_nm: list[str] = []
            bad_lang_sub_nm: list[str] = []
            summary_rows_nm: list[tuple[str, int, int, int, str, str]] = []

            for out_name, info in nm_stats.items():
                v = int(info.get("video") or 0)
                a = int(info.get("audio") or 0)
                s = int(info.get("subtitles") or 0)
                langs_map: dict[str, set[str]] = info.get("langs", {})  # type: ignore
                if v > 1:
                    more_than_1_video_nm.append((out_name, v))
                if a > 1:
                    more_than_1_audio_nm.append((out_name, a))
                if s > 1:
                    more_than_1_subtitle_nm.append((out_name, s))
                if v == 0:
                    no_video_nm.append(out_name)
                if a == 0:
                    no_audio_nm.append(out_name)
                if s == 0:
                    no_subtitles_nm.append(out_name)

                lang_flags: list[str] = []
                if v > 0:
                    for lang_val in langs_map.get("video", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_vid):
                            bad_lang_vid_nm.append(out_name)
                            lang_flags.append("vid")
                            break
                if a > 0:
                    for lang_val in langs_map.get("audio", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_aud):
                            bad_lang_aud_nm.append(out_name)
                            lang_flags.append("aud")
                            break
                if s > 0:
                    for lang_val in langs_map.get("subtitles", set()) or {"und"}:
                        if not _lang_matches(lang_val, allowed_sub):
                            bad_lang_sub_nm.append(out_name)
                            lang_flags.append("sub")
                            break

                summary_rows_nm.append(
                    (
                        out_name,
                        v,
                        a,
                        s,
                        ",".join(lang_flags),
                        info.get("source_path", ""),
                    )
                )

            def _html_table_block(title: str, headers: list[str], rows: list[list[str]]) -> str:
                if not rows:
                    return "<p><em>None</em></p>"
                out = ["<table class=\"tt-table\">", "<thead><tr>"]
                for h in headers:
                    out.append(f"<th>{h}</th>")
                out.append("</tr></thead><tbody>")
                for row in rows:
                    out.append("<tr>")
                    for cell in row:
                        out.append(f"<td>{cell}</td>")
                    out.append("</tr>")
                out.append("</tbody></table>")
                return "".join(out)

            def _wrap_details(title: str, body: str, *, has_data: bool, open_state: bool = False) -> str:
                cls = "tt-details has-data" if has_data else "tt-details no-data"
                return f"<details class=\"{cls}\"{' open' if open_state else ''}><summary>{title}</summary>{body}</details>"

            def _wrap_subsection(title: str, body: str, *, has_data: bool, open_state: bool = False) -> str:
                cls = "tt-subdetails has-data" if has_data else "tt-subdetails no-data"
                return f"<details class=\"{cls}\"{' open' if open_state else ''}><summary>{title}</summary>{body}</details>"

            def _table_from_pairs(title: str, pairs: list[tuple[str, int]]) -> str:
                rows = [[fn, str(cnt)] for fn, cnt in pairs]
                return _html_table_block(title, ["filename", "count"], rows)

            def _lang_table(title: str, target: list[str], key: str, expected: str) -> str:
                lang_rows: list[list[str]] = []
                for fn in target:
                    langs = sorted(actual_langs.get(key, {}).get(fn, set()) or {"und"})
                    lang_rows.append([fn, expected, ", ".join(langs)])
                return _html_table_block(title, ["filename", "expected_lang", "actual_lang"], lang_rows)

            def _list_table(title: str, items: list[str]) -> str:
                rows = [[fn] for fn in items]
                return _html_table_block(title, ["filename"], rows)

            def _lang_table_nm(title: str, target: list[str], key: str, expected: str) -> str:
                lang_rows: list[list[str]] = []
                for fn in target:
                    langs = sorted((nm_stats.get(fn, {}).get("langs", {}) or {}).get(key, set()) or {"und"})
                    lang_rows.append([fn, expected, ", ".join(langs)])
                return _html_table_block(title, ["output_filename", "expected_lang", "actual_lang"], lang_rows)

            html_parts.append("<!doctype html>")
            html_parts.append("<html><head><meta charset=\"utf-8\"><title>MKV Scan Tracks Summary</title>")
            html_parts.append(
                "<style>"
                "body{font-family:'Segoe UI',Helvetica,Arial,sans-serif;background:#f8fbff;color:#1a1d21;padding:18px;line-height:1.5;}"
                "h1{font-size:1.6rem;margin:0 0 8px;font-weight:700;color:#0b5ed7;}"
                "h2{font-size:1.2rem;margin:16px 0 8px;font-weight:700;color:#0f5132;}"
                "h3{font-size:1rem;margin:12px 0 6px;font-weight:700;color:#0b5ed7;}"
                "h4{font-size:0.95rem;margin:10px 0 6px;font-weight:700;color:#495057;}"
                ".summary-bar{margin:10px 0 14px;padding:10px 12px;background:#e7f1ff;border:1px solid #cfe2ff;border-radius:8px;font-size:0.95rem;}"
                ".summary-bar strong{color:#0b5ed7;}"
                ".tt-table{border-collapse:collapse;width:100%;margin:6px 0 12px;background:#fff;}"
                ".tt-table th,.tt-table td{border:1px solid #dee2e6;padding:6px 8px;font-size:0.9rem;}"
                ".tt-table thead tr{background:linear-gradient(90deg,#0b5ed7,#4e8df7);color:#fff;}"
                ".tt-table th{color:#fff;text-align:left;}"
                ".tt-table tr:nth-child(even){background:#f8f9fa;}"
                ".tt-table tr.warn td{background:#fff3cd;}"
                ".tt-details{border:1px solid #ced4da;border-radius:8px;padding:6px 10px;margin:10px 0;background:#fff;}"
                ".tt-details.has-data > summary{background:linear-gradient(90deg,#d0f0d0,#e8f7e8);border:1px solid #b2dfb2;}"
                ".tt-details.no-data > summary{background:linear-gradient(90deg,#f8f9fa,#eef2ff);border:1px solid #d1d5db;}"
                ".tt-details > summary{cursor:pointer;font-weight:700;font-size:1rem;color:#0b5ed7;padding:6px 8px;border-radius:6px;}"
                ".tt-subdetails{margin:8px 0;border:1px solid #e9ecef;border-radius:6px;padding:4px 6px;background:#fdfdff;}"
                ".tt-subdetails.has-data > summary{background:linear-gradient(90deg,#e6f4ea,#f1faf3);border:1px solid #b2dfb2;}"
                ".tt-subdetails.no-data > summary{background:linear-gradient(90deg,#f8f9fa,#f1f3f5);border:1px solid #d1d5db;}"
                ".tt-subdetails summary{cursor:pointer;font-weight:600;font-size:0.95rem;color:#495057;padding:4px 6px;border-radius:4px;}"
                "a{color:#0b5ed7;text-decoration:none;}a:hover{text-decoration:underline;}"
                "</style>"
            )
            html_parts.append("</head><body>")
            html_parts.append(f"<h1>📋 Scan Summary</h1><p><strong>Generated:</strong> {html_path.name}</p>")

            totals_html = (
                f"<div class=\"summary-bar\">"
                f"🎞️ Files scanned: <strong>{scanned_files}</strong> &nbsp; "
                f"🎚️ Tracks: <strong>{len(track_rows)}</strong> &nbsp; "
                f"🧊 Non-HEVC: <strong>{len(non_hevc_rows)}</strong> &nbsp; "
                f"⚠️ Failures: <strong>{len(failed_files)}</strong> &nbsp; "
                f"⏭️ Skipped: <strong>{len(skipped_files)}</strong> &nbsp; "
                f"🧩 Non-MKV ext subs rows: <strong>{len(external_subtitle_rows)}</strong> &nbsp; "
                f"🧩 MKV ext subs rows: <strong>{len(mkv_ext_subtitle_rows)}</strong> &nbsp; "
                f"🧊 Non-MKV non-HEVC: <strong>{len(non_mkv_non_hevc_rows)}</strong>"
                f"</div>"
            )
            html_parts.append(totals_html)

            expected_vid = ", ".join(allowed_vid) if allowed_vid else "(any)"
            expected_aud = ", ".join(allowed_aud) if allowed_aud else "(any)"
            expected_sub = ", ".join(allowed_sub) if allowed_sub else "(any)"

            # MKV section
            mkv_body_parts: list[str] = []
            mkv_body_parts.append(
                _wrap_subsection(
                    "📄 Per-file summary",
                    _html_table_block(
                        "📄 Per-file summary",
                        ["filename", "video", "audio", "subtitles", "lang_issues"],
                        [
                            [fn, str(v), str(a), str(s), lang_flags]
                            for fn, v, a, s, lang_flags in summary_rows
                        ],
                    ),
                    has_data=bool(summary_rows),
                    open_state=True,
                )
            )
            mkv_body_parts.append(
                _wrap_subsection("🎞️ more_than_1_video", _table_from_pairs("🎞️ more_than_1_video", more_than_1_video), has_data=bool(more_than_1_video))
            )
            mkv_body_parts.append(
                _wrap_subsection("🔊 more_than_1_audio", _table_from_pairs("🔊 more_than_1_audio", more_than_1_audio), has_data=bool(more_than_1_audio))
            )
            mkv_body_parts.append(
                _wrap_subsection("📝 more_than_1_subtitle", _table_from_pairs("📝 more_than_1_subtitle", more_than_1_subtitle), has_data=bool(more_than_1_subtitle))
            )
            mkv_body_parts.append(_wrap_subsection("🚫 no_video", _list_table("🚫 no_video", no_video), has_data=bool(no_video)))
            mkv_body_parts.append(_wrap_subsection("🔇 no_audio", _list_table("🔇 no_audio", no_audio), has_data=bool(no_audio)))
            mkv_body_parts.append(_wrap_subsection("🈚 no_subtitles", _list_table("🈚 no_subtitles", no_subtitles), has_data=bool(no_subtitles)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - video", _lang_table("⚠️ lang mismatch - video", bad_vid, "video", expected_vid), has_data=bool(bad_vid)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - audio", _lang_table("⚠️ lang mismatch - audio", bad_aud, "audio", expected_aud), has_data=bool(bad_aud)))
            mkv_body_parts.append(_wrap_subsection("⚠️ lang mismatch - subtitles", _lang_table("⚠️ lang mismatch - subtitles", bad_sub, "subtitles", expected_sub), has_data=bool(bad_sub)))

            mkv_has_data = any(
                [
                    summary_rows,
                    more_than_1_video,
                    more_than_1_audio,
                    more_than_1_subtitle,
                    no_video,
                    no_audio,
                    no_subtitles,
                    bad_vid,
                    bad_aud,
                    bad_sub,
                ]
            )

            html_parts.append(
                _wrap_details("🎬 MKV files", "".join(mkv_body_parts), has_data=mkv_has_data, open_state=True)
            )

            # Non-MKV section
            non_mkv_body: list[str] = []
            non_mkv_body.append(
                _wrap_subsection(
                    "📄 Per-file summary",
                    _html_table_block(
                        "📄 Per-file summary",
                        ["output_filename", "video", "audio", "subtitles", "lang_issues", "source_path"],
                        [
                            [fn, str(v), str(a), str(s), lang_flags, src]
                            for fn, v, a, s, lang_flags, src in sorted(summary_rows_nm, key=lambda r: r[0])
                        ],
                    ),
                    has_data=bool(summary_rows_nm),
                    open_state=True,
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🎞️ more_than_1_video",
                    _table_from_pairs("🎞️ more_than_1_video", sorted(more_than_1_video_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_video_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🔊 more_than_1_audio",
                    _table_from_pairs("🔊 more_than_1_audio", sorted(more_than_1_audio_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_audio_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "📝 more_than_1_subtitle",
                    _table_from_pairs("📝 more_than_1_subtitle", sorted(more_than_1_subtitle_nm, key=lambda r: r[0])),
                    has_data=bool(more_than_1_subtitle_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🚫 no_video",
                    _list_table("🚫 no_video", sorted(no_video_nm)),
                    has_data=bool(no_video_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🔇 no_audio",
                    _list_table("🔇 no_audio", sorted(no_audio_nm)),
                    has_data=bool(no_audio_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "🈚 no_subtitles",
                    _list_table("🈚 no_subtitles", sorted(no_subtitles_nm)),
                    has_data=bool(no_subtitles_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - video",
                    _lang_table_nm("⚠️ lang mismatch - video", sorted(set(bad_lang_vid_nm)), "video", expected_vid),
                    has_data=bool(bad_lang_vid_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - audio",
                    _lang_table_nm("⚠️ lang mismatch - audio", sorted(set(bad_lang_aud_nm)), "audio", expected_aud),
                    has_data=bool(bad_lang_aud_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "⚠️ lang mismatch - subtitles",
                    _lang_table_nm("⚠️ lang mismatch - subtitles", sorted(set(bad_lang_sub_nm)), "subtitles", expected_sub),
                    has_data=bool(bad_lang_sub_nm),
                )
            )
            non_mkv_body.append(
                _wrap_subsection(
                    "📁 non_video_files",
                    _html_table_block(
                        "📁 non_video_files",
                        ["filename", "path"],
                        [
                            [Path(p).name, p]
                            for p in sorted(unmatched_subtitle_files)
                        ],
                    ),
                    has_data=bool(unmatched_subtitle_files),
                )
            )

            non_mkv_has_data = bool(
                summary_rows_nm
                or more_than_1_video_nm
                or more_than_1_audio_nm
                or more_than_1_subtitle_nm
                or no_video_nm
                or no_audio_nm
                or no_subtitles_nm
                or bad_lang_vid_nm
                or bad_lang_aud_nm
                or bad_lang_sub_nm
                or unmatched_subtitle_files
            )

            html_parts.append(
                _wrap_details("🧩 Non-MKV files", "".join(non_mkv_body), has_data=non_mkv_has_data, open_state=True)
            )

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
