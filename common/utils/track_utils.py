"""
Track extraction and normalization helpers shared across video tooling.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def flag_string(val: object) -> str:
    """Normalize mkvmerge boolean-ish values to yes/no strings."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "yes" if val else "no"
    if isinstance(val, (int, float)):
        return "yes" if val != 0 else "no"
    text = str(val).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return "yes"
    if text in {"0", "false", "no", "n", "off"}:
        return "no"
    return ""


def extract_tracks(path: Path, payload: dict) -> List[Dict[str, str]]:
    """
    Convert mkvmerge JSON payload into normalized track rows.
    """
    tracks = payload.get("tracks") or []
    rows: List[Dict[str, str]] = []
    for t in tracks:
        ttype = str(t.get("type") or "").lower()
        props = t.get("properties") or {}
        raw_id = t.get("id")
        raw_number = props.get("number")
        track_id = raw_id if raw_id is not None else raw_number
        name_val = props.get("track_name") or t.get("name") or ""
        lang_val = props.get("language") or props.get("language_ietf") or "und"
        codec_val = t.get("codec") or props.get("codec_id") or ""
        default_val = (
            props.get("default_track", None)
            if "default_track" in props
            else t.get("default_track", None)
        )
        if default_val is None:
            default_val = props.get("flag-default", None) or t.get("flag-default", None)
        if default_val is None:
            default_val = props.get("flag_default", None) or t.get("flag_default", None)
        forced_val = (
            props.get("forced_track", None)
            if "forced_track" in props
            else t.get("forced_track", None)
        )
        if forced_val is None:
            forced_val = props.get("flag-forced", None) or t.get("flag-forced", None)
        if forced_val is None:
            forced_val = props.get("flag_forced", None) or t.get("flag_forced", None)
        encoding_val = ""
        if ttype == "subtitles":
            # For subtitle tracks in external subtitle files (non-video), enforce defaults;
            # for embedded subtitles in videos, keep mkvmerge-reported flags.
            is_external_sub = path.suffix.lower() not in {".mkv", ".mp4", ".mov", ".avi", ".wmv", ".flv", ".webm", ".m4v", ".ts", ".m2ts"}
            if is_external_sub:
                default_val = True
                forced_val = False
                encoding_val = "UTF-8"
            else:
                encoding_val = props.get("encoding") or props.get("codec_private_data") or ""
        edited_name = ""
        if ttype == "video":
            edited_name = path.stem
        elif ttype in {"audio", "subtitles"}:
            lang_upper = str(lang_val).upper()
            edited_name = f"{lang_upper} ({codec_val})" if codec_val else lang_upper

        row = {
            "filename": path.name,
            "output_filename": path.with_suffix(".mkv").name,
            "output_path": str(path.with_suffix(".mkv")),
            "input_path": str(path),
            "type": ttype,
            "id": str(track_id) if track_id is not None else "",
            "name": str(name_val),
            "edited_name": edited_name,
            "lang": str(lang_val).strip() or "und",
            "codec": str(codec_val),
            "default": "yes",
            "forced": flag_string(forced_val),
            # encoding is meaningful for subtitle tracks only
            "encoding": str(encoding_val) if encoding_val is not None else "",
            "path": str(path),
        }
        rows.append(row)
    return rows


_TRACK_COLUMN_KEYS: List[str] = []


def _get_track_column_keys() -> List[str]:
    global _TRACK_COLUMN_KEYS
    if _TRACK_COLUMN_KEYS:
        return _TRACK_COLUMN_KEYS
    try:
        from common.utils.column_utils import load_column_specs  # local import to avoid cycles

        specs = load_column_specs("mkv_scan_columns").get("track", [])  # type: ignore[arg-type]
        keys: List[str] = []
        for col in specs:
            key = getattr(col, "key", None) or (col.get("key") if isinstance(col, dict) else None)
            if key:
                keys.append(str(key))
        if keys:
            _TRACK_COLUMN_KEYS = keys
            return keys
    except Exception:
        pass
    # Fallback to known defaults
    _TRACK_COLUMN_KEYS = [
        "tags",
        "output_filename",
        "type",
        "id",
        "name",
        "edited_name",
        "lang",
        "codec",
        "default",
        "forced",
        "encoding",
        "input_path",
        "output_path",
        "path",
    ]
    return _TRACK_COLUMN_KEYS


def load_tracks_from_csv(csv_path: Path) -> Dict[str, Dict[str, List[dict]]]:
    """
    Load track definitions from a scan tracks CSV and bucket by file/type.
    """
    from common.shared.report import load_tabular_rows  # local import to avoid cycles

    mapping: Dict[str, Dict[str, List[dict]]] = {}
    track_keys = _get_track_column_keys()
    path_key_order = [k for k in track_keys if k in {"output_path", "input_path", "path"}]
    if "file" not in path_key_order:
        path_key_order.append("file")
    rows, _ = load_tabular_rows(csv_path)
    for row in rows:
        file_path = ""
        for key in path_key_order:
            val = row.get(key)
            if val:
                file_path = str(val).strip()
                if file_path:
                    break
        if not file_path:
            continue
        normalized_file = str(Path(file_path).expanduser().resolve())
        entry = _normalize_track_entry(row)
        if entry is None:
            continue
        file_bucket = mapping.setdefault(
            normalized_file,
            {"video": [], "audio": [], "subtitles": []},
        )
        file_bucket[entry["type"]].append(entry)
    return mapping


# Track normalization helpers shared across tools
TRACK_TYPE_MAP = {
    "video": "video",
    "audio": "audio",
    "sub": "subtitles",
    "subs": "subtitles",
    "subtitle": "subtitles",
    "subtitles": "subtitles",
}


def get_mkvmerge_info(path: Path, *, log=None) -> Optional[dict]:
    """Run mkvmerge probe and return JSON payload."""
    from common.base.ops import run_command  # local import to avoid cycles
    import json as _json

    code, out, err = run_command(["mkvmerge", "-J", str(path)], capture=True, stream=False)
    if code != 0 or not out:
        if log:
            log.error(f"mkvmerge failed on {path.name}: {err.strip() if err else 'no output'}")
        return None
    try:
        return _json.loads(out)
    except _json.JSONDecodeError as exc:
        if log:
            log.error(f"JSON parse error for {path}: {exc}")
        return None


def build_mkvmerge_cmd(
    input_file: Path,
    output_file: Path,
    video_ids: List[str],
    audio_ids: List[str],
    subtitle_ids: List[str],
    track_meta: Dict[str, dict],
    title: Optional[str] = None,
) -> List[str]:
    cmd = ["mkvmerge", "-o", str(output_file)]
    if title:
        cmd += ["--title", title]
    if video_ids:
        cmd += ["--video-tracks", ",".join(video_ids)]
    if audio_ids:
        cmd += ["--audio-tracks", ",".join(audio_ids)]
    if subtitle_ids:
        cmd += ["--subtitle-tracks", ",".join(subtitle_ids)]

    track_order = video_ids + audio_ids + subtitle_ids
    if track_order:
        cmd += ["--track-order", ",".join(f"0:{tid}" for tid in track_order)]

    for tid, meta in track_meta.items():
        if meta.get("name") is not None:
            cmd += ["--track-name", f"{tid}:{meta['name']}"]
        if meta.get("lang"):
            cmd += ["--language", f"{tid}:{meta['lang']}"]
        if meta.get("default") is not None:
            cmd += ["--default-track", f"{tid}:{_bool_to_flag(meta['default'])}"]
        if meta.get("forced") is not None:
            cmd += ["--forced-track", f"{tid}:{_bool_to_flag(meta['forced'])}"]

    cmd.append(str(input_file))
    return cmd


def _bool_to_flag(value: bool) -> str:
    return "yes" if value else "no"


def current_tracks_by_type(info: dict) -> Dict[str, Dict[str, dict]]:
    mapping: Dict[str, Dict[str, dict]] = {"video": {}, "audio": {}, "subtitles": {}}
    for track in info.get("tracks", []):
        track_type = TRACK_TYPE_MAP.get((track.get("type") or "").lower())
        if track_type not in mapping:
            continue
        tid = str(track.get("id"))
        props = track.get("properties", {})
        mapping[track_type][tid] = {
            "lang": (props.get("language") or "und").lower(),
            "name": props.get("track_name") or "",
            "default": bool(props.get("default_track")),
            "forced": bool(props.get("forced_track")),
        }
    return mapping


def add_fallback_track(
    track_type: str,
    plan: Dict[str, List[dict]],
    current_map: Dict[str, Dict[str, dict]],
    mkv_path: Path,
    *,
    logger=None,
) -> Optional[dict]:
    available = current_map.get(track_type, {})
    log = logger
    for tid, props in available.items():
        already_present = any(entry["id"] == tid for entry in plan[track_type])
        if already_present:
            return None
        fallback_entry = {
            "id": tid,
            "type": track_type,
            "lang": props.get("lang") or None,
            "name": props.get("name") or None,
            "suggested_rename": None,
            "default": props.get("default"),
            "forced": props.get("forced"),
            "_fallback": True,
        }
        plan[track_type].append(fallback_entry)
        if log:
            log.warning(
                "⚠️ %s missing in definition for %s; retaining track id %s.",
                track_type.capitalize(),
                mkv_path.name,
                tid,
            )
        return fallback_entry
    if log:
        log.error(f"❌ No {track_type} tracks available in {mkv_path.name} to satisfy requirements.")
    return None


def prepare_track_plan(
    mkv_path: Path,
    track_rows: Dict[str, List[dict]],
    current_info: dict,
    *,
    logger=None,
) -> Tuple[Dict[str, List[dict]], List[str]]:
    plan = {
        "video": list(track_rows.get("video", [])),
        "audio": list(track_rows.get("audio", [])),
        "subtitles": list(track_rows.get("subtitles", [])),
    }
    reasons: List[str] = []
    current_map = current_tracks_by_type(current_info)

    if not plan["video"]:
        fallback = add_fallback_track("video", plan, current_map, mkv_path, logger=logger)
        if fallback:
            reasons.append(f"retained video track {fallback['id']}")
    if not plan["audio"]:
        fallback = add_fallback_track("audio", plan, current_map, mkv_path, logger=logger)
        if fallback:
            reasons.append(f"retained audio track {fallback['id']}")

    return plan, reasons


def desired_track_name(entry: dict) -> Optional[str]:
    original = (entry.get("name") or "").strip()
    edited = (entry.get("edited_name") or "").strip()
    suggested = (entry.get("suggested_rename") or "").strip()
    candidate = edited or suggested
    if candidate and candidate != original:
        return candidate
    return None


def compute_track_differences(
    current_info: dict,
    plan: Dict[str, List[dict]],
) -> Tuple[bool, List[str]]:
    current_map = current_tracks_by_type(current_info)
    reasons: List[str] = []

    for track_type, desired_entries in plan.items():
        desired_ids = [entry["id"] for entry in desired_entries]
        current_ids = list(current_map.get(track_type, {}).keys())

        for entry in desired_entries:
            tid = entry["id"]
            if tid not in current_map.get(track_type, {}):
                reasons.append(f"missing {track_type} track id {tid}")
                continue
            current_meta = current_map[track_type][tid]
            desired_name = desired_track_name(entry)
            if desired_name is not None and desired_name != current_meta.get("name", ""):
                reasons.append(f"track {tid} name differs")
            desired_lang = (entry.get("lang") or "").strip().lower()
            if desired_lang and desired_lang != current_meta.get("lang", "und"):
                reasons.append(f"track {tid} language differs")
            desired_default = entry.get("default")
            if desired_default is not None and bool(desired_default) != bool(current_meta.get("default")):
                reasons.append(f"track {tid} default flag differs")
            desired_forced = entry.get("forced")
            if desired_forced is not None and bool(desired_forced) != bool(current_meta.get("forced")):
                reasons.append(f"track {tid} forced flag differs")

        for tid in current_ids:
            if tid not in desired_ids:
                reasons.append(f"{track_type} track {tid} will be removed")

    return (len(reasons) > 0), reasons


def build_track_ids(plan: Dict[str, List[dict]]) -> Tuple[List[str], List[str], List[str]]:
    return (
        [entry["id"] for entry in plan.get("video", [])],
        [entry["id"] for entry in plan.get("audio", [])],
        [entry["id"] for entry in plan.get("subtitles", [])],
    )


def build_track_metadata(plan: Dict[str, List[dict]]) -> Dict[str, dict]:
    metadata: Dict[str, dict] = {}
    for entries in plan.values():
        for entry in entries:
            tid = entry["id"]
            metadata.setdefault(tid, {})
            effective_name = desired_track_name(entry)
            if effective_name is not None:
                metadata[tid]["name"] = effective_name
            if entry.get("lang"):
                metadata[tid]["lang"] = entry.get("lang")
            if entry.get("default") is not None:
                metadata[tid]["default"] = bool(entry.get("default"))
            if entry.get("forced") is not None:
                metadata[tid]["forced"] = bool(entry.get("forced"))
    return metadata


def desired_mkv_title(plan: Dict[str, List[dict]]) -> Optional[str]:
    for entry in plan.get("video", []):
        edited = (entry.get("edited_name") or "").strip()
        if edited:
            return edited
    return None


def current_mkv_title(info: dict) -> str:
    container = info.get("container") or {}
    props = container.get("properties") or {}
    title_val = props.get("title") or props.get("segment_title") or ""
    return str(title_val or "")

def resolve_tracks_csvs(
    roots: Sequence[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> List[Path]:
    """Discover latest report exports for scan_mkv_tracks variants."""
    from common.shared.report import discover_latest_csvs  # local import to avoid cycles

    report_dirs: List[Path] = []
    for root in roots:
        root = root.expanduser().resolve()
        reports_dir = (root / output_root).resolve() if output_root else (root / "reports").resolve()
        if not reports_dir.exists():
            continue
        report_dirs.append(reports_dir)

    if not tracks_csv_types:
        collected: List[Path] = []
        for base_name in ("scan_mkv_issues", "scan_mkv_ok"):
            try:
                matches = discover_latest_csvs(report_dirs, base_name, csv_parts)
            except FileNotFoundError:
                matches = []
            for m in matches:
                if m not in collected:
                    collected.append(m)
        if collected:
            return collected
        return discover_latest_csvs(report_dirs, "mkv_scan_tracks", csv_parts)

    results: List[Path] = []
    for t in tracks_csv_types:
        tclean = str(t).strip().lower()
        if tclean == "ok":
            base_name = "scan_mkv_ok"
        elif tclean == "issues":
            base_name = "scan_mkv_issues"
        else:
            base_name = tclean
        try:
            matches = discover_latest_csvs(report_dirs, base_name, csv_parts)
        except FileNotFoundError:
            matches = []
        for m in matches:
            if m not in results:
                results.append(m)
    return results


def find_latest_tracks_csv(
    roots: Sequence[Path],
    output_root: Optional[Path | str],
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> Optional[Path]:
    matches = resolve_tracks_csvs(roots, output_root, None, tracks_csv_types)
    return matches[0] if matches else None


def load_track_definitions(
    def_file: Optional[Path],
    roots: Sequence[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, List[dict]]]:
    from common.base.file_io import read_json  # local import to avoid cycles

    if def_file:
        def_path = def_file.expanduser().resolve()
        if not def_path.exists():
            return {}
        if def_path.suffix.lower() == ".csv":
            return load_tracks_from_csv(def_path)
        try:
            payload = read_json(def_path)
            if isinstance(payload, dict):
                return _normalize_json_definition(payload)
        except Exception:
            return {}
        return {}

    matches = resolve_tracks_csvs(roots, output_root, csv_parts, tracks_csv_types)
    if not matches:
        return {}
    mapping: Dict[str, Dict[str, List[dict]]] = {}
    for m in matches:
        try:
            chunk_map = load_tracks_from_csv(m)
        except Exception:
            continue
        for file_path, buckets in chunk_map.items():
            file_bucket = mapping.setdefault(
                file_path, {"video": [], "audio": [], "subtitles": []}
            )
            for k in ("video", "audio", "subtitles"):
                file_bucket[k].extend(buckets.get(k, []))
    return mapping


def _normalize_json_definition(payload: dict) -> Dict[str, Dict[str, List[dict]]]:
    mapping: Dict[str, Dict[str, List[dict]]] = {}
    for raw_file, tracks in payload.items():
        normalized_file = str(Path(raw_file).expanduser().resolve())
        file_bucket = mapping.setdefault(
            normalized_file,
            {"video": [], "audio": [], "subtitles": []},
        )
        for kind, entries in tracks.items():
            normalized_kind = TRACK_TYPE_MAP.get(str(kind).lower())
            if normalized_kind not in file_bucket:
                continue
            if isinstance(entries, dict):
                candidate_entries = [entries]
            elif isinstance(entries, list):
                candidate_entries = entries
            else:
                continue
            for entry in candidate_entries:
                if not isinstance(entry, dict):
                    continue
                payload = dict(entry)
                payload.setdefault("id", entry.get("id"))
                payload.setdefault("type", normalized_kind)
                normalized_entry = _normalize_track_entry(payload)
                if normalized_entry is None:
                    continue
                file_bucket[normalized_kind].append(normalized_entry)
    return mapping


def _normalize_track_entry(row: dict) -> Optional[dict]:
    track_id = str(row.get("id") or "").strip()
    if not track_id:
        return None

    track_type = str(row.get("type") or "").strip().lower()
    if track_type not in {"video", "audio", "subtitles"}:
        return None

    edited = (row.get("edited_name") or "").strip() or None
    suggested = (row.get("suggested_rename") or "").strip() or None
    name_val = (row.get("name") or row.get("track_name") or "").strip() or None
    desired_name = edited if edited and edited != name_val else None

    entry = {
        "id": track_id,
        "type": track_type,
        "lang": (row.get("lang") or "").strip() or None,
        "name": name_val,
        "edited_name": edited,
        "suggested_rename": suggested,
        "desired_name": desired_name,
        "default": _parse_bool(row.get("default")),
        "forced": _parse_bool(row.get("forced")),
    }
    return entry


def _parse_bool(val: object) -> Optional[bool]:
    if val is None:
        return None
    sval = str(val).strip().lower()
    if sval in {"1", "true", "yes", "y", "on"}:
        return True
    if sval in {"0", "false", "no", "n", "off"}:
        return False
    return None
