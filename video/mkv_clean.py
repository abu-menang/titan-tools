"""
video.mkv_clean

Automated MKV cleaning workflow fed by vid-mkv-scan track exports.

Workflow summary:
 - Discover the latest scan_mkv_tracks_* (or legacy mkv_scan_tracks_*) reports under each root
   (or accept an explicit definition)
 - For each file, keep only the tracks present in the report (adding safety fallbacks)
 - Apply suggested track titles / language / default / forced flags during remux
 - Produce revertable backups alongside a CSV report of the run
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from common.base.file_io import open_file, read_json
from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.report import export_report, discover_latest_csvs, load_tabular_rows
from common.shared.loader import load_task_config
from common.shared.utils import Progress

log = get_logger(__name__)

NAME_LIST_PATTERN = "scan_mkv_tracks_*.csv"
TRACK_TYPE_MAP = {
    "video": "video",
    "audio": "audio",
    "sub": "subtitles",
    "subs": "subtitles",
    "subtitle": "subtitles",
    "subtitles": "subtitles",
}


# ---------------------------------------------------------------------------
# Report / Definition loading
# ---------------------------------------------------------------------------

def _parse_bool(token: object) -> Optional[bool]:
    if token is None:
        return None
    if isinstance(token, bool):
        return token
    text = str(token).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _normalize_track_entry(row: dict) -> Optional[dict]:
    track_id = str(row.get("id") or "").strip()
    if not track_id:
        return None

    track_type = TRACK_TYPE_MAP.get((row.get("type") or "").strip().lower())
    if track_type not in {"video", "audio", "subtitles"}:
        return None

    entry = {
        "id": track_id,
        "type": track_type,
        "lang": (row.get("lang") or "").strip() or None,
        "name": (row.get("name") or "").strip() or None,
        "suggested_rename": (
            row.get("edited_name")
            or row.get("suggested_rename")
            or ""
        ).strip() or None,
        "default": _parse_bool(row.get("default")),
        "forced": _parse_bool(row.get("forced")),
    }
    return entry


def _load_tracks_from_csv(csv_path: Path) -> Dict[str, Dict[str, List[dict]]]:
    mapping: Dict[str, Dict[str, List[dict]]] = {}
    rows, _ = load_tabular_rows(csv_path)
    for row in rows:
        file_path = (row.get("path") or row.get("file") or "").strip()
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


def _apply_clean_tag(path: Path, *, dry_run: bool = False) -> None:
    """Apply a timestamp tag to a cleaned file using extended attributes."""

    tag = datetime.now().strftime("%Y_%m_%d-%H_%M")
    if dry_run:
        log.info(f"[DRY-RUN] Would replace user.xdg.tags on {path} with {tag}")
        return

    # Remove any existing tags then set the new one.
    run_command(
        ["setfattr", "-x", "user.xdg.tags", str(path)],
        capture=True,
        stream=False,
    )

    code, _, err = run_command(
        ["setfattr", "-n", "user.xdg.tags", "-v", tag, str(path)],
        capture=True,
        stream=False,
    )
    if code != 0:
        log.warning(f"Failed to tag {path} with user.xdg.tags={tag}: {err.strip() if err else 'unknown error'}")


def resolve_tracks_csvs(
    roots: List[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> List[Path]:
    """Discover latest report exports for scan_mkv_tracks variants.

    tracks_csv_types may include any of: 'ok', 'issues'. If None, legacy
    behaviour is used and we search for base 'scan_mkv_tracks' and fall back
    to legacy 'mkv_scan_tracks'.
    """
    report_dirs: List[Path] = []
    for root in roots:
        root = root.expanduser().resolve()
        reports_dir = (root / output_root).resolve() if output_root else (root / "reports").resolve()
        if not reports_dir.exists():
            log.debug(f"Reports directory missing under {root}: {reports_dir}")
            continue
        report_dirs.append(reports_dir)

    # If no specific types requested, keep legacy behaviour (single base name)
    if not tracks_csv_types:
        # Prefer new per-status exports; fall back to legacy combined naming.
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
            # Allow callers to pass full base name as well
            base_name = tclean
        try:
            matches = discover_latest_csvs(report_dirs, base_name, csv_parts)
        except FileNotFoundError:
            matches = []
        for m in matches:
            if m not in results:
                results.append(m)
    return results


def _find_latest_tracks_csv(
    roots: List[Path],
    output_root: Optional[Path | str],
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> Optional[Path]:
    matches = resolve_tracks_csvs(roots, output_root, None, tracks_csv_types)
    return matches[0] if matches else None


def _load_track_definitions(
    def_file: Optional[Path],
    roots: List[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
    tracks_csv_types: Optional[Iterable[str]] = None,
) -> Dict[str, Dict[str, List[dict]]]:
    if def_file:
        def_path = def_file.expanduser().resolve()
        if not def_path.exists():
            log.error(f"Definition file not found: {def_path}")
            return {}
        if def_path.suffix.lower() == ".csv":
            log.info(f"📄 Using track definition report: {def_path}")
            return _load_tracks_from_csv(def_path)
        try:
            payload = read_json(def_path)
            log.info(f"📄 Loaded JSON definition: {def_path}")
            if isinstance(payload, dict):
                return _normalize_json_definition(payload)
            log.error("JSON definition must be a mapping of file paths to track lists")
        except Exception as exc:
            log.error(f"Failed to parse definition {def_path}: {exc}")
        return {}

    # Discover one or more track CSVs according to requested types and parts
    matches = resolve_tracks_csvs(roots, output_root, csv_parts, tracks_csv_types)
    if not matches:
        log.error("❌ Could not locate mkv_scan_tracks report under reports directories.")
        return {}
    mapping: Dict[str, Dict[str, List[dict]]] = {}
    for m in matches:
        log.info(f"📄 Loading track definition CSV: {m}")
        try:
            chunk_map = _load_tracks_from_csv(m)
        except Exception as exc:
            log.error(f"Failed to load {m}: {exc}")
            continue
        for file_path, buckets in chunk_map.items():
            file_bucket = mapping.setdefault(
                file_path, {"video": [], "audio": [], "subtitles": []}
            )
            for k in ("video", "audio", "subtitles"):
                file_bucket[k].extend(buckets.get(k, []))
    return mapping


# ---------------------------------------------------------------------------
# mkvmerge Introspection helpers
# ---------------------------------------------------------------------------

def _get_mkvmerge_info(path: Path) -> Optional[dict]:
    code, out, err = run_command(["mkvmerge", "-J", str(path)], capture=True, stream=False)
    if code != 0 or not out:
        log.error(f"mkvmerge failed on {path.name}: {err.strip() if err else 'no output'}")
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        log.error(f"JSON parse error for {path}: {exc}")
        return None


def _current_tracks_by_type(info: dict) -> Dict[str, Dict[str, dict]]:
    mapping: Dict[str, Dict[str, dict]] = {
        "video": {},
        "audio": {},
        "subtitles": {},
    }
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


def _add_fallback_track(
    track_type: str,
    plan: Dict[str, List[dict]],
    current_map: Dict[str, Dict[str, dict]],
    mkv_path: Path,
) -> Optional[dict]:
    available = current_map.get(track_type, {})
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
        log.warning(
            "⚠️ %s missing in definition for %s; retaining track id %s.",
            track_type.capitalize(),
            mkv_path.name,
            tid,
        )
        return fallback_entry
    log.error(f"❌ No {track_type} tracks available in {mkv_path.name} to satisfy requirements.")
    return None


def _prepare_plan(
    mkv_path: Path,
    track_rows: Dict[str, List[dict]],
    current_info: dict,
) -> Tuple[Dict[str, List[dict]], List[str]]:
    plan = {
        "video": list(track_rows.get("video", [])),
        "audio": list(track_rows.get("audio", [])),
        "subtitles": list(track_rows.get("subtitles", [])),
    }
    reasons: List[str] = []
    current_map = _current_tracks_by_type(current_info)

    if not plan["video"]:
        fallback = _add_fallback_track("video", plan, current_map, mkv_path)
        if fallback:
            reasons.append(f"retained video track {fallback['id']}")
    if not plan["audio"]:
        fallback = _add_fallback_track("audio", plan, current_map, mkv_path)
        if fallback:
            reasons.append(f"retained audio track {fallback['id']}")

    return plan, reasons


def _desired_track_name(entry: dict) -> Optional[str]:
    return entry.get("suggested_rename") or entry.get("name")


def _compute_differences(
    current_info: dict,
    plan: Dict[str, List[dict]],
) -> Tuple[bool, List[str]]:
    current_map = _current_tracks_by_type(current_info)
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
            desired_name = _desired_track_name(entry)
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


def _build_track_ids(plan: Dict[str, List[dict]]) -> Tuple[List[str], List[str], List[str]]:
    return (
        [entry["id"] for entry in plan.get("video", [])],
        [entry["id"] for entry in plan.get("audio", [])],
        [entry["id"] for entry in plan.get("subtitles", [])],
    )


def _build_track_metadata(plan: Dict[str, List[dict]]) -> Dict[str, dict]:
    metadata: Dict[str, dict] = {}
    for entries in plan.values():
        for entry in entries:
            tid = entry["id"]
            metadata.setdefault(tid, {})
            effective_name = _desired_track_name(entry)
            if effective_name is not None:
                metadata[tid]["name"] = effective_name
            if entry.get("lang"):
                metadata[tid]["lang"] = entry.get("lang")
            if entry.get("default") is not None:
                metadata[tid]["default"] = bool(entry.get("default"))
            if entry.get("forced") is not None:
                metadata[tid]["forced"] = bool(entry.get("forced"))
    return metadata


def _bool_to_flag(value: bool) -> str:
    return "yes" if value else "no"


def _build_mkvmerge_cmd(
    input_file: Path,
    output_file: Path,
    video_ids: List[str],
    audio_ids: List[str],
    subtitle_ids: List[str],
    track_meta: Dict[str, dict],
) -> List[str]:
    cmd = ["mkvmerge", "-o", str(output_file)]
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def vid_mkv_clean(
    def_file: Optional[Path] = None,
    roots: Optional[List[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path | str] = None,
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    roots = [Path(p).expanduser() for p in (roots or [Path.cwd()])]
    # Load task-level configuration (if present) to determine which
    # mkv_scan_tracks CSV variants to consume and any csv_part selections.
    try:
        task_conf = load_task_config("vid_mkv_clean", None)
    except Exception:
        task_conf = {}

    csv_parts = task_conf.get("csv_part")
    tracks_csv_types = task_conf.get("tracks_csv_types")

    track_definitions = _load_track_definitions(
        def_file,
        roots,
        output_root,
        csv_parts=csv_parts,
        tracks_csv_types=tracks_csv_types,
    )
    if not track_definitions:
        log.error("❌ No track definitions available; aborting mkv_clean run.")
        return []

    base_output_dir = ensure_dir(output_dir or Path("./reports"))
    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir_candidate = base_output_dir / f"{run_stamp}_mkv_clean"
    counter = 1
    while run_dir_candidate.exists():
        run_dir_candidate = base_output_dir / f"{run_stamp}_mkv_clean_{counter:02d}"
        counter += 1
    run_dir = ensure_dir(run_dir_candidate)
    backup_dir = ensure_dir(run_dir / "ori")
    cleaned_dir = ensure_dir(run_dir / "staging")

    results: List[Dict[str, str]] = []
    cleaned_files: List[str] = []
    dry_run_files: List[str] = []
    missing_files: List[str] = []
    nochange_files: List[str] = []
    failed_files: List[Tuple[str, str]] = []

    for mkv_path_str, track_rows in Progress(track_definitions.items(), desc="Cleaning MKVs"):
        mkv_path = Path(mkv_path_str)
        if not mkv_path.exists():
            log.warning(f"⚠️ File not found: {mkv_path}")
            results.append({
                "name": mkv_path.name,
                "status": "missing",
                "message": "file not found",
                "size_old": "",
                "size_new": "",
            })
            missing_files.append(str(mkv_path))
            continue

        current_info = _get_mkvmerge_info(mkv_path)
        if current_info is None:
            results.append({
                "name": mkv_path.name,
                "status": "error",
                "message": "failed to probe file",
                "size_old": human_size(mkv_path.stat().st_size),
                "size_new": "",
            })
            failed_files.append((str(mkv_path), "probe failed"))
            continue

        plan, safety_notes = _prepare_plan(mkv_path, track_rows, current_info)
        if not plan["video"] or not plan["audio"]:
            msg = "missing required video/audio tracks"
            log.error(f"❌ {mkv_path.name}: {msg}.")
            results.append({
                "name": mkv_path.name,
                "status": "error",
                "message": msg,
                "size_old": human_size(mkv_path.stat().st_size),
                "size_new": "",
            })
            failed_files.append((str(mkv_path), msg))
            continue
        needs_clean, reasons = _compute_differences(current_info, plan)
        reasons.extend(safety_notes)

        if not needs_clean:
            log.info(f"✅ {mkv_path.name}: already matches track plan.")
            results.append({
                "name": mkv_path.name,
                "status": "ok",
                "message": "; ".join(reasons) if reasons else "already clean",
                "size_old": human_size(mkv_path.stat().st_size),
                "size_new": "",
            })
            nochange_files.append(str(mkv_path))
            continue

        video_ids, audio_ids, subtitle_ids = _build_track_ids(plan)
        track_meta = _build_track_metadata(plan)
        cleaned_tmp = cleaned_dir / f"cleaned-{mkv_path.name}"
        if cleaned_tmp.exists():
            cleaned_tmp.unlink()

        cmd = _build_mkvmerge_cmd(mkv_path, cleaned_tmp, video_ids, audio_ids, subtitle_ids, track_meta)
        log.debug(f"Running mkvmerge: {' '.join(cmd)}")

        original_size = mkv_path.stat().st_size

        if dry_run:
            log.info(f"[DRY-RUN] Would execute: {' '.join(cmd)}")
            results.append({
                "name": mkv_path.name,
                "status": "dry-run",
                "message": "; ".join(reasons),
                "size_old": human_size(original_size),
                "size_new": "",
            })
            dry_run_files.append(str(mkv_path))
            continue

        code, _, err = run_command(cmd, capture=True, stream=False)
        if code != 0:
            log.error(f"❌ mkvmerge failed for {mkv_path.name}: {err.strip() if err else 'unknown error'}")
            if cleaned_tmp.exists():
                cleaned_tmp.unlink()
            results.append({
                "name": mkv_path.name,
                "status": "error",
                "message": err.strip() if err else "mkvmerge failed",
                "size_old": human_size(original_size),
                "size_new": "",
            })
            failed_files.append((str(mkv_path), err.strip() if err else "mkvmerge failed"))
            continue

        try:
            backup_target = backup_dir / mkv_path.name
            if backup_target.exists():
                backup_target.unlink()
            move_file(mkv_path, backup_target)
            move_file(cleaned_tmp, mkv_path)
            try:
                _apply_clean_tag(mkv_path, dry_run=dry_run)
            except Exception:
                log.warning(f"Failed to apply tag to {mkv_path}")
            new_size = mkv_path.stat().st_size
            log.info(f"✅ Cleaned {mkv_path.name}")
            results.append({
                "name": mkv_path.name,
                "status": "cleaned",
                "message": "; ".join(reasons),
                "size_old": human_size(original_size),
                "size_new": human_size(new_size),
            })
            cleaned_files.append(str(mkv_path))
        except Exception as exc:
            log.error(f"💥 Post-remux handling failed for {mkv_path.name}: {exc}")
            if cleaned_tmp.exists():
                cleaned_tmp.unlink()
            results.append({
                "name": mkv_path.name,
                "status": "error",
                "message": str(exc),
                "size_old": human_size(original_size),
                "size_new": "",
            })
            failed_files.append((str(mkv_path), str(exc)))

    export_report(
        results,
        base_name="vid_mkv_clean",
        output_dir=run_dir,
        write_csv_file=True,
        dry_run=dry_run,
    )

    summary_path = run_dir / "summary.txt"
    with open_file(summary_path, "w") as handle:
        handle.write("======== SUMMARY ========\n")
        handle.write(f"Total files listed : {len(track_definitions)}\n")
        handle.write(f"Cleaned            : {len(cleaned_files)}\n")
        handle.write(f"Dry-run            : {len(dry_run_files)}\n")
        handle.write(f"No change          : {len(nochange_files)}\n")
        handle.write(f"Missing            : {len(missing_files)}\n")
        handle.write(f"Failed             : {len(failed_files)}\n")
        handle.write("=========================\n\n")

        def _write_section(title: str, items: Iterable[str | Tuple[str, str]], with_reason: bool = False) -> None:
            items = list(items)
            handle.write(f"{title}: {len(items)}\n")
            if not items:
                handle.write("- None -\n\n")
                return
            handle.write("-" * len(title) + "\n")
            for item in items:
                if with_reason and isinstance(item, tuple):
                    handle.write(f"{item[0]} — {item[1]}\n")
                else:
                    handle.write(f"{item}\n")
            handle.write("\n")

        _write_section("Cleaned Files", cleaned_files)
        _write_section("Dry-Run Files", dry_run_files)
        _write_section("No-Change Files", nochange_files)
        _write_section("Missing Files", missing_files)
        _write_section("Failed Files", failed_files, with_reason=True)
        handle.write(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        if dry_run:
            handle.write("[DRY-RUN] mkvmerge commands were not executed.\n")

    log.info(f"📂 MKV clean artifacts saved to: {run_dir}")

    return results
