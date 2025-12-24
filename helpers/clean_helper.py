"""
Helper for cleaning MKV files using track plans produced by scan.py CSV exports.

This is a trimmed version of vid_mkv_clean that consumes an explicit tracks CSV
instead of discovering reports on disk.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from typing import TypedDict

from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import run_command
from common.shared.loader import load_media_types
from common.shared.report import load_tabular_rows, write_csv
from common.shared.utils import Progress
from tqdm.contrib.logging import logging_redirect_tqdm # type: ignore
from common.utils.track_utils import (
    build_mkvmerge_cmd,
    build_track_ids,
    build_track_metadata,
    compute_track_differences,
    current_mkv_title,
    desired_mkv_title,
    get_mkvmerge_info,
    load_tracks_from_csv,
    prepare_track_plan,
)
from common.utils.tag_utils import write_fs_tag

Replacement = Tuple[str, str, str] | Tuple[str, str, str, str]


class CleanHelperResult(TypedDict):
    results: List[Dict[str, str]]
    cleaned: List[str]
    replacements: List[Replacement]
    dry_run: List[str]
    missing: List[str]
    nochange: List[str]
    failed: List[Tuple[str, str]]
    run_dir: Optional[Path]
    tracks_csv: Path
    clean_output_dir: Optional[Path]


log = get_logger(__name__)


def _empty_result(csv_path: Path) -> CleanHelperResult:
    return {
        "results": [],
        "cleaned": [],
        "replacements": [],
        "dry_run": [],
        "missing": [],
        "nochange": [],
        "failed": [],
        "run_dir": None,
        "tracks_csv": csv_path,
        "clean_output_dir": None,
    }


def clean_with_tracks_csv(
    tracks_csv: Path | str,
    output_dir: Optional[Path] = None,
    dry_run: bool = False,
    run_dir: Optional[Path] = None,
    clean_output_dir: Optional[Path] = None,
    target_ext: Optional[str] = None,
    extra_tags: Optional[List[str]] = None,
) -> CleanHelperResult:
    csv_path = Path(tracks_csv).expanduser().resolve()
    if not csv_path.exists():
        log.error("❌ Tracks CSV not found: %s", csv_path)
        return _empty_result(csv_path)

    raw_rows, _ = load_tabular_rows(csv_path)
    track_definitions = load_tracks_from_csv(csv_path)
    if not track_definitions:
        log.error("❌ No track definitions available in %s; aborting.", csv_path)
        return _empty_result(csv_path)

    subtitle_exts = {s.lower() for s in load_media_types().subtitle_exts}

    def _coerce_bool(val: object) -> Optional[bool]:
        if val is None:
            return None
        sval = str(val).strip().lower()
        if sval in {"1", "true", "yes", "y", "on"}:
            return True
        if sval in {"0", "false", "no", "n", "off"}:
            return False
        return None

    external_subs: Dict[str, List[dict]] = {}
    for row in raw_rows:
        ttype = str(row.get("type") or "").lower()
        if ttype != "subtitles":
            continue
        input_path_val = row.get("input_path") or row.get("path")
        output_path_val = row.get("output_path")
        if not input_path_val or not output_path_val:
            continue
        input_path = Path(str(input_path_val)).expanduser()
        if input_path.suffix.lower() not in subtitle_exts:
            continue
        tid = str(row.get("id") or "").strip()
        if not tid:
            continue
        name_val = (row.get("name") or row.get("track_name") or "").strip() or None
        edited_val = (row.get("edited_name") or "").strip() or None
        desired_name = edited_val if edited_val and edited_val != name_val else None
        normalized_output = str(Path(str(output_path_val)).expanduser().resolve())
        external_subs.setdefault(normalized_output, []).append(
            {
                "id": tid,
                "path": input_path.expanduser().resolve(),
                "lang": (row.get("lang") or "").strip() or None,
                "name": (row.get("name") or "").strip() or None,
                "desired_name": desired_name,
                "default": _coerce_bool(row.get("default")),
                "forced": _coerce_bool(row.get("forced")),
            }
        )

    base_output_dir = ensure_dir(output_dir or Path("./reports"))
    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    def _unique_dir(base: Path, name: str) -> Path:
        candidate = base / name
        counter = 1
        while candidate.exists():
            candidate = base / f"{name}_{counter:02d}"
            counter += 1
        return ensure_dir(candidate)

    run_dir = ensure_dir(run_dir) if run_dir else _unique_dir(base_output_dir, f"{run_stamp}_clean_helper")
    cleaned_dir = ensure_dir(clean_output_dir) if clean_output_dir else _unique_dir(
        base_output_dir, f"cleaned_{run_stamp}"
    )
    log.info("Cleaned files will be written to %s", cleaned_dir)

    results: List[Dict[str, str]] = []
    cleaned_files: List[str] = []
    replacements: List[Replacement] = []
    external_sidecars: Dict[str, List[Path]] = {}
    dry_run_files: List[str] = []
    missing_files: List[str] = []
    nochange_files: List[str] = []
    failed_files: List[Tuple[str, str]] = []

    progress = Progress(track_definitions.items(), desc="Cleaning MKVs")

    def _log_with_progress(level: str, msg: str, *args):
        if level == "debug":
            log.debug(msg, *args)
        elif level == "warning":
            log.warning(msg, *args)
        elif level == "error":
            log.error(msg, *args)
        else:
            log.info(msg, *args)
        try:
            progress.write(msg % args if args else msg)
        except Exception:
            # Fallback silently if formatting fails
            pass

    with logging_redirect_tqdm():
        for mkv_path_str, track_rows in progress:
            mkv_path = Path(mkv_path_str)
            if mkv_path.suffix.lower() in subtitle_exts:
                # Skip standalone subtitle files; they are handled as external tracks elsewhere.
                continue
            base_output = mkv_path.with_suffix(".mkv")
            output_filename = base_output.name
            output_path = base_output
            if target_ext:
                output_filename = base_output.with_suffix(target_ext).name
                output_path = base_output.with_suffix(target_ext)
            if not mkv_path.exists():
                _log_with_progress("warning", "⚠️ File not found: %s", mkv_path)
                results.append(
                    {
                        "name": output_filename,
                        "status": "missing",
                        "message": "file not found",
                        "size_old": "",
                        "size_new": "",
                    }
                )
                missing_files.append(str(mkv_path))
                continue

            current_info = get_mkvmerge_info(mkv_path, log=log)
            if current_info is None:
                results.append(
                    {
                        "name": output_filename,
                        "status": "error",
                        "message": "failed to probe file",
                        "size_old": human_size(mkv_path.stat().st_size),
                        "size_new": "",
                    }
                )
                failed_files.append((str(mkv_path), "probe failed"))
                continue

            plan, safety_notes = prepare_track_plan(mkv_path, track_rows, current_info, logger=log)
            if not plan["video"] or not plan["audio"]:
                msg = "missing required video/audio tracks"
                _log_with_progress("error", "❌ %s: %s.", mkv_path.name, msg)
                results.append(
                    {
                        "name": output_filename,
                        "status": "error",
                        "message": msg,
                        "size_old": human_size(mkv_path.stat().st_size),
                        "size_new": "",
                    }
                )
                failed_files.append((str(mkv_path), msg))
                continue

            needs_clean, reasons = compute_track_differences(current_info, plan)
            reasons.extend(safety_notes)
            desired_title = desired_mkv_title(plan)
            current_title = current_mkv_title(current_info)
            title_needs_update = bool(desired_title) and desired_title != current_title
            if title_needs_update:
                reasons.append("metadata title differs")

            if not needs_clean and not title_needs_update:
                _log_with_progress("info", "✅ %s: already matches track plan.", mkv_path.name)
                results.append(
                    {
                        "name": output_filename,
                        "status": "ok",
                        "message": "; ".join(reasons) if reasons else "already clean",
                        "size_old": human_size(mkv_path.stat().st_size),
                        "size_new": "",
                    }
                )
                nochange_files.append(str(mkv_path))
                continue

            video_ids, audio_ids, subtitle_ids = build_track_ids(plan)
            track_meta = build_track_metadata(plan)
            cleaned_tmp = cleaned_dir / output_filename
            if cleaned_tmp.exists():
                cleaned_tmp.unlink()

            target_keys = {
                str(mkv_path.resolve()),
                str(output_path.expanduser().resolve()),
            }
            ext_entries_raw: List[dict] = []
            for key in target_keys:
                ext_entries_raw.extend(external_subs.get(key, []))
            seen_sidecars: set[tuple[Path, str]] = set()
            ext_entries: List[dict] = []
            for entry in ext_entries_raw:
                sidecar_key = (entry["path"], entry["id"])
                if sidecar_key in seen_sidecars:
                    continue
                seen_sidecars.add(sidecar_key)
                if entry["path"].exists():
                    ext_entries.append(entry)

            def _apply_track_meta(cmd_list: List[str], meta: Dict[str, dict]) -> None:
                for tid, meta_cfg in meta.items():
                    if meta_cfg.get("name") is not None:
                        cmd_list += ["--track-name", f"{tid}:{meta_cfg['name']}"]
                    if meta_cfg.get("lang"):
                        cmd_list += ["--language", f"{tid}:{meta_cfg['lang']}"]
                    if meta_cfg.get("default") is not None:
                        cmd_list += ["--default-track", f"{tid}:{'yes' if meta_cfg['default'] else 'no'}"]
                    if meta_cfg.get("forced") is not None:
                        cmd_list += ["--forced-track", f"{tid}:{'yes' if meta_cfg['forced'] else 'no'}"]

            if ext_entries:
                ext_ids = {entry["id"] for entry in ext_entries}
                subtitle_ids_main = [tid for tid in subtitle_ids if tid not in ext_ids]
                track_meta_main = {tid: meta for tid, meta in track_meta.items() if tid not in ext_ids}
                track_order_parts = [f"0:{tid}" for tid in video_ids + audio_ids + subtitle_ids_main]

                cmd = ["mkvmerge", "-o", str(cleaned_tmp)]
                if desired_title:
                    cmd += ["--title", desired_title]
                if video_ids:
                    cmd += ["--video-tracks", ",".join(video_ids)]
                if audio_ids:
                    cmd += ["--audio-tracks", ",".join(audio_ids)]
                if subtitle_ids_main:
                    cmd += ["--subtitle-tracks", ",".join(subtitle_ids_main)]
                _apply_track_meta(cmd, track_meta_main)

                cmd.append(str(mkv_path))

                for idx, entry in enumerate(ext_entries, start=1):
                    ext_track_id = str(entry.get("source_track_id") or 0)
                    track_order_parts.append(f"{idx}:{ext_track_id}")
                    cmd += ["--subtitle-tracks", ext_track_id]
                    ext_meta = dict(track_meta.get(entry["id"]) or {})
                    desired_name = entry.get("desired_name") or entry.get("name")
                    if desired_name:
                        ext_meta["name"] = desired_name
                    if entry.get("lang") and not ext_meta.get("lang"):
                        ext_meta["lang"] = entry["lang"]
                    if entry.get("default") is not None and ext_meta.get("default") is None:
                        ext_meta["default"] = entry["default"]
                    if entry.get("forced") is not None and ext_meta.get("forced") is None:
                        ext_meta["forced"] = entry["forced"]
                    if ext_meta:
                        _apply_track_meta(cmd, {ext_track_id: ext_meta})
                    cmd.append(str(entry["path"]))
                external_sidecars[str(cleaned_tmp)] = [entry["path"] for entry in ext_entries]

                if track_order_parts:
                    cmd += ["--track-order", ",".join(track_order_parts)]
            else:
                cmd = build_mkvmerge_cmd(
                    mkv_path,
                    cleaned_tmp,
                    video_ids,
                    audio_ids,
                    subtitle_ids,
                    track_meta,
                    title=desired_title,
                )

            log.debug("Running mkvmerge: %s", " ".join(cmd))

            original_size = mkv_path.stat().st_size

            if dry_run:
                _log_with_progress("info", "[DRY-RUN] Would execute: %s", " ".join(cmd))
                results.append(
                    {
                        "name": output_filename,
                        "status": "dry-run",
                        "message": "; ".join(reasons),
                        "size_old": human_size(original_size),
                        "size_new": "",
                    }
                )
                dry_run_files.append(str(mkv_path))
                continue

            code, _, err = run_command(cmd, capture=True, stream=False)
            if code != 0:
                _log_with_progress("error", "❌ mkvmerge failed for %s: %s", mkv_path.name, err.strip() if err else "unknown error")
                if cleaned_tmp.exists():
                    cleaned_tmp.unlink()
                results.append(
                    {
                        "name": output_filename,
                        "status": "error",
                        "message": err.strip() if err else "mkvmerge failed",
                        "size_old": human_size(original_size),
                        "size_new": "",
                    }
                )
                failed_files.append((str(mkv_path), err.strip() if err else "mkvmerge failed"))
                continue

            try:
                try:
                    tag_val = datetime.now().strftime("%Y_%m_%d-%H_%M")
                    tags_to_apply = [tag_val]
                    if extra_tags:
                        tags_to_apply.extend(extra_tags)
                    if dry_run:
                        _log_with_progress("info", "[DRY-RUN] Would set user.xdg.tags=%s on %s", ",".join(tags_to_apply), cleaned_tmp)
                    else:
                        if not write_fs_tag(cleaned_tmp, "user.xdg.tags", ",".join(tags_to_apply)):
                            _log_with_progress("warning", "Failed to tag %s with user.xdg.tags=%s", cleaned_tmp, ",".join(tags_to_apply))
                except Exception:
                    _log_with_progress("warning", "Failed to apply tag to %s", cleaned_tmp)
                new_size = cleaned_tmp.stat().st_size
                _log_with_progress("info", "✅ Cleaned %s -> %s", mkv_path.name, cleaned_tmp)
                results.append(
                    {
                        "name": output_filename,
                        "status": "cleaned",
                        "message": "; ".join(reasons),
                        "size_old": human_size(original_size),
                        "size_new": human_size(new_size),
                    }
                )
                dest_path = output_path
                cleaned_files.append(str(cleaned_tmp))
                replacements.append((str(mkv_path), str(cleaned_tmp), str(dest_path)))
                # If we merged external subtitle sidecars, archive them alongside the original video,
                # but do not move/replace the sidecar in place (the merged MKV already contains it).
                if external_sidecars.get(str(cleaned_tmp)):
                    for sidecar in external_sidecars[str(cleaned_tmp)]:
                        replacements.append((str(sidecar), str(sidecar), str(sidecar), "archive_only"))
            except Exception as exc:
                _log_with_progress("error", "💥 Post-remux handling failed for %s: %s", mkv_path.name, exc)
                if cleaned_tmp.exists():
                    cleaned_tmp.unlink()
                results.append(
                    {
                        "name": output_filename,
                        "status": "error",
                        "message": str(exc),
                        "size_old": human_size(original_size),
                        "size_new": "",
                    }
                )
                failed_files.append((str(mkv_path), str(exc)))

    # Write consolidated results CSV into the cleaned output directory as clean_helper.csv
    try:
        report_path = (cleaned_dir / "clean_helper.csv").resolve()
        if dry_run:
            log.info("[DRY-RUN] Would write CSV: %s", report_path)
        else:
            fieldnames = list(results[0].keys()) if results else None
            write_csv(results, report_path, fieldnames=fieldnames, dry_run=dry_run)
            log.info("Report export completed for 'clean_helper' (saved) -> %s", report_path)
    except Exception:
        log.exception("Failed to write clean_helper.csv report")

    return {
        "results": results,
        "cleaned": cleaned_files,
        "replacements": replacements,
        "dry_run": dry_run_files,
        "missing": missing_files,
        "nochange": nochange_files,
        "failed": failed_files,
        "run_dir": run_dir,
        "tracks_csv": csv_path,
        "clean_output_dir": cleaned_dir,
    }


__all__ = ["clean_with_tracks_csv"]
