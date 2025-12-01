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
from common.shared.report import write_csv
from common.shared.utils import Progress
from tqdm.contrib.logging import logging_redirect_tqdm
from common.utils.track_utils import (
    build_mkvmerge_cmd,
    build_track_ids,
    build_track_metadata,
    compute_track_differences,
    get_mkvmerge_info,
    load_tracks_from_csv,
    prepare_track_plan,
)
from common.utils.tag_utils import write_fs_tag

class CleanHelperResult(TypedDict):
    results: List[Dict[str, str]]
    cleaned: List[str]
    replacements: List[Tuple[str, str, str]]
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
) -> CleanHelperResult:
    csv_path = Path(tracks_csv).expanduser().resolve()
    if not csv_path.exists():
        log.error("❌ Tracks CSV not found: %s", csv_path)
        return _empty_result(csv_path)

    track_definitions = load_tracks_from_csv(csv_path)
    if not track_definitions:
        log.error("❌ No track definitions available in %s; aborting.", csv_path)
        return _empty_result(csv_path)

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
    replacements: List[Tuple[str, str, str]] = []
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
            if not mkv_path.exists():
                _log_with_progress("warning", "⚠️ File not found: %s", mkv_path)
                results.append(
                    {
                        "name": mkv_path.name,
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
                        "name": mkv_path.name,
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
                        "name": mkv_path.name,
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

            if not needs_clean:
                _log_with_progress("info", "✅ %s: already matches track plan.", mkv_path.name)
                results.append(
                    {
                        "name": mkv_path.name,
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
            dest_name = mkv_path.with_suffix(target_ext).name if target_ext else mkv_path.name
            cleaned_tmp = cleaned_dir / dest_name
            if cleaned_tmp.exists():
                cleaned_tmp.unlink()

            cmd = build_mkvmerge_cmd(mkv_path, cleaned_tmp, video_ids, audio_ids, subtitle_ids, track_meta)
            log.debug("Running mkvmerge: %s", " ".join(cmd))

            original_size = mkv_path.stat().st_size

            if dry_run:
                _log_with_progress("info", "[DRY-RUN] Would execute: %s", " ".join(cmd))
                results.append(
                    {
                        "name": mkv_path.name,
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
                        "name": mkv_path.name,
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
                    if dry_run:
                        _log_with_progress("info", "[DRY-RUN] Would set user.xdg.tags=%s on %s", tag_val, cleaned_tmp)
                    else:
                        if not write_fs_tag(cleaned_tmp, "user.xdg.tags", tag_val):
                            _log_with_progress("warning", "Failed to tag %s with user.xdg.tags=%s", cleaned_tmp, tag_val)
                except Exception:
                    _log_with_progress("warning", "Failed to apply tag to %s", cleaned_tmp)
                new_size = cleaned_tmp.stat().st_size
                _log_with_progress("info", "✅ Cleaned %s -> %s", mkv_path.name, cleaned_tmp)
                results.append(
                    {
                        "name": mkv_path.name,
                        "status": "cleaned",
                        "message": "; ".join(reasons),
                        "size_old": human_size(original_size),
                        "size_new": human_size(new_size),
                    }
                )
                dest_path = mkv_path.with_suffix(target_ext) if target_ext else mkv_path
                cleaned_files.append(str(cleaned_tmp))
                replacements.append((str(mkv_path), str(cleaned_tmp), str(dest_path)))
            except Exception as exc:
                _log_with_progress("error", "💥 Post-remux handling failed for %s: %s", mkv_path.name, exc)
                if cleaned_tmp.exists():
                    cleaned_tmp.unlink()
                results.append(
                    {
                        "name": mkv_path.name,
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
