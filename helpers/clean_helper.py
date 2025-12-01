"""
Helper for cleaning MKV files using track plans produced by scan.py CSV exports.

This is a trimmed version of vid_mkv_clean that consumes an explicit tracks CSV
instead of discovering reports on disk.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from common.base.file_io import open_file
from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.report import export_report
from common.shared.utils import Progress
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

log = get_logger(__name__)


def clean_with_tracks_csv(
    tracks_csv: Path | str,
    output_dir: Optional[Path] = None,
    dry_run: bool = False,
) -> Dict[str, Union[List[Dict[str, str]], List[str], Path, str]]:
    csv_path = Path(tracks_csv).expanduser().resolve()
    if not csv_path.exists():
        log.error("❌ Tracks CSV not found: %s", csv_path)
        return []

    track_definitions = load_tracks_from_csv(csv_path)
    if not track_definitions:
        log.error("❌ No track definitions available in %s; aborting.", csv_path)
        return []

    base_output_dir = ensure_dir(output_dir or Path("./reports"))
    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir_candidate = base_output_dir / f"{run_stamp}_clean_helper"
    counter = 1
    while run_dir_candidate.exists():
        run_dir_candidate = base_output_dir / f"{run_stamp}_clean_helper_{counter:02d}"
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
            log.warning("⚠️ File not found: %s", mkv_path)
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
            log.error("❌ %s: %s.", mkv_path.name, msg)
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
            log.info("✅ %s: already matches track plan.", mkv_path.name)
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
        cleaned_tmp = cleaned_dir / f"cleaned-{mkv_path.name}"
        if cleaned_tmp.exists():
            cleaned_tmp.unlink()

        cmd = build_mkvmerge_cmd(mkv_path, cleaned_tmp, video_ids, audio_ids, subtitle_ids, track_meta)
        log.debug("Running mkvmerge: %s", " ".join(cmd))

        original_size = mkv_path.stat().st_size

        if dry_run:
            log.info("[DRY-RUN] Would execute: %s", " ".join(cmd))
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
            log.error("❌ mkvmerge failed for %s: %s", mkv_path.name, err.strip() if err else "unknown error")
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
            backup_target = backup_dir / mkv_path.name
            if backup_target.exists():
                backup_target.unlink()
            move_file(mkv_path, backup_target)
            move_file(cleaned_tmp, mkv_path)
            try:
                tag_val = datetime.now().strftime("%Y_%m_%d-%H_%M")
                if dry_run:
                    log.info("[DRY-RUN] Would set user.xdg.tags=%s on %s", tag_val, mkv_path)
                else:
                    if not write_fs_tag(mkv_path, "user.xdg.tags", tag_val):
                        log.warning("Failed to tag %s with user.xdg.tags=%s", mkv_path, tag_val)
            except Exception:
                log.warning("Failed to apply tag to %s", mkv_path)
            new_size = mkv_path.stat().st_size
            log.info("✅ Cleaned %s", mkv_path.name)
            results.append(
                {
                    "name": mkv_path.name,
                    "status": "cleaned",
                    "message": "; ".join(reasons),
                    "size_old": human_size(original_size),
                    "size_new": human_size(new_size),
                }
            )
            cleaned_files.append(str(mkv_path))
        except Exception as exc:
            log.error("💥 Post-remux handling failed for %s: %s", mkv_path.name, exc)
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

    export_report(
        results,
        base_name="clean_helper",
        output_dir=run_dir,
        write_csv_file=True,
        dry_run=dry_run,
    )

    return {
        "results": results,
        "cleaned": cleaned_files,
        "dry_run": dry_run_files,
        "missing": missing_files,
        "nochange": nochange_files,
        "failed": failed_files,
        "run_dir": run_dir,
        "tracks_csv": csv_path,
    }


__all__ = ["clean_with_tracks_csv"]
