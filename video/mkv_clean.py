"""
video.mkv_clean

Automated MKV cleaning workflow fed by scan-tracks track exports.

Workflow summary:
 - Discover the latest scan_mkv_tracks_* (or legacy mkv_scan_tracks_*) reports under each root
   (or accept an explicit definition)
 - For each file, keep only the tracks present in the report (adding safety fallbacks)
 - Apply suggested track titles / language / default / forced flags during remux
 - Produce revertable backups alongside a CSV report of the run
"""

from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from common.base.file_io import open_file
from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.report import export_report
from common.utils.track_utils import (
    TRACK_TYPE_MAP,
    build_mkvmerge_cmd,
    build_track_ids,
    build_track_metadata,
    compute_track_differences,
    get_mkvmerge_info,
    load_track_definitions,
    prepare_track_plan,
)
from common.utils.tag_utils import write_fs_tag
from common.shared.loader import load_task_config
from common.shared.utils import Progress

log = get_logger(__name__)

NAME_LIST_PATTERN = "scan_mkv_tracks_*.csv"

def vid_mkv_clean(
    def_file: Optional[Path] = None,
    roots: Optional[Sequence[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path | str] = None,
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    resolved_roots: List[Path] = [Path(p).expanduser() for p in (roots or [Path.cwd()])]
    # Load task-level configuration (if present) to determine which
    # mkv_scan_tracks CSV variants to consume and any csv_part selections.
    try:
        task_conf = load_task_config("vid_mkv_clean", None)
    except Exception:
        task_conf = {}

    csv_parts = task_conf.get("csv_part")
    tracks_csv_types = task_conf.get("tracks_csv_types")

    track_definitions = load_track_definitions(
        def_file,
        resolved_roots,
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

        current_info = get_mkvmerge_info(mkv_path, log=log)
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

        plan, safety_notes = prepare_track_plan(mkv_path, track_rows, current_info, logger=log)
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
        needs_clean, reasons = compute_track_differences(current_info, plan)
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

        video_ids, audio_ids, subtitle_ids = build_track_ids(plan)
        track_meta = build_track_metadata(plan)
        cleaned_tmp = cleaned_dir / f"cleaned-{mkv_path.name}"
        if cleaned_tmp.exists():
            cleaned_tmp.unlink()

        cmd = build_mkvmerge_cmd(mkv_path, cleaned_tmp, video_ids, audio_ids, subtitle_ids, track_meta)
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
                tag_val = datetime.now().strftime("%Y_%m_%d-%H_%M")
                if dry_run:
                    log.info("[DRY-RUN] Would set user.xdg.tags=%s on %s", tag_val, mkv_path)
                else:
                    if not write_fs_tag(mkv_path, "user.xdg.tags", tag_val):
                        log.warning("Failed to tag %s with user.xdg.tags=%s", mkv_path, tag_val)
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
