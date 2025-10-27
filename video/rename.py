"""
video.rename

Batch rename and metadata update tool for Titan Tools.

Supports:
 - Applying edits recorded in the mkv_scan_name_list report export
 - Updating MKV/MP4 title metadata
 - Dry-run safety, revert CSV generation, and textual summaries
"""

from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from common.base.file_io import open_file
from common.base.fs import ensure_dir, human_size
from common.base.logging import get_logger
from common.base.ops import run_command, move_file
from common.shared.report import export_report, discover_latest_csvs, load_tabular_rows
from common.shared.utils import Progress

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NAME_LIST_PATTERN = "mkv_scan_name_list_*.csv"


def _resolve_reports_dir(root: Path, output_root: Optional[Path | str]) -> Path:
    root = Path(root).expanduser().resolve()
    if output_root is None:
        return (root / "reports").resolve()

    candidate = Path(output_root).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve()


def _find_latest_name_list(
    roots: List[Path],
    output_root: Optional[Path | str],
) -> Optional[Path]:
    matches = resolve_name_list_csvs(roots, output_root)
    return matches[0] if matches else None


def resolve_name_list_csvs(
    roots: List[Path],
    output_root: Optional[Path | str],
    csv_parts: Optional[Iterable[int]] = None,
) -> List[Path]:
    report_dirs: List[Path] = []
    for root in roots:
        reports_dir = _resolve_reports_dir(root, output_root)
        if not reports_dir.exists():
            log.debug(f"Reports directory not found for root {root}: {reports_dir}")
            continue
        report_dirs.append(reports_dir)

    return discover_latest_csvs(report_dirs, "mkv_scan_name_list", csv_parts)


def _load_name_list_rows(report_path: Path) -> tuple[List[Dict[str, str]], List[str]]:
    report_path = report_path.resolve()
    if not report_path.exists():
        raise FileNotFoundError(f"Name list report not found: {report_path}")

    rows, fieldnames = load_tabular_rows(report_path)

    normalized_rows: List[Dict[str, str]] = []
    for row in rows:
        if "title" not in row and "metadata_title" in row:
            row["title"] = row.get("metadata_title", "")
        if row.get("path"):
            normalized_rows.append(row)

    normalized_fieldnames = [
        "title" if name == "metadata_title" else name for name in fieldnames
    ]

    return normalized_rows, normalized_fieldnames


def _probe_metadata_title(file_path: Path) -> str:
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


def _apply_original_suffix(proposed: str, original_path: Path) -> str:
    proposed = proposed.strip()
    if not proposed:
        return proposed

    original_suffix = "".join(original_path.suffixes)
    if not original_suffix:
        return proposed

    proposed_path = Path(proposed)
    proposed_suffix = "".join(proposed_path.suffixes)
    if proposed_suffix and proposed_suffix.lower() == original_suffix.lower():
        return proposed

    base = proposed
    for suffix in proposed_path.suffixes:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
    if not base:
        base = proposed_path.stem or proposed

    return f"{base}{original_suffix}"


def _update_metadata_title(path: Path, new_title: str, dry_run: bool = False) -> bool:
    """
    Update the embedded title metadata of a media file using ffmpeg.
    """
    if not new_title:
        return False

    if dry_run:
        log.info(f"[DRY-RUN] Would update metadata title → {new_title}")
        return True

    temp_file = path.with_suffix(f".tmp{path.suffix}")
    cmd = [
        "ffmpeg", "-y", "-i", str(path),
        "-map", "0", "-c", "copy",
        "-metadata", f"title={new_title}",
        str(temp_file),
    ]
    code, out, err = run_command(cmd, capture=True, stream=False)
    if code == 0:
        move_file(temp_file, path, dry_run=False)
        log.debug(f"Updated metadata title: {path.name} → {new_title}")
        return True
    else:
        log.warning(f"ffmpeg failed for {path.name}: {err.strip() if err else 'unknown error'}")
        if temp_file.exists():
            temp_file.unlink(missing_ok=True)
        return False


# ---------------------------------------------------------------------------
# Main Logic
# ---------------------------------------------------------------------------

def vid_rename(
    name_list_file: Optional[Path] = None,
    roots: Optional[List[Path | str]] = None,
    output_dir: Optional[Path] = None,
    output_root: Optional[Path | str] = None,
    update_metadata: bool = True,
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    """
    Apply renames and metadata updates described in the mkv_scan name list report.

    Args:
        name_list_file: Optional explicit path to a mkv_scan_name_list report.
        roots: Directories whose reports should be inspected for name lists.
        output_dir: Directory for reports and backups (defaults beside the report).
        output_root: Optional reports subdirectory (used to locate the report when
                     name_list_file is omitted).
        update_metadata: If True, update embedded titles for edited entries.
        dry_run: Simulate actions without writing changes.
    """

    resolved_roots = [Path(p).expanduser().resolve() for p in (roots or [Path.cwd()])]
    if not resolved_roots:
        log.error("❌ No roots available for rename processing.")
        return []

    if name_list_file:
        name_list_path = Path(name_list_file).expanduser().resolve()
    else:
        name_list_path = _find_latest_name_list(resolved_roots, output_root)
        if name_list_path is None:
            log.error("❌ Could not locate any mkv_scan_name_list report under reports directories.")
            return []

    if not name_list_path.exists():
        log.error(f"❌ Name list report not found: {name_list_path}")
        return []

    log.info(f"📄 Using name list: {name_list_path}")

    rows, fieldnames = _load_name_list_rows(name_list_path)
    if not rows:
        log.warning("No editable entries found in the name list report.")
        return []

    log.info(f"📊 Report entries loaded: {len(rows)}")

    required_columns = {"path", "type", "name", "edited_name", "title", "edited_title"}
    missing_columns = required_columns.difference(fieldnames)
    if missing_columns:
        log.error(f"Name list report missing required columns: {', '.join(sorted(missing_columns))}")
        return []

    if output_dir:
        output_base = ensure_dir(Path(output_dir).expanduser())
    else:
        output_base = ensure_dir(name_list_path.parent / "rename")
    backup_dir = ensure_dir(output_base / "rename_ori")
    log.info(f"📁 Output directory: {output_base}")
    revert_path = output_base / "revert.csv"
    summary_path = output_base / "summary.txt"

    results: List[Dict[str, str]] = []
    renamed: List[tuple[str, str]] = []
    meta_updated: List[tuple[str, str]] = []
    skipped: List[tuple[str, str]] = []
    failed: List[tuple[str, str]] = []

    with open_file(revert_path, "w", newline="") as revert_handle:
        writer = csv.DictWriter(
            revert_handle,
            fieldnames=["path", "type", "name", "edited_name", "title", "edited_title"],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()

        for row in Progress(rows, desc="Applying edits"):
            path_value = (row.get("path") or "").strip()
            if not path_value:
                failed.append(("<missing>", "missing path in report"))
                continue

            original_path = Path(path_value).expanduser()
            original_type = (row.get("type") or "").strip()
            entry_type = original_type.lower()
            edited_name_raw = (row.get("edited_name") or "").strip()
            title_value = (row.get("title") or row.get("metadata_title") or "").strip()
            edited_title_value = (row.get("edited_title") or "").strip()
            new_title_value = edited_title_value.strip()

            if not original_path.exists():
                failed.append((path_value, "file not found"))
                target_name_missing = _apply_original_suffix(edited_name_raw, original_path) if edited_name_raw else original_path.name
                results.append({
                    "old": original_path.name,
                    "new": target_name_missing,
                    "status": "missing",
                    "message": "file not found",
                    "old_path": path_value,
                    "new_path": "",
                    "size_old": "",
                    "size_new": "",
                })
                continue

            original_size_bytes = original_path.stat().st_size
            target_name = original_path.name
            if edited_name_raw:
                target_name = _apply_original_suffix(edited_name_raw, original_path)
            if entry_type == "d":
                current_title = title_value
            else:
                current_title = _probe_metadata_title(original_path) or title_value
            new_size_bytes = original_size_bytes
            needs_rename = bool(edited_name_raw) and target_name != original_path.name
            needs_meta = (
                update_metadata
                and entry_type != "d"
                and bool(new_title_value)
                and new_title_value != current_title
            )

            if not needs_rename and not needs_meta:
                skipped.append((path_value, "no changes"))
                log.info(
                    "⏭️ Skipping %s — rename=%s, title_change=%s",
                    original_path.name,
                    "no" if not needs_rename else "yes",
                    "no" if not needs_meta else "yes",
                )
                results.append({
                    "old": original_path.name,
                    "new": original_path.name,
                    "status": "skipped",
                    "message": "no changes",
                    "old_path": path_value,
                    "new_path": path_value,
                    "size_old": human_size(original_size_bytes),
                    "size_new": human_size(original_size_bytes),
                })
                continue

            new_path = original_path
            rename_success = False
            metadata_success = False

            try:
                if needs_rename:
                    destination = original_path.with_name(target_name)
                    if not dry_run:
                        backup_target = backup_dir / original_path.name
                        ensure_dir(backup_dir)
                        shutil.copy2(original_path, backup_target)
                    move_file(original_path, destination, dry_run=dry_run)
                    new_path = destination
                    rename_success = True
                    renamed.append((path_value, str(destination)))
                    log.info(f"🔄 {original_path.name} → {destination.name}")
                    if new_path.exists():
                        new_size_bytes = new_path.stat().st_size

                if needs_meta:
                    metadata_success = _update_metadata_title(new_path, new_title_value, dry_run=dry_run)
                    if metadata_success:
                        meta_updated.append((str(new_path), new_title_value))
                        if new_path.exists():
                            new_size_bytes = new_path.stat().st_size
                    else:
                        failed.append((str(new_path), "metadata update failed"))

                if rename_success or metadata_success:
                    writer.writerow({
                        "path": str(new_path),
                        "type": original_type,
                        "name": new_path.name,
                        "edited_name": original_path.name,
                        "title": new_title_value if metadata_success else current_title,
                        "edited_title": current_title,
                    })

                status_tokens: List[str] = []
                if rename_success:
                    status_tokens.append("renamed")
                if metadata_success:
                    status_tokens.append("meta")

                status = "+".join(status_tokens) if status_tokens else "error"
                message = []
                if rename_success:
                    message.append("renamed")
                if metadata_success:
                    message.append("metadata updated")
                if needs_meta and not metadata_success:
                    message.append("metadata update failed")

                results.append({
                    "old": original_path.name,
                    "new": new_path.name,
                    "status": status,
                    "message": "; ".join(message) if message else "",
                    "old_path": path_value,
                    "new_path": str(new_path),
                    "size_old": human_size(original_size_bytes),
                    "size_new": human_size(new_size_bytes) if new_path.exists() else human_size(original_size_bytes),
                })

            except Exception as exc:
                failed.append((path_value, str(exc)))
                log.error(f"Rename failed for {original_path}: {exc}")
                results.append({
                    "old": original_path.name,
                    "new": target_name,
                    "status": "error",
                    "message": str(exc),
                    "old_path": path_value,
                    "new_path": str(new_path),
                    "size_old": human_size(original_size_bytes),
                    "size_new": "",
                })

    summary_lines = [
        "======== SUMMARY ========",
        f"✅ Renamed          : {len(renamed)}",
        f"🎵 Metadata Updated : {len(meta_updated)}",
        f"⚠️  Skipped          : {len(skipped)}",
        f"❌ Failed           : {len(failed)}",
        "=========================",
        "",
    ]

    def _append_section(title: str, entries: List[tuple[str, str]]) -> None:
        if not entries:
            return
        summary_lines.append(f"---- {title} ({len(entries)}) ----")
        for item in entries:
            summary_lines.append(" → ".join(item))
        summary_lines.append("")

    _append_section("✅ Renamed", renamed)
    _append_section("🎵 Metadata Updated", meta_updated)
    _append_section("⚠️ Skipped", skipped)
    _append_section("❌ Failed", failed)
    summary_lines.append(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with open_file(summary_path, "w") as summary_handle:
        summary_handle.write("\n".join(summary_lines) + "\n")

    log.info("======== SUMMARY ========")
    log.info(f"✅ Renamed          : {len(renamed)}")
    log.info(f"🎵 Metadata Updated : {len(meta_updated)}")
    log.info(f"⚠️ Skipped          : {len(skipped)}")
    log.info(f"❌ Failed           : {len(failed)}")
    log.info(f"Revert CSV written → {revert_path}")
    log.info(f"Summary written    → {summary_path}")

    export_report(
        results,
        base_name="vid_rename",
        output_dir=output_base,
        write_csv_file=True,
        dry_run=dry_run,
    )

    return results
