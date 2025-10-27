"""
file.renamer

Rename files and directories based on the latest file_scan report (CSV).
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from common.base.file_io import open_file
from common.base.fs import ensure_dir
from common.base.logging import get_logger
from common.base.ops import move_file, run_command
from common.shared.report import export_report, discover_latest_csvs, load_tabular_rows

from .utils import resolve_output_directory

log = get_logger(__name__)


def _load_rows(report_path: Path) -> List[Dict[str, str]]:
    rows, _ = load_tabular_rows(report_path)
    normalized: List[Dict[str, str]] = []
    for row in rows:
        if "title" not in row and "metadata_title" in row:
            row["title"] = row.get("metadata_title", "")
        if row.get("path"):
            normalized.append(row)
    return normalized


def _partition_rows(rows: Iterable[Dict[str, str]]) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    files: List[Dict[str, str]] = []
    dirs: List[Dict[str, str]] = []
    for row in rows:
        (dirs if (row.get("type") or "").lower() == "d" else files).append(row)
    return files, dirs


def _compute_target_name(path: Path, edited_name: str) -> Optional[str]:
    edited = edited_name.strip()
    if not edited:
        return None
    if path.is_file():
        suffix = path.suffix
        if suffix and edited.lower().endswith(suffix.lower()):
            return edited
        return edited + suffix
    return edited


def _apply_move(path: Path, target_name: str, *, dry_run: bool) -> Tuple[bool, Optional[Path], Optional[str]]:
    destination = path.with_name(target_name)
    try:
        move_file(path, destination, dry_run=dry_run)
        return True, destination, None
    except Exception as exc:
        return False, None, str(exc)


def _find_latest_csv(base_dir: Path, base_name: str) -> Optional[Path]:
    matches = discover_latest_csvs([base_dir], base_name)
    return matches[0] if matches else None


def resolve_scan_csvs(
    base_dir: Path,
    base_name: str,
    csv_parts: Optional[Iterable[int]] = None,
) -> List[Path]:
    return discover_latest_csvs([base_dir], base_name, csv_parts)


def _write_summary(
    run_dir: Path,
    renamed: List[str],
    skipped: List[str],
    failed: List[Tuple[str, str]],
    meta_updates: List[str],
    dry_run: bool,
) -> None:
    summary_path = run_dir / "summary.txt"
    with open_file(summary_path, "w") as handle:
        handle.write("======== SUMMARY ========\n")
        handle.write(f"Renamed : {len(renamed)}\n")
        handle.write(f"Metadata : {len(meta_updates)}\n")
        handle.write(f"Skipped : {len(skipped)}\n")
        handle.write(f"Failed  : {len(failed)}\n")
        handle.write("=========================\n\n")

        def _section(title: str, items: Iterable, with_reason: bool = False) -> None:
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

        _section("Renamed Entries", renamed)
        _section("Metadata Updates", meta_updates)
        _section("Skipped Entries", skipped)
        _section("Failed Entries", failed, with_reason=True)
        handle.write(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        if dry_run:
            handle.write("[DRY-RUN] No changes were applied.\n")


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
    import json

    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        log.debug(f"Invalid JSON from ffprobe for {file_path}")
        return ""

    return payload.get("format", {}).get("tags", {}).get("title", "") or ""


def _update_metadata_title(path: Path, new_title: str, *, dry_run: bool) -> bool:
    if not new_title:
        return False
    if dry_run:
        log.info(f"[DRY-RUN] Would update metadata title → {new_title}")
        return True

    temp_file = path.with_suffix(f".tmp{path.suffix}")
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(path),
        "-map",
        "0",
        "-c",
        "copy",
        "-metadata",
        f"title={new_title}",
        str(temp_file),
    ]
    code, _, err = run_command(cmd, capture=True, stream=False)
    if code == 0:
        move_file(temp_file, path, dry_run=False)
        log.debug(f"Updated metadata title: {path.name} → {new_title}")
        return True
    log.warning(f"ffmpeg failed for {path.name}: {err.strip() if err else 'unknown error'}")
    if temp_file.exists():
        temp_file.unlink(missing_ok=True)
    return False


def rename_from_scan(
    root: Path,
    *,
    csv_file: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    base_name: str = "file_scan",
    dry_run: bool = False,
) -> List[Dict[str, str]]:
    root = root.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root directory not found: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"Root path is not a directory: {root}")

    base_output = resolve_output_directory(root, output_dir)
    if csv_file is not None:
        csv_path = Path(csv_file).expanduser().resolve()
        if not csv_path.exists():
            raise FileNotFoundError(f"Report file not found: {csv_path}")
    else:
        csv_path = _find_latest_csv(base_output, base_name)
        if not csv_path:
            raise FileNotFoundError(f"No {base_name}_*.csv found under {base_output}")

    rows = _load_rows(csv_path)
    files, dirs = _partition_rows(rows)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    run_dir_candidate = base_output / f"{timestamp}_file_rename"
    counter = 1
    while run_dir_candidate.exists():
        run_dir_candidate = base_output / f"{timestamp}_file_rename_{counter:02d}"
        counter += 1
    run_dir = ensure_dir(run_dir_candidate)
    revert_path = run_dir / "revert.csv"

    results: List[Dict[str, str]] = []
    renamed_entries: List[str] = []
    skipped_entries: List[str] = []
    failed_entries: List[Tuple[str, str]] = []
    metadata_updates: List[str] = []
    log_messages: List[str] = []

    with open_file(revert_path, "w", newline="") as revert_handle:
        writer = csv.DictWriter(
            revert_handle,
            fieldnames=["path", "filename", "edited_filename", "title"],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()

        def process(row: Dict[str, str]) -> None:
            original_path = Path(row["path"]).expanduser()
            edited_name = (row.get("edited_name") or "").strip()
            edited_title = (row.get("edited_title") or "").strip()
            entry_type = (row.get("type") or "").lower()

            if not original_path.exists():
                failed_entries.append((row["path"], "path not found"))
                results.append({
                    "path": row["path"],
                    "type": row.get("type", ""),
                    "status": "missing",
                    "message": "path not found",
                })
                return

            target_name = _compute_target_name(original_path, edited_name)
            current_metadata = ""
            desired_metadata = edited_title
            metadata_changed = False

            if entry_type != "d":
                current_metadata = _probe_metadata_title(original_path)
                metadata_changed = bool(desired_metadata) and desired_metadata != current_metadata

            rename_needed = bool(target_name and original_path.name != target_name)

            if not rename_needed and not metadata_changed:
                skipped_entries.append(row["path"])
                results.append({
                    "path": row["path"],
                    "type": row.get("type", ""),
                    "status": "skipped",
                    "message": "no change",
                })
                return

            new_path = original_path
            if rename_needed:
                success, moved_path, error = _apply_move(original_path, target_name, dry_run=dry_run)
                if not success or moved_path is None:
                    failed_entries.append((row["path"], error or "rename failed"))
                    results.append({
                        "path": row["path"],
                        "type": row.get("type", ""),
                        "status": "error",
                        "message": error or "rename failed",
                    })
                    return
                renamed_entries.append(row["path"])
                log_messages.append(f"Renamed: {original_path} → {moved_path}")
                new_path = moved_path

            metadata_success = False
            if metadata_changed and entry_type != "d":
                metadata_success = _update_metadata_title(new_path, desired_metadata, dry_run=dry_run)
                if metadata_success:
                    metadata_updates.append(str(new_path))
                    log_messages.append(f"Updated metadata title for {new_path}: {desired_metadata}")
                else:
                    failed_entries.append((row["path"], "metadata update failed"))
                    results.append({
                        "path": row["path"],
                        "type": row.get("type", ""),
                        "status": "error",
                        "message": "metadata update failed",
                    })
                    return

            status_labels: List[str] = []
            if rename_needed:
                status_labels.append("renamed")
            if metadata_success:
                status_labels.append("meta")

            results.append({
                "path": row["path"],
                "type": row.get("type", ""),
                "status": "+".join(status_labels) if status_labels else "processed",
                "message": str(new_path),
            })

            if not dry_run:
                revert_metadata = current_metadata if entry_type != "d" else ""
                writer.writerow({
                    "path": str(new_path),
                    "filename": new_path.name,
                    "edited_filename": original_path.name,
                    "title": revert_metadata,
                })

        for row in files:
            process(row)

        for row in sorted(dirs, key=lambda r: len(Path(r["path"]).parts), reverse=True):
            process(row)

    export_report(
        results,
        base_name="file_rename",
        output_dir=run_dir,
        write_csv_file=True,
        dry_run=dry_run,
    )

    _write_summary(run_dir, renamed_entries, skipped_entries, failed_entries, metadata_updates, dry_run)
    rename_log_path = run_dir / "rename-log.txt"
    with open_file(rename_log_path, "w") as log_handle:
        log_handle.write("\n".join(log_messages) + ("\n" if log_messages else ""))

    log.info(f"📂 File rename artifacts written to: {run_dir}")
    return results


def cli(argv: Optional[Iterable[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Rename entries using a file_scan report (CSV).")
    parser.add_argument("csv_file", nargs="?", help="Report exported by file-scan (defaults to latest CSV).")
    parser.add_argument("--config", "-c", help="Path to YAML configuration (defaults to configs/config.yaml).")
    parser.add_argument("--dry-run", action="store_true", help="Simulate renames without applying changes.")
    parser.add_argument("--base-name", "-b", help="Override base name of the file_scan report.")
    parser.add_argument("--root", help="Explicit root directory (defaults to first config root).")

    from common.shared.loader import load_task_config
    from common.base.logging import setup_logging

    args = parser.parse_args(list(argv) if argv is not None else None)

    config_candidates = []
    if args.config:
        config_candidates.append(Path(args.config).expanduser())
    module_config = Path(__file__).resolve().parents[1] / "configs" / "config.yaml"
    config_candidates.append(module_config)
    config_candidates.append(Path.cwd() / "configs" / "config.yaml")

    config_path = next((c for c in config_candidates if c.exists()), None)
    if config_path is None:
        raise SystemExit("Configuration file not found. Provide --config explicitly.")

    cfg = load_task_config("file_rename", config_path)
    roots = [Path(p).expanduser().resolve() for p in cfg.get("roots", [])]
    if args.root:
        root_path = Path(args.root).expanduser().resolve()
    else:
        if not roots:
            raise SystemExit("file_rename config requires at least one root")
        root_path = roots[0]

    base_name = args.base_name or cfg.get("base_name") or "file_scan"
    output_dir = Path(cfg["output_dir"]).expanduser() if cfg.get("output_dir") else None
    if output_dir and output_dir.name == "file_rename":
        output_dir = output_dir.parent
    elif output_dir is None and cfg.get("__output_root__"):
        output_dir = Path(cfg["__output_root__"]).expanduser()

    logging_cfg = cfg.get("__logging__") or {}
    if logging_cfg:
        setup_logging(
            level=logging_cfg.get("level"),
            use_rich=logging_cfg.get("use_rich"),
            log_dir=logging_cfg.get("log_dir"),
            file_prefix=logging_cfg.get("file_prefix"),
        )

    csv_parts = cfg.get("csv_part") or []
    if args.csv_file:
        targets = [Path(args.csv_file).expanduser().resolve()]
    elif csv_parts:
        try:
            targets = resolve_scan_csvs(resolve_output_directory(root_path, output_dir), base_name, csv_parts)
        except FileNotFoundError as exc:
            raise SystemExit(str(exc))
    else:
        targets = []

    if not targets:
        rename_from_scan(
            root=root_path,
            csv_file=None,
            output_dir=output_dir,
            base_name=base_name,
            dry_run=args.dry_run or bool(cfg.get("dry_run", False)),
        )
    else:
        for csv_path in targets:
            rename_from_scan(
                root=root_path,
                csv_file=csv_path,
                output_dir=output_dir,
                base_name=base_name,
                dry_run=args.dry_run or bool(cfg.get("dry_run", False)),
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
