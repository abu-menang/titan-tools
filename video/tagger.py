"""
Apply filesystem tags to files listed in scan CSVs.

Tags applied:
 - timestamp (always)
 - additional tags from config (optional)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Sequence, Optional

from common.shared.report import load_tabular_rows
from common.utils.tag_utils import write_fs_tag
from common.base.logging import get_logger

log = get_logger(__name__)

PATH_FIELDS: Sequence[str] = ("output_path", "input_path", "path", "file", "output_filename", "name")


def _extract_paths_from_csv(csv_path: Path) -> List[Path]:
    rows, _ = load_tabular_rows(csv_path)
    paths: List[Path] = []
    for row in rows:
        target: str | None = None
        for key in PATH_FIELDS:
            val = row.get(key)
            if val:
                target = str(val).strip()
                if target:
                    break
        if not target:
            continue
        p = Path(target).expanduser()
        if p.exists():
            paths.append(p)
    return paths


def tag_files_from_csv_dir(
    csv_dir: Path | str,
    roots: Optional[Iterable[Path | str]] = None,
    tags: Iterable[str] | None = None,
    *,
    dry_run: bool = False,
) -> dict:
    base_dirs = [Path(r).expanduser() for r in (roots or [])] or [Path.cwd()]
    csv_dir_path = Path(csv_dir).expanduser()
    candidate_dirs = []
    if csv_dir_path.is_absolute():
        candidate_dirs.append(csv_dir_path)
    else:
        candidate_dirs.extend([root / csv_dir_path for root in base_dirs])

    resolved_dir: Optional[Path] = None
    for cand in candidate_dirs:
        if cand.is_dir():
            resolved_dir = cand.resolve()
            break
    if resolved_dir is None:
        raise FileNotFoundError(f"CSV directory not found under roots: {csv_dir_path} (roots={base_dirs})")

    tag_list = [t for t in (tags or []) if str(t).strip()]
    timestamp = None
    results = {"tagged": 0, "skipped": 0, "missing": 0, "csvs": 0}

    for csv_path in sorted(resolved_dir.glob("*.csv")):
        results["csvs"] += 1
        targets = _extract_paths_from_csv(csv_path)
        if not targets:
            log.info("No valid paths found in %s", csv_path)
            continue
        for p in targets:
            if not p.exists():
                log.warning("Skipping missing file from CSV: %s", p)
                results["missing"] += 1
                continue
            tags_to_apply = list(tag_list)
            if timestamp is None:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M")
            tags_to_apply.insert(0, timestamp)
            if dry_run:
                log.info("[DRY-RUN] Would set user.xdg.tags=%s on %s", ",".join(tags_to_apply), p)
                results["skipped"] += 1
                continue
            try:
                # Clear existing tags then set new ones.
                write_fs_tag(p, "user.xdg.tags", "")
                if not write_fs_tag(p, "user.xdg.tags", ",".join(tags_to_apply)):
                    log.warning("Failed to tag %s with %s", p, ",".join(tags_to_apply))
                    results["skipped"] += 1
                else:
                    results["tagged"] += 1
            except Exception as exc:  # pragma: no cover - safety
                log.warning("Failed to update tags for %s: %s", p, exc)
                results["skipped"] += 1
    return results


__all__ = ["tag_files_from_csv_dir"]
