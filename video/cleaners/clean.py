"""
Discover scan output CSVs under clean_dir and invoke clean_helper to process them.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from common.base.logging import get_logger
from common.shared.loader import load_output_dirs
from helpers.clean_helper import clean_with_tracks_csv
from common.utils.fbr_utils import move_cleaned_files, prepare_clean_run_dirs
from common.utils.tag_utils import write_fs_tag
from common.utils.csv_utils import discover_csvs
from datetime import datetime

log = get_logger(__name__)


def discover_clean_csvs(
    roots: Iterable[Path | str],
    output_root: Optional[Path | str] = None,
    clean_dir_key: str = "clean_dir",
) -> List[Path]:
    roots_resolved = [Path(r).expanduser().resolve() for r in roots]
    cfg = load_output_dirs()
    clean_dir = cfg.get(clean_dir_key)
    target_dir = clean_dir or ""
    return discover_csvs(roots_resolved, output_root, target_dir)


def run_cleaner(
    roots: Optional[Iterable[Path | str]] = None,
    output_root: Optional[Path | str] = None,
    output_dir: Optional[Path | str] = None,
    dry_run: bool = False,
) -> None:
    roots = list(roots or [Path.cwd()])
    csvs = discover_clean_csvs(roots, output_root)
    if not csvs:
        log.info("No clean_dir CSVs found.")
        return

    cfg = load_output_dirs()
    cleaned_dir_name = str(cfg.get("temp_dir") or "temp")

    def _resolve_output_dir() -> Path:
        if output_dir:
            return Path(output_dir).expanduser()
        if output_root:
            return Path(output_root).expanduser()
        return Path("./reports").expanduser().resolve()

    out_dir = _resolve_output_dir()
    per_dir_state: dict[Path, tuple[Path, Path, str, Path]] = {}

    for csv_path in csvs:
        log.info("Processing tracks CSV: %s", csv_path)
        csv_dir = csv_path.parent.resolve()
        if csv_dir not in per_dir_state:
            run_dir, clean_output_dir, run_stamp = prepare_clean_run_dirs(csv_dir, cleaned_dir_name)
            ori_dir = (csv_dir / "ori").expanduser()
            per_dir_state[csv_dir] = (run_dir, clean_output_dir, run_stamp, ori_dir)
        else:
            run_dir, clean_output_dir, run_stamp, ori_dir = per_dir_state[csv_dir]
        res = clean_with_tracks_csv(
            csv_path,
            output_dir=out_dir,
            dry_run=dry_run,
            run_dir=run_dir,
            clean_output_dir=clean_output_dir,
        )
        # Apply tag per cleaned file
        tag_val = datetime.now().strftime("%Y_%m_%d-%H_%M")
        for cleaned in res.get("cleaned", []):
            p = Path(cleaned)
            if dry_run:
                log.info("[DRY-RUN] Would set user.xdg.tags=%s on %s", tag_val, p)
            else:
                if not write_fs_tag(p, "user.xdg.tags", tag_val):
                    log.warning("Failed to tag %s with user.xdg.tags=%s", p, tag_val)
        move_cleaned_files(res.get("replacements", []), ori_dir, dry_run=dry_run, logger=log)
        # Write summary
        if run_dir:
            summary_path = Path(run_dir) / f"summary_{run_stamp}_{csv_path.stem}.txt"
            clean_output_dir = res.get("clean_output_dir")
            with open(summary_path, "w") as handle:
                handle.write("======== SUMMARY ========\n")
                handle.write(f"Tracks CSV          : {res.get('tracks_csv')}\n")
                handle.write(f"Clean output dir    : {clean_output_dir}\n")
                handle.write(f"Total files listed  : {len(res.get('results', []))}\n")
                handle.write(f"Cleaned             : {len(res.get('cleaned', []))}\n")
                handle.write(f"Dry-run             : {len(res.get('dry_run', []))}\n")
                handle.write(f"No change needed    : {len(res.get('nochange', []))}\n")
                handle.write(f"Missing             : {len(res.get('missing', []))}\n")
                handle.write(f"Failed              : {len(res.get('failed', []))}\n")
                handle.write(f"Run dir             : {run_dir}\n")
            log.info("Summary written to %s", summary_path)


if __name__ == "__main__":
    run_cleaner()
