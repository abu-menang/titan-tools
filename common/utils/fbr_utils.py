from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Union

from common.base.fs import ensure_dir


def _unique_dir(base: Path, name: str) -> Path:
    candidate = base / name
    counter = 1
    while candidate.exists():
        candidate = base / f"{name}_{counter:02d}"
        counter += 1
    ensure_dir(candidate)
    return candidate


def prepare_clean_run_dirs(
    output_dir: Path,
    cleaned_dir_name: str,
    run_prefix: str = "clean_helper",
) -> Tuple[Path, Path, str]:
    """
    Prepare the report run directory and destination for cleaned files.

    Returns (run_dir, cleaned_output_dir, run_stamp).
    """
    ensure_dir(output_dir)

    cleaned_name = cleaned_dir_name
    run_stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

    # Logs and CSVs go under a persistent logs/ directory
    run_dir = ensure_dir(output_dir / "logs")
    cleaned_output_dir = _unique_dir(output_dir, f"{cleaned_name}_{run_stamp}")
    return run_dir, cleaned_output_dir, run_stamp


def _unique_file(base: Path, src: Path) -> Path:
    candidate = base / src.name
    counter = 1
    while candidate.exists():
        candidate = base / f"{src.stem}_{counter}{src.suffix}"
        counter += 1
    return candidate


def move_cleaned_files(
    replacements: Iterable[Union[Tuple[str, str], Tuple[str, str, str]]],
    ori_dir: Path,
    *,
    dry_run: bool = False,
    logger=None,
) -> List[Tuple[str, str]]:
    """
    Archive originals to `ori_dir` and move cleaned files into their place.

    Returns a list of (source, destination) moves performed or that would be performed in dry-run mode.
    """
    ori_dir = ensure_dir(ori_dir)
    moves: List[Tuple[str, str]] = []

    def _log(level: str, message: str, *args):
        if logger and hasattr(logger, level):
            getattr(logger, level)(message, *args)
        else:
            print(message % args if args else message)

    for repl in replacements:
        if len(repl) == 3:
            original, cleaned, dest_override = repl  # type: ignore[misc]
        else:
            original, cleaned = repl  # type: ignore[misc]
            dest_override = original
        orig_path = Path(original)
        cleaned_path = Path(cleaned)
        dest_backup = _unique_file(ori_dir, orig_path)
        dest_final = Path(dest_override)

        if dry_run:
            _log("info", "[DRY-RUN] Would move original %s -> %s", orig_path, dest_backup)
            _log("info", "[DRY-RUN] Would move cleaned %s -> %s", cleaned_path, dest_final)
            moves.append((str(orig_path), str(dest_backup)))
            moves.append((str(cleaned_path), str(dest_final)))
            continue

        try:
            ensure_dir(dest_backup.parent)
            if orig_path.exists():
                orig_path.rename(dest_backup)
                moves.append((str(orig_path), str(dest_backup)))
                _log("info", "Moved original %s -> %s", orig_path, dest_backup)
            else:
                _log("warning", "Original file missing, cannot archive: %s", orig_path)

            ensure_dir(dest_final.parent)
            if cleaned_path.exists():
                cleaned_path.rename(dest_final)
                moves.append((str(cleaned_path), str(dest_final)))
                _log("info", "Moved cleaned %s -> %s", cleaned_path, dest_final)
            else:
                _log("warning", "Cleaned file missing, cannot move into place: %s", cleaned_path)
        except Exception as exc:
            _log("error", "Failed to move files for %s: %s", orig_path.name, exc)

    return moves


__all__ = ["prepare_clean_run_dirs", "move_cleaned_files"]
