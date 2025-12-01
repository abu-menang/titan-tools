"""
common.shared.utils

Common reusable utilities shared across Titan Tools modules.

Enhancements:
 - Added dry-run support for move/copy/remove functions
 - Added path_exists() helper
 - Improved doc clarity and logging safety
"""

from __future__ import annotations
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Any

from tqdm import tqdm
from common.base.fs import ensure_dir, human_size


# ----------------------------------------------------------------------
# PATH UTILITIES
# ----------------------------------------------------------------------

def safe_filename(name: str) -> str:
    """
    Sanitize a filename by replacing invalid characters with underscores.
    """
    invalid = '<>:"/\\|?*\n\r\t'
    for ch in invalid:
        name = name.replace(ch, "_")
    return name.strip()


def path_exists(path: Path | str) -> bool:
    """
    Check if a given path exists.
    Returns True if it exists (file or directory), False otherwise.
    """
    return Path(path).expanduser().exists()


# ----------------------------------------------------------------------
# FILE OPERATIONS
# ----------------------------------------------------------------------

def move_file(src: Path | str, dst: Path | str, overwrite: bool = False, dry_run: bool = False) -> None:
    """
    Move a file safely to a new location. Creates destination dirs if needed.

    Args:
        src: Source file
        dst: Destination file
        overwrite: Overwrite existing destination file
        dry_run: Simulate action without performing it
    """
    src_path = Path(src)
    dst_path = Path(dst)
    ensure_dir(dst_path.parent)

    if not src_path.exists():
        raise FileNotFoundError(f"Source not found: {src_path}")

    if dst_path.exists():
        if not overwrite:
            raise FileExistsError(f"Destination exists: {dst_path}")
        if dry_run:
            print(f"[DRY-RUN] Would overwrite: {dst_path}")
            return
        dst_path.unlink()

    if dry_run:
        print(f"[DRY-RUN] Would move {src_path} → {dst_path}")
        return

    shutil.move(str(src_path), str(dst_path))


def copy_file(src: Path | str, dst: Path | str, overwrite: bool = False, dry_run: bool = False) -> None:
    """
    Copy a file safely to a new location. Creates destination dirs if needed.
    """
    src_path = Path(src)
    dst_path = Path(dst)
    ensure_dir(dst_path.parent)

    if not src_path.exists():
        raise FileNotFoundError(f"Source not found: {src_path}")

    if dst_path.exists() and not overwrite:
        raise FileExistsError(f"Destination exists: {dst_path}")

    if dry_run:
        print(f"[DRY-RUN] Would copy {src_path} → {dst_path}")
        return

    shutil.copy2(src_path, dst_path)


def remove_file(path: Path | str, dry_run: bool = False) -> bool:
    """
    Remove a file safely.
    Returns True if removed, False otherwise.
    """
    p = Path(path)
    if not p.exists():
        print(f"[INFO] File not found: {p}")
        return False

    if dry_run:
        print(f"[DRY-RUN] Would delete: {p}")
        return True

    try:
        p.unlink()
        print(f"🗑️ Deleted file: {p}")
        return True
    except Exception as e:
        print(f"⚠️ Failed to remove {p}: {e}")
        return False


# ----------------------------------------------------------------------
# PROGRESS + TIMING HELPERS
# ----------------------------------------------------------------------

class Progress:
    """
    Simple wrapper for tqdm progress bars that automatically closes
    on completion or interruption.
    """

    def __init__(self, iterable: Iterable[Any], desc: str = "Processing"):
        self._tqdm = tqdm(iterable, desc=desc, ncols=100, leave=False, dynamic_ncols=True)

    def __iter__(self):
        for item in self._tqdm:
            yield item
        self._tqdm.close()

    def update(self, n: int = 1):
        self._tqdm.update(n)

    def close(self):
        self._tqdm.close()

    def write(self, message: str):
        """Print a message above the progress bar on its own line."""
        self._tqdm.write(message)


def timeit(func):
    """
    Decorator to measure execution time of a function.
    Example:
        @timeit
        def heavy_task():
            ...
    """
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"⏱️ {func.__name__} took {elapsed:.2f}s")
        return result

    return wrapper


# ----------------------------------------------------------------------
# TERMINAL UTILITIES
# ----------------------------------------------------------------------

def clear_console() -> None:
    """Clear the current terminal screen."""
    os.system("cls" if os.name == "nt" else "clear")


def confirm(prompt: str) -> bool:
    """
    Simple yes/no confirmation prompt.
    """
    resp = input(f"{prompt} [y/N]: ").strip().lower()
    return resp == "y"


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    print("✅ common.shared.utils self-test:")
    tmp_dir = ensure_dir("./tmp_test")
    print(" - Directory created:", tmp_dir)
    print(" - Safe filename:", safe_filename("bad:/file*name?.mkv"))
    print(" - Human size:", human_size(123456789))
    move_file("test.txt", tmp_dir / "test.txt", dry_run=True)
