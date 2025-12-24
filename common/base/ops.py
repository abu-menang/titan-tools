"""
common.base.ops

Unified operational helpers for Titan Tools.

Enhancements:
 - Added dry-run support for destructive operations
 - Added optional real-time streaming for subprocess commands
 - Clearer error handling and logging integration
 - Compatible with existing domain modules
"""

from __future__ import annotations
import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Union, List, Dict, Tuple

from .logging import get_logger
from .fs import ensure_dir, ensure_parent, human_size

log = get_logger(__name__)


# ----------------------------------------------------------------------
# FILESYSTEM OPERATIONS
# ----------------------------------------------------------------------

def remove_file(path: Path | str, dry_run: bool = False) -> bool:
    """
    Safely remove a file. Returns True if removed, False if not found.

    Args:
        path: File path
        dry_run: Simulate delete without performing it
    """
    p = Path(path)
    if not p.exists():
        log.debug(f"File not found (skip delete): {p}")
        return False

    if dry_run:
        log.info(f"[DRY-RUN] Would delete file: {p}")
        return True

    try:
        p.unlink()
        log.debug(f"🗑️ Deleted file: {p}")
        return True
    except Exception as e:
        log.error(f"Failed to remove {p}: {e}")
        return False


def remove_dir(path: Path | str, dry_run: bool = False) -> bool:
    """
    Safely remove a directory and its contents.
    """
    p = Path(path)
    if not p.exists() or not p.is_dir():
        log.debug(f"Directory not found (skip delete): {p}")
        return False

    if dry_run:
        log.info(f"[DRY-RUN] Would delete directory: {p}")
        return True

    try:
        shutil.rmtree(p)
        log.debug(f"🗑️ Deleted directory: {p}")
        return True
    except Exception as e:
        log.error(f"Failed to remove directory {p}: {e}")
        return False


def move_to_trash(path: Path | str, dry_run: bool = False) -> bool:
    """
    Move a file or directory to OS Trash/Recycle Bin instead of deleting.
    """
    p = Path(path)
    if not p.exists():
        log.debug(f"File not found (skip trash): {p}")
        return False

    if dry_run:
        log.info(f"[DRY-RUN] Would move to trash: {p}")
        return True

    try:
        from send2trash import send2trash # type: ignore
        send2trash(str(p))
        log.debug(f"♻️ Moved to trash: {p}")
        return True
    except ImportError:
        log.warning("send2trash not installed; performing normal delete instead.")
        return remove_file(p)
    except Exception as e:
        log.error(f"Failed to move {p} to trash: {e}")
        return False


def copy_tree(src: Path | str, dst: Path | str, overwrite: bool = False, dry_run: bool = False) -> None:
    """
    Recursively copy directory tree with overwrite and dry-run options.
    """
    src, dst = Path(src), Path(dst)
    ensure_dir(dst)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            copy_tree(item, target, overwrite, dry_run)
        else:
            if target.exists() and not overwrite:
                log.warning(f"Skip existing file: {target}")
                continue
            if dry_run:
                log.info(f"[DRY-RUN] Would copy {item} → {target}")
            else:
                shutil.copy2(item, target)
                log.debug(f"Copied: {item} → {target}")


def move_file(src: Path, dst: Path, dry_run: bool = False) -> None:
    """
    Move a file safely with logging and dry-run support.

    Args:
        src: Source path.
        dst: Destination path.
        dry_run: Simulate the move without actually performing it.
    """
    src, dst = Path(src), Path(dst)
    ensure_parent(dst)

    if dry_run:
        log.info(f"[DRY-RUN] Would move {src} → {dst}")
        return

    try:
        shutil.move(str(src), str(dst))
        log.debug(f"Moved {src} → {dst}")
    except Exception as e:
        log.error(f"Move failed {src} → {dst}: {e}")
        raise

# ----------------------------------------------------------------------
# SHELL / SUBPROCESS HELPERS
# ----------------------------------------------------------------------

def run_command(
    cmd: Union[str, List[str]],
    cwd: Optional[Path | str] = None,
    capture: bool = True,
    stream: bool = False,
    check: bool = False,
    timeout: Optional[int] = None,
) -> Tuple[int, str, str]:
    """
    Execute a shell command with optional output capture or live streaming.

    Args:
        cmd: Command string or list
        cwd: Working directory
        capture: Capture stdout/stderr (ignored if stream=True)
        stream: Stream output in real time instead of capturing
        check: Raise if return code != 0
        timeout: Max seconds before killing process

    Returns:
        tuple: (exit_code, stdout, stderr)
    """
    shell_mode = isinstance(cmd, str)
    log.debug(f"▶️ Running command: {cmd} (cwd={cwd})")

    try:
        if stream:
            process = subprocess.Popen(
                cmd,
                cwd=str(cwd) if cwd else None,
                shell=shell_mode,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            output_lines: List[str] = []
            for line in process.stdout:  # type: ignore[union-attr]
                print(line.rstrip())
                output_lines.append(line.rstrip())
            process.wait(timeout=timeout)
            return (process.returncode, "\n".join(output_lines), "")
        else:
            result = subprocess.run(
                cmd,
                cwd=str(cwd) if cwd else None,
                shell=shell_mode,
                capture_output=capture,
                text=True,
                timeout=timeout,
                check=check,
            )
            out, err = result.stdout.strip(), result.stderr.strip()
            if result.returncode == 0:
                log.debug(f"✅ Command OK: {cmd}")
            else:
                log.warning(f"⚠️ Command returned {result.returncode}: {cmd}")
                if err:
                    log.debug(f"stderr: {err}")
            return result.returncode, out, err
    except subprocess.TimeoutExpired:
        log.error(f"⏱️ Command timed out: {cmd}")
        return (124, "", "timeout")
    except subprocess.CalledProcessError as e:
        log.error(f"❌ Command failed: {cmd} → {e}")
        return (e.returncode, e.output, e.stderr)
    except Exception as e:
        log.error(f"🚨 Error running {cmd}: {e}")
        return (1, "", str(e))


# ----------------------------------------------------------------------
# FILE INFORMATION
# ----------------------------------------------------------------------

def file_info(path: Path | str) -> Dict[str, Union[str, int, float]]:
    """
    Return metadata about a file.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)

    stat = p.stat()
    return {
        "path": str(p.resolve()),
        "size": stat.st_size,
        "size_human": human_size(stat.st_size),
        "modified": stat.st_mtime,
        "created": stat.st_ctime,
        "is_dir": p.is_dir(),
        "is_file": p.is_file(),
    }


# ----------------------------------------------------------------------
# SELF TEST
# ----------------------------------------------------------------------

if __name__ == "__main__":
    log.info("✅ common.base.ops self-test:")
    tmp = Path("./tmp_ops_test")
    ensure_dir(tmp)
    test_file = tmp / "test.txt"
    test_file.write_text("hello world")

    log.info(f"File info: {file_info(test_file)}")

    # Dry-run removal
    remove_file(test_file, dry_run=True)
    remove_dir(tmp, dry_run=True)

    # Command examples
    code, out, err = run_command(["echo", "Hello Titan!"])
    log.info(f"Command output: {out}")
    run_command("ls -l", stream=True)
