#!/usr/bin/env python3
"""
Titan Tools Installer
---------------------
Handles installation and uninstallation of Titan-based tools.
Now Git-aware: skips local backup creation if a .git folder is found.
"""

import shutil
import os
import sys
from pathlib import Path
from titan_tools.common import color_text, now_ts


def _in_git_repo(path: Path) -> bool:
    """Return True if the given path is inside a Git repository."""
    try:
        for parent in [path] + list(path.parents):
            if (parent / ".git").exists():
                return True
        return False
    except Exception:
        return False


def install_self(script_name, backup_enabled=True):
    """
    Install script to /usr/local/bin/{script_name}.

    Behavior:
      - Detects script source path (e.g., ~/scripts/media/).
      - If /usr/local/bin/{script_name} exists:
          * Prompt for backup (unless Git repo detected).
          * Stores backup under <source_dir>/backup/{script_name}_bkup_<timestamp>.py
      - Removes old installs in common paths.
      - Copies new script to /usr/local/bin/{script_name}.
      - Makes it executable.
      - Prints helpful, colorized messages.
    """
    print(color_text(f"🧹 Preparing to install {script_name}...", "cyan"))

    # --- Determine source script location ---
    src = Path(__file__).resolve().parent.parent / f"{script_name}.py"

    if not src.exists():
        import inspect
        try:
            caller = Path(inspect.stack()[-1].filename).resolve()
            src = caller
        except Exception:
            print(color_text("❌ Unable to detect script source file.", "red"))
            return 1

    src_dir = src.parent
    backup_dir = src_dir / "backup"
    target = Path(f"/usr/local/bin/{script_name}")

    # --- Detect Git repo ---
    in_git = _in_git_repo(src_dir)
    if in_git:
        print(color_text("💡 Git repository detected — skipping backup (Git handles versioning).", "cyan"))

    # --- Backup existing file if applicable ---
    if target.exists() and backup_enabled and not in_git:
        print(color_text(f"⚠️  A version of {script_name} already exists in /usr/local/bin.", "yellow"))
        choice = input("Would you like to back it up before replacing? (y/n): ").strip().lower()
        if choice == "y":
            backup_dir.mkdir(exist_ok=True)
            backup_file = backup_dir / f"{script_name}_bkup_{now_ts()}.py"
            try:
                shutil.copy2(target, backup_file)
                print(color_text(f"🗄️  Backed up existing version to: {backup_file}", "green"))
            except Exception as e:
                print(color_text(f"⚠️  Backup failed: {e}", "red"))

    # --- Remove old installs ---
    for stale in [
        Path(f"/usr/local/bin/{script_name}"),
        Path(f"/usr/local/sbin/{script_name}"),
        Path.home() / ".local" / "bin" / script_name,
    ]:
        if stale.exists():
            try:
                stale.unlink()
                print(f"✅ Removed old {stale}")
            except Exception as e:
                print(f"⚠️ Could not remove {stale}: {e}")

    # --- Copy new version ---
    try:
        shutil.copy2(src, target)
        os.chmod(target, 0o755)
        print(color_text(f"✅ Installed {script_name} to {target}", "green"))
        return 0
    except Exception as e:
        print(color_text(f"❌ Install failed: {e}", "red"))
        return 1


def uninstall_self(script_name):
    """Uninstall script from /usr/local/bin, /usr/local/sbin, and ~/.local/bin."""
    print(color_text(f"🧹 Uninstalling {script_name}...", "cyan"))
    removed = False

    for p in [
        Path(f"/usr/local/bin/{script_name}"),
        Path(f"/usr/local/sbin/{script_name}"),
        Path.home() / ".local" / "bin" / script_name,
    ]:
        if p.exists():
            try:
                p.unlink()
                print(f"✅ Removed {p}")
                removed = True
            except Exception as e:
                print(f"⚠️ Could not remove {p}: {e}")

    if removed:
        print(color_text("✅ Uninstallation complete.", "green"))
    else:
        print("⚠️ No installed copies found.")
    return 0
