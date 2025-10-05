#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
file-renamer.py
----------------

Safely rename files and update Title metadata using CSVs produced by file-lister.

Features:
 - Reads CSV with columns: path, filename, edited_filename, title, edited_title
 - Performs renames and/or ffmpeg Title metadata updates
 - Generates revert.csv and summary.txt
 - Supports dry-run simulation
 - Logs all actions (file-renamer-log.txt)
 - Integrates with Titan Tools for shared utilities

=====================================================================
⚙️ Command Options
=====================================================================

--input-file, --if          Path to CSV file (from file-lister)
--output-base-dir, --obd    Base directory for <timestamp>_renamer folder
--dry-run, --dr             Simulate renames and metadata updates (no writes)
--verbose, -vb              Increase log verbosity
--install                   Install system-wide to /usr/local/bin/file-renamer
--uninstall                 Remove installed copies
--version                   Show script + Titan Tools version
-h, --help                  Show this help message
=====================================================================
"""

# =====================================================
# Titan Tools Import Guard
# =====================================================
try:
    from titan_tools.common import color_text, now_ts, print_progress
    from titan_tools.logger import setup_logger
    from titan_tools.installer import install_self, uninstall_self
    from titan_tools.metadata import get_metadata_title
    from titan_tools.output import make_output_dir
except ModuleNotFoundError:
    import sys
    from pathlib import Path
    home = str(Path.home())
    print("\033[93m⚠️  Titan Tools library not found.\033[0m")
    print("\033[93m   Installed scripts will not run unless titan_tools is accessible.\033[0m\n")
    print("\033[96m💡 To fix this, add the following line to your shell profile:\033[0m")
    print(f"\033[92m   export PYTHONPATH=\"$PYTHONPATH:{home}/scripts\"\033[0m\n")
    print("\033[96mThen reload your shell or run:\033[92m source ~/.bashrc\033[0m\n")
    sys.exit(1)

# =====================================================
# Standard Library Imports
# =====================================================
import os
import csv
import sys
import time
import shutil
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

TITAN_VERSION = "1.0.0"

# =====================================================
# Core Operations
# =====================================================

def update_metadata_title(file_path: Path, new_title: str, logger, dry_run=False):
    """Safely update Title metadata using ffmpeg copy."""
    if dry_run:
        logger.info(color_text(f"[DRY-RUN] Would update metadata title: {file_path} -> {new_title}", "cyan"))
        return

    tmp_file = file_path.with_name(file_path.stem + ".tmp" + file_path.suffix)
    cmd = [
        "ffmpeg", "-y", "-i", str(file_path),
        "-map", "0", "-c", "copy",
        "-metadata", f"title={new_title}", str(tmp_file)
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        if tmp_file.exists():
            tmp_file.unlink()
        raise RuntimeError(f"Metadata update failed: {e.stderr.decode(errors='ignore')}")

    tmp_file.replace(file_path)
    logger.info(color_text(f"Updated metadata title: {file_path.name} -> {new_title}", "green"))


def rename_file(orig_path: Path, new_name: str, logger, dry_run=False):
    """Rename file in the same directory."""
    new_path = orig_path.with_name(new_name)
    if orig_path == new_path:
        return orig_path
    if dry_run:
        logger.info(color_text(f"[DRY-RUN] Would rename: {orig_path.name} -> {new_name}", "yellow"))
        return new_path
    orig_path.rename(new_path)
    logger.info(color_text(f"Renamed: {orig_path.name} -> {new_name}", "green"))
    return new_path


def process_csv(csv_file: Path, run_dir: Path, logger, dry_run=False):
    """Read CSV, perform renames/metadata updates, write revert + summary."""
    summary_path = run_dir / "summary.txt"
    revert_path = run_dir / "revert.csv"

    renamed, meta_updated, skipped, failed = [], [], [], []

    with open(csv_file, newline="", encoding="utf-8") as f, \
         open(revert_path, "w", newline="", encoding="utf-8") as revert_f:
        reader = csv.DictReader(f)
        fieldnames = ["path", "edited_filename", "metadata_title"]
        writer = csv.DictWriter(revert_f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()

        rows = list(reader)
        total = len(rows)
        logger.info(color_text(f"📄 CSV entries loaded: {total}", "cyan"))
        print(color_text(f"\n📊 Processing entries ({total} total):", "cyan"))

        for idx, row in enumerate(rows, start=1):
            print_progress(idx, total)
            try:
                orig_path = Path(row.get("path", "")).resolve()
                edited_filename = row.get("edited_filename", "").strip()
                new_title = row.get("edited_title", "").strip() or row.get("title", "").strip()

                if not orig_path.exists():
                    failed.append((str(orig_path), "File not found"))
                    continue

                old_title = get_metadata_title(orig_path, logger)
                needs_rename = bool(edited_filename and edited_filename != orig_path.name)
                needs_meta = bool(new_title and new_title != old_title)

                if not needs_rename and not needs_meta:
                    skipped.append((str(orig_path), "No changes detected"))
                    continue

                new_path = orig_path
                if needs_rename:
                    new_path = rename_file(orig_path, edited_filename, logger, dry_run=dry_run)
                    renamed.append((str(orig_path), str(new_path)))

                if needs_meta:
                    try:
                        update_metadata_title(new_path, new_title, logger, dry_run=dry_run)
                        meta_updated.append((str(new_path), new_title))
                    except Exception as e:
                        failed.append((str(new_path), f"Metadata update failed: {e}"))
                        continue

                writer.writerow({
                    "path": str(new_path),
                    "edited_filename": orig_path.name,
                    "metadata_title": old_title
                })

            except Exception as e:
                failed.append((row.get("path", "?"), str(e)))

    # Summary file
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("======== SUMMARY ========\n")
        f.write(f"✅ Renamed          : {len(renamed)}\n")
        f.write(f"🎵 Metadata Updated : {len(meta_updated)}\n")
        f.write(f"⚠️  Skipped          : {len(skipped)}\n")
        f.write(f"❌ Failed           : {len(failed)}\n")
        f.write("=========================\n\n")

        def section(title, entries):
            if entries:
                f.write(f"---- {title} ({len(entries)}) ----\n")
                for e in entries:
                    f.write(" → ".join(map(str, e)) + "\n")
                f.write("\n")

        section("✅ Renamed", renamed)
        section("🎵 Metadata Updated", meta_updated)
        section("⚠️ Skipped", skipped)
        section("❌ Failed", failed)
        f.write(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Console summary
    logger.info(color_text("======== SUMMARY ========", "cyan"))
    logger.info(color_text(f"✅ Renamed          : {len(renamed)}", "green"))
    logger.info(color_text(f"🎵 Metadata Updated : {len(meta_updated)}", "cyan"))
    logger.info(color_text(f"⚠️ Skipped          : {len(skipped)}", "yellow"))
    logger.info(color_text(f"❌ Failed           : {len(failed)}", "red"))
    logger.info(color_text(f"Summary written: {summary_path}", "magenta"))

    print(color_text("\n✅ Processing complete.", "green"))
    print(color_text(f"📂 Results saved to: {run_dir}", "cyan"))


# =====================================================
# Main Entry Point
# =====================================================
def main():
    parser = argparse.ArgumentParser(description="Rename files and update metadata titles using CSV from file-lister.")
    parser.add_argument("--input-file", "--if", required=False, help="CSV file (from file-lister).")
    parser.add_argument("--output-base-dir", "--obd", default=None, help="Base output directory for <timestamp>_renamer.")
    parser.add_argument("--dry-run", "--dr", action="store_true", help="Simulate changes without writing.")
    parser.add_argument("--verbose", "-vb", action="store_true", help="Enable verbose logging (DEBUG).")
    parser.add_argument("--install", action="store_true", help="Install system-wide.")
    parser.add_argument("--uninstall", action="store_true", help="Uninstall system copy.")
    parser.add_argument("--version", action="store_true", help="Show script and Titan Tools version info.")
    args = parser.parse_args()

    if args.version:
        print(f"file-renamer  v{TITAN_VERSION}")
        print("Titan Tools   v1.0.0")
        sys.exit(0)

    if args.install:
        sys.exit(install_self("file-renamer"))
    if args.uninstall:
        sys.exit(uninstall_self("file-renamer"))

    if not args.input_file:
        parser.print_help()
        sys.exit(2)

    target_csv = Path(args.input_file).resolve()
    if not target_csv.exists():
        print(color_text(f"❌ Input CSV not found: {target_csv}", "red"))
        sys.exit(1)

    base_out = Path(args.output_base_dir).resolve() if args.output_base_dir else target_csv.parent
    run_dir = make_output_dir(base_out, "renamer")

    log_path = run_dir / "file-renamer-log.txt"
    logger = setup_logger("file-renamer", log_path)
    if args.verbose:
        logger.setLevel("DEBUG")

    start = time.time()
    try:
        process_csv(target_csv, run_dir, logger, dry_run=args.dry_run)
    except KeyboardInterrupt:
        logger.warning(color_text("⚠️  Interrupted by user.", "yellow"))
    except Exception as e:
        logger.error(color_text(f"💥 Unexpected error: {e}", "red"))
    finally:
        elapsed = time.time() - start
        mins, secs = divmod(elapsed, 60)
        print(color_text(f"🕒 Time taken: {int(mins)}m {secs:.2f}s", "cyan"))


if __name__ == "__main__":
    main()
