#!/usr/bin/env python3
"""
file-list.py
------------

Scan a directory recursively and produce CSV file(s) with the following columns:
 - path            : full path including filename and extension
 - filename        : filename only (without path or extension)
 - edited_filename : empty column (to be filled manually later)
 - metadata_title  : Title from file-level metadata (if available)

Options:
  --output-base DIR   Base directory for output (creates timestamped folder inside)
  --max-files N       Split output files into chunks of N files each.
                      By default, all files are listed in a single CSV.
  -h, --help          Show this help

Usage examples:
  file-list ./input_dir --output-base ./results
  file-list /mnt/media --output-base /mnt/reports --max-files 100

Installer:
  sudo ~/scripts/file-list.py install
  sudo ~/scripts/file-list.py uninstall

Notes:
 - Directories named like <timestamp>_<any string> (e.g. 20251004-123456_file_list)
   will be completely skipped (not scanned, not recursed).
 - Metadata title is retrieved using `ffprobe` (from ffmpeg package), supports many file types.
"""

import argparse
import csv
import shutil
import sys
import re
import logging
import subprocess
import json
import os
from pathlib import Path
from datetime import datetime
from math import ceil

# ---------------------- Helpers ----------------------

def now_ts():
    """Return current timestamp string like 20251004-123456"""
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def get_metadata_title(filepath, logger=None):
    """Return Title metadata using ffprobe (universal for many file types)"""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(filepath)],
            check=True,
            capture_output=True,
            text=True
        )
        meta = json.loads(result.stdout)
        return meta.get("format", {}).get("tags", {}).get("title", "") or ""
    except Exception as e:
        if logger:
            logger.warning(f"Metadata extraction failed for {filepath}: {e}")
        return ""

def scan_directory(input_dir, logger=None):
    """Recursively scan input_dir."""
    base = Path(input_dir).resolve()
    entries, skipped_files, failed, skipped_dirs = [], [], [], []
    timestamp_out_pattern = re.compile(r"^\d{8}-\d{6}_.+$")

    for root, dirs, files in os.walk(base):
        # skip timestamped dirs
        pruned = [d for d in dirs if timestamp_out_pattern.match(d)]
        for d in pruned:
            full_dir = Path(root) / d
            logger.info(f"Skipping directory: {full_dir}")
            skipped_dirs.append(str(full_dir))
        dirs[:] = [d for d in dirs if not timestamp_out_pattern.match(d)]

        for fname in files:
            f = Path(root) / fname
            if not f.is_file():
                skipped_files.append((str(f.resolve()), "Not a regular file"))
                continue
            fullpath = str(f.resolve())
            stem = f.stem
            try:
                meta_title = get_metadata_title(f, logger=logger)
                entries.append((fullpath, stem, "", meta_title))
            except Exception as e:
                failed.append((fullpath, str(e)))

    return entries, skipped_files, failed, skipped_dirs

def write_csv(entries, csv_path):
    """Write entries to CSV file"""
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["path", "filename", "edited_filename", "metadata_title"])
        writer.writerows(entries)

def split_and_write_csv(entries, run_dir, max_files, logger):
    """Split entries into chunks of max_files and write multiple CSVs"""
    total = len(entries)
    chunks = ceil(total / max_files)
    logger.info(f"Splitting {total} entries into {chunks} CSV file(s)")

    for i in range(chunks):
        start = i * max_files
        end = start + max_files
        chunk = entries[start:end]
        csv_path = run_dir / f"file_list_{i+1}.csv"
        write_csv(chunk, csv_path)
        logger.info(f"CSV chunk {i+1} written: {csv_path}")

def write_summary(run_dir, processed, skipped_files, failed, skipped_dirs, logger):
    """Write summary report"""
    summary_file = run_dir / "summary.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("======== SUMMARY ========\n")
        f.write(f"Processed successfully: {processed}\n")
        f.write(f"Skipped directories   : {len(skipped_dirs)}\n")
        f.write(f"Skipped files        : {len(skipped_files)}\n")
        f.write(f"Failed files         : {len(failed)}\n")
        f.write("=========================\n\n")

        if skipped_dirs:
            f.write("---- Skipped Directories ----\n")
            for d in skipped_dirs:
                f.write(f"{d}\n")
            f.write("\n")

        if skipped_files:
            f.write("---- Skipped Files ----\n")
            for path, reason in skipped_files:
                f.write(f"{path} | {reason}\n")
            f.write("\n")

        if failed:
            f.write("---- Failed Files ----\n")
            for path, reason in failed:
                f.write(f"{path} | {reason}\n")
            f.write("\n")

    logger.info("======== SUMMARY ========")
    logger.info(f"Processed successfully: {processed}")
    logger.info(f"Skipped dirs: {len(skipped_dirs)}, skipped files: {len(skipped_files)}, failed: {len(failed)}")
    logger.info(f"Summary written: {summary_file}")

# ---------------------- Installer helpers ----------------------

def install_self(target='/usr/local/bin/file-list'):
    """Install script"""
    stale_paths = [
        Path("/usr/local/bin/file-list"),
        Path("/usr/local/sbin/file-list"),
        Path.home() / ".local/bin/file-list"
    ]
    for sp in stale_paths:
        if sp.exists():
            try:
                sp.unlink()
                print(f"Removed stale copy: {sp}")
            except Exception as e:
                print(f"Warning: could not remove {sp}: {e}")

    src = Path(__file__).resolve()
    tgt = Path(target)
    tgt.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, tgt)
    tgt.chmod(0o755)
    print(f"Installed to {tgt}")
    return tgt

def uninstall_self(target='/usr/local/bin/file-list'):
    """Uninstall script"""
    removed = False
    for path in [
        Path(target),
        Path("/usr/local/sbin/file-list"),
        Path.home() / ".local/bin/file-list"
    ]:
        if path.exists():
            try:
                path.unlink()
                print(f"Removed {path}")
                removed = True
            except Exception as e:
                print(f"Warning: could not remove {path}: {e}")
    return removed

# ---------------------- Main ----------------------

def main():
    parser = argparse.ArgumentParser(prog="file-list", add_help=False)
    parser.add_argument("input_dir", nargs="?", help="Directory to scan")
    parser.add_argument("--output-base", help="Base directory for output")
    parser.add_argument("--max-files", type=int, default=0, help="Split output files into chunks of N files each")
    parser.add_argument("-h", "--help", action="help", help="Show this help")
    parser.add_argument("command", nargs="?", help="Optional: 'install' or 'uninstall'")
    args = parser.parse_args()

    # Installer
    if args.input_dir in ("install", "--install"):
        install_self(); sys.exit(0)
    if args.input_dir in ("uninstall", "--uninstall"):
        sys.exit(0 if uninstall_self() else 1)

    if not args.input_dir:
        print(__doc__)
        sys.exit(2)

    input_dir = Path(args.input_dir).resolve()
    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}")
        sys.exit(2)

    # Output setup
    base_out = Path(args.output_base or Path.cwd())
    run_dir = base_out / f"{now_ts()}_file_list"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Logging
    log_path = run_dir / "file-list-log.txt"
    logger = logging.getLogger("file-list")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Run scan
    logger.info(f"Scanning directory: {input_dir}")
    entries, skipped_files, failed, skipped_dirs = scan_directory(input_dir, logger=logger)

    if args.max_files > 0:
        split_and_write_csv(entries, run_dir, args.max_files, logger)
    else:
        csv_path = run_dir / "file_list.csv"
        write_csv(entries, csv_path)
        logger.info(f"CSV written: {csv_path}")

    # Summary
    write_summary(run_dir, len(entries), skipped_files, failed, skipped_dirs, logger)
    logger.info(f"All outputs in: {run_dir}")

if __name__ == "__main__":
    main()
