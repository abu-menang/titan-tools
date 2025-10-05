#!/usr/bin/env python3
"""
mkv-scan.py
-----------

A robust MKV metadata scanner using mkvmerge.

This script recursively scans directories for MKV files and extracts detailed
track metadata for video, audio, and subtitle streams.

It outputs track information in either CSV or JSON format, with optional batch
splitting, live progress bar, and comprehensive summary reporting.

=====================================================================
⚙️ Command Options
=====================================================================

Option:           --input-dir / --id
Type:             String
Default:          directory where command is run
Description:      The root folder to recursively scan for .mkv files.

Option:           --filename-base / --fb
Type:             String
Default:          tracks
Description:      Base name for output files (without extension).
                  Example: --filename-base anime → anime.csv or anime.json.

Option:           --file-type / --ft
Type:             csv / json
Default:          csv
Description:      Format of the main output file. Controls whether results are
                  written in CSV or JSON.

Option:           --output-base-dir / --obd
Type:             Path
Default:          Same as --input-dir, or current directory if not given
Description:      Directory in which to create the output folder named
                  <timestamp>_scan.

Option:           --batch-size / --bs
Type:             Integer
Default:          0 (disabled)
Description:      Split output into batches of N files per report.
                  Example: --batch-size 10 → tracks_batch_01.csv, batch_02.csv, etc.

Special Commands:
  install         Installs the script system-wide to /usr/local/bin/mkv-scan,
                  removing old copies from /usr/local/sbin and ~/.local/bin.

  uninstall       Removes all installed copies of mkv-scan from common paths.

=====================================================================
💡 Usage Examples
=====================================================================

1. Basic scan (default CSV output)
   mkv-scan --id ./movies

2. JSON output, custom file base name
   mkv-scan --id ./anime --fb anime --ft json

3. Output results to another directory
   mkv-scan --id ./anime --obd ./reports

4. Split results into multiple files (10 files per batch)
   mkv-scan --id ./shows --bs 10

5. Install or uninstall system-wide
   sudo mkv-scan install
   sudo mkv-scan uninstall

=====================================================================
"""

import os
import re
import csv
import sys
import json
import time
import logging
import subprocess
import shutil
import argparse
from datetime import datetime
from pathlib import Path


# =====================================================
# Utility Helpers
# =====================================================

def color_text(text, color):
    colors = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "cyan": "\033[96m",
        "reset": "\033[0m",
    }
    return f"{colors.get(color, '')}{text}{colors['reset']}"

def now_ts():
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def print_progress(current, total, width=40):
    if total <= 0:
        return
    percent = (current / total) * 100
    filled = int(width * current // total)
    bar = "#" * filled + "-" * (width - filled)
    sys.stdout.write(f"\r[{bar}] {percent:5.1f}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")


# =====================================================
# mkvmerge Parsing
# =====================================================

def run_mkvmerge_json(filepath):
    """Run mkvmerge -J and parse JSON output."""
    cmd = ["mkvmerge", "-J", str(filepath)]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(res.stdout)
    except subprocess.CalledProcessError as e:
        return f"mkvmerge error: {e.stderr.strip() or e}"
    except json.JSONDecodeError:
        return "invalid JSON"
    except Exception as e:
        return f"exception: {e}"

def parse_mkvmerge_json(file_path, info):
    """Extract track metadata."""
    if isinstance(info, str):
        return None, info
    if not info or "tracks" not in info:
        return None, "no track data"

    entries = []
    base_name = Path(file_path).stem  # filename without path or extension

    for t in info["tracks"]:
        ttype = t.get("type")
        track_id = t.get("id")
        codec = t.get("codec", "")
        lang = t.get("properties", {}).get("language", "und")
        name = t.get("properties", {}).get("track_name", "")
        default = str(t.get("properties", {}).get("default_track", False)).lower()
        forced = str(t.get("properties", {}).get("forced_track", False)).lower()

        # Suggested rename logic
        if ttype == "video":
            suggested_rename = base_name
        elif ttype in ("audio", "subtitles"):
            lang_code = lang.upper() if len(lang) == 3 else lang[:3].upper()
            suggested_rename = f"{lang_code} ({codec})"
        else:
            suggested_rename = ""

        entries.append({
            "file": str(file_path),
            "type": ttype,
            "id": track_id,
            "codec": codec,
            "lang": lang,
            "name": name,
            "suggested_rename": suggested_rename,
            "default": default,
            "forced": forced
        })
    return entries, None


# =====================================================
# Writers
# =====================================================

def write_csv(output_path, entries, logger):
    headers = ["file", "type", "id", "codec", "lang", "name", "suggested_rename", "default", "forced"]
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for i, block in enumerate(entries):
            for row in block:
                writer.writerow(row)
            if i < len(entries) - 1:
                fh.write("\n\n")  # two blank lines as separator
    logger.info(f"✅ CSV written: {output_path}")

def write_json(output_path, entries, logger):
    flat = [row for block in entries for row in block]
    flat.sort(key=lambda x: str(x["file"]).lower())
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(flat, f, indent=2, ensure_ascii=False)
    logger.info(f"✅ JSON written: {output_path}")


# =====================================================
# Scanner
# =====================================================

def is_timestamped_dir(path: Path):
    """Detect <timestamp>_something directories."""
    return bool(re.match(r"^\d{8,}[_-].*", path.name))

def scan_directory(target_dir, logger):
    """Recursively scan for MKV files."""
    skipped_videos, skipped_others, skipped_dirs, failed_files, non_hevc, results = [], [], [], [], [], []
    ignored_roots = set()
    video_exts = {".mp4", ".avi", ".mov", ".wmv", ".flv", ".m4v", ".webm", ".ts", ".m2ts", ".3gp"}

    all_files = []
    for root, dirs, files in os.walk(target_dir, topdown=True):
        root_path = Path(root)
        timestamped = [d for d in dirs if is_timestamped_dir(Path(d))]
        if timestamped:
            for ig in timestamped:
                ignored_roots.add(str((root_path / ig).resolve()))
            dirs[:] = [d for d in dirs if d not in timestamped]
        for name in files:
            path = (root_path / name).resolve()
            if not any(str(path).startswith(ignored) for ignored in ignored_roots):
                all_files.append(path)

    total_files = len(all_files)
    processed = 0
    print(color_text(f"\n📊 Scanning progress ({total_files} files total):", "cyan"))

    for root, dirs, files in os.walk(target_dir, topdown=True):
        root_path = Path(root)
        timestamped = [d for d in dirs if is_timestamped_dir(Path(d))]
        if timestamped:
            for ig in timestamped:
                ignored_roots.add(str((root_path / ig).resolve()))
                msg = f"⏩ Ignored timestamped dir (excluded): {root_path/ig}"
                print(color_text(msg, "yellow"))
                logger.info(msg)
            dirs[:] = [d for d in dirs if d not in timestamped]

        try:
            current_files = list(files)
        except Exception as e:
            skipped_dirs.append(str(root_path))
            logger.warning(color_text(f"⚠️ Could not access {root_path}: {e}", "yellow"))
            continue

        for name in current_files:
            path = (root_path / name).resolve()
            if any(str(path).startswith(ignored) for ignored in ignored_roots):
                processed += 1
                print_progress(processed, total_files)
                continue

            ext = path.suffix.lower()
            if ext != ".mkv":
                (skipped_videos if ext in video_exts else skipped_others).append(str(path))
                processed += 1
                print_progress(processed, total_files)
                continue

            start = time.time()
            info = run_mkvmerge_json(path)
            if isinstance(info, str):
                failed_files.append((str(path), info))
                logger.error(f"💥 {path}: {info}")
            else:
                parsed, err = parse_mkvmerge_json(path, info)
                if err:
                    failed_files.append((str(path), err))
                elif parsed:
                    for t in parsed:
                        if t["type"] == "video" and "hevc" not in t["codec"].lower():
                            non_hevc.append(str(path))
                            break
                    results.append(parsed)
            elapsed = time.time() - start
            print(color_text(f"✅ {path.name} processed in {elapsed:.2f}s", "green"))
            processed += 1
            print_progress(processed, total_files)

    print()
    return results, skipped_videos, skipped_others, skipped_dirs, failed_files, non_hevc


# =====================================================
# Installer
# =====================================================

def install_self(target='/usr/local/bin/mkv-scan'):
    print("🧹 Cleaning old installs...")
    for p in [
        Path("/usr/local/bin/mkv-scan"),
        Path("/usr/local/sbin/mkv-scan"),
        Path.home() / ".local/bin/mkv-scan"
    ]:
        if p.exists():
            try:
                p.unlink()
                print(f"  ✅ Removed {p}")
            except Exception as e:
                print(f"  ⚠️ Could not remove {p}: {e}")
    tgt = Path(target)
    shutil.copy2(Path(__file__).resolve(), tgt)
    os.chmod(tgt, 0o755)
    print(f"✅ Installed to {tgt}")

def uninstall_self():
    print("🧹 Uninstalling mkv-scan...")
    for p in [
        Path("/usr/local/bin/mkv-scan"),
        Path("/usr/local/sbin/mkv-scan"),
        Path.home() / ".local/bin/mkv-scan"
    ]:
        if p.exists():
            try:
                p.unlink()
                print(f"✅ Removed {p}")
            except Exception as e:
                print(f"⚠️ Could not remove {p}: {e}")
    print("✅ Uninstallation complete.")


# =====================================================
# Main
# =====================================================

def main():
    if len(sys.argv) > 1 and sys.argv[1].lower() in ("install", "uninstall"):
        return install_self() if sys.argv[1] == "install" else uninstall_self()

    parser = argparse.ArgumentParser(description="Scan MKV files recursively and extract track metadata.")
    parser.add_argument("--input-dir", "--id", default=".", help="Root folder to scan.")
    parser.add_argument("--filename-base", "--fb", default="tracks", help="Base output filename (no extension).")
    parser.add_argument("--file-type", "--ft", choices=["csv", "json"], default="csv", help="Output file format.")
    parser.add_argument("--output-base-dir", "--obd", default=None, help="Base dir where <timestamp>_scan folder is created.")
    parser.add_argument("--batch-size", "--bs", type=int, default=0, help="Split output into batches of N files each.")
    args = parser.parse_args()

    target = Path(args.input_dir).resolve()
    if not target.exists():
        print(color_text("❌ Input directory not found.", "red"))
        sys.exit(1)

    base_out = Path(args.output_base_dir).resolve() if args.output_base_dir else target
    run_dir = base_out / f"{now_ts()}_scan"
    run_dir.mkdir(parents=True, exist_ok=True)

    # Logging setup
    log_path = run_dir / "mkv-scan-log.txt"
    logger = logging.getLogger("mkv-scan")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(color_text(f"🎬 Scanning MKVs in: {target}", "cyan"))
    start = time.time()

    all_entries, skipped_videos, skipped_others, skipped_dirs, failed_files, non_hevc = scan_directory(target, logger)
    total_files = len(all_entries)
    total_tracks = sum(len(block) for block in all_entries)
    elapsed = time.time() - start
    mins, secs = divmod(elapsed, 60)

    filename_base = args.filename_base
    file_type = args.file_type
    batch_size = args.batch_size

    if total_files == 0:
        logger.warning(color_text("⚠️ No MKV files found.", "yellow"))

    print(color_text(f"🧾 Output format: {file_type.upper()}", "cyan"))
    print(color_text(f"📁 Output dir: {run_dir}", "cyan"))

    # Sort lists for readability
    skipped_videos.sort()
    skipped_others.sort()
    skipped_dirs.sort()
    failed_files.sort(key=lambda x: x[0])
    non_hevc.sort()

    # Write main report(s)
    if batch_size and total_files > batch_size:
        for i in range(0, total_files, batch_size):
            batch = all_entries[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            part = f"_batch_{batch_num:02d}"
            out_path = run_dir / f"{filename_base}{part}.{file_type}"
            if file_type == "csv":
                write_csv(out_path, batch, logger)
            else:
                write_json(out_path, batch, logger)
    else:
        out_path = run_dir / f"{filename_base}_all.{file_type}"
        if file_type == "csv":
            write_csv(out_path, all_entries, logger)
        else:
            write_json(out_path, all_entries, logger)

    # Non-HEVC separate file (only if non-empty)
    if non_hevc:
        non_hevc_path = run_dir / "non_hevc.txt"
        with open(non_hevc_path, "w", encoding="utf-8") as f:
            f.write(f"🚫 Non-HEVC Files ({len(non_hevc)})\n")
            f.write("----------------------\n")
            for path in non_hevc:
                f.write(f"{path}\n")
        logger.info(f"📝 Non-HEVC list written: {non_hevc_path}")

    # Write summary.txt
    summary_path = run_dir / "summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("======== SUMMARY ========\n")
        f.write(f"✅ Files scanned: {total_files}\n")
        f.write(f"🎞️ Tracks found: {total_tracks}\n")
        f.write(f"🕒 Time taken: {int(mins)}m {secs:.2f}s\n\n")

        def write_section(title, items, include_reason=False, hint=None):
            f.write(f"{title}: {len(items)}\n")
            if items:
                f.write("-" * len(title) + "\n")
                for item in items:
                    if include_reason:
                        f.write(f"{item[0]} — {item[1]}\n")
                    else:
                        f.write(f"{item}\n")
                if hint:
                    f.write(f"\n({hint})\n")
            f.write("\n")

        write_section("⚠️ Skipped Videos", skipped_videos)
        write_section("⚠️ Skipped Others", skipped_others)
        write_section("📁 Skipped Dirs", skipped_dirs)
        write_section("💥 Failed Files", failed_files, include_reason=True)
        write_section("🚫 Non-HEVC Files", non_hevc, hint="see non_hevc.txt for full list" if non_hevc else None)

        f.write(f"Summary generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    print(color_text("\n✅ Scan complete.", "green"))
    print(f"📂 Results saved to: {run_dir}")

if __name__ == "__main__":
    main()
